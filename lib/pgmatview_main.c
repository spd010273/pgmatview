/*
    Entry point for cachemgr process
*/
#include "postgres.h"

#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"


#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "tcop/utility.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1( cachemgr_launch );

void _PG_init( void );
void cachemgr_main( Datum ) pg_attribute_noreturn();

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

static int cachemgr_sleep_time   = 10;
static int cachemgr_worker_count = 1;

#define PROCESS_NAME "cachemgr_bgw"
#define WORKER_NAME "cachemgr_bgw"

static void cachemgr_sigterm( SIGNAL_ARGS )
{
    int save_errno = errno;

    got_sigterm = true;
    SetLatch( MyLatch );

    errno = save_errno;
}


static void cachemgr_sighup( SIGNAL_ARGS )
{
    int save_errno = errno;

    got_sighup = true;
    SetLatch( MyLatch );

    errno = save_errno;
}

static void initialize_cachemgr( void )
{
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect(); // Changes palloc context
    PushActiveSnapshop( GetTransactionSnapshot() );
    pgstat_report_activity( STATE_RUNNING, "Initializing Cache Manager" );

    /* D O   S O M E   S T U F F */


    SPI_finish(); // Closes function context, frees pallocs
    PopActiveSnapshot();
    CommitTransactionCommand();
    pgstat_report_activity( STATE_IDLE, NULL );
}

void cachemgr_main( Dataum main_arg )
{
    int index = DataumGetInt32( main_arg );

    StringInfoData buffer; // Used for strings n such

    /* Setup signal handlers */
    pqsignal( SIGHUP, cachemgr_sighup );
    pqsignal( SIGTERM, cachemgr_sigterm );

    BackgroundWorkerUnblockSignals();

    BackgroundWorkerInitializeConnection( "postgres", NULL );

    // probably should log that we're alive
    initialize_cachemgr();
    /* Main Loop */
    while( !got_sigterm )
    {
        int rc;

        rc = WaitLatch(
            MyLatch,
            WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
            cachemgr_sleep_time * 1000L,
            PG_WAIT_EXTENSION
        );

        ResetLatch( MyLatch );

        if( rc & WL_POSTMASTER_DEATH )
        {
            // Something bad probably happened, let's GTFO
            proc_exit( 1 );
        }

        CHECK_FOR_INTERRUPTS();

        if( got_sighup )
        {
            got_sighup = false;
            ProcessConfigFile( PGC_SIGHUP );
        }
    
        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        SPI_connect();
        PushActiveSnapshot( GetTransactionSnapshot() );
        pgstat_report_activity( STATE_RUNNING, "Foobar" );

        /* D O   S O M E   S T U F F */
 
        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
        pgstat_report_stat( false );
        pgstat_report_activity( STATE_IDLE, NULL );
    }

    proc_exit( 1 );
}

/* Entrypoint of this module */
void _PG_init( void )
{
    BackgroundWorker worker;
    unsigned int i;

    DefineCustomIntVariable(
        "cachemgr.sleep_time",
        "Duration between invalidation peeks (ms)",
        NULL,
        &cachemgr_sleep_time,
        10,
        1,
        INT_MAX,
        PGC_SIGHUP,
        0,
        NULL,
        NULL,
        NULL
    );
    
    if( !process_shared_preload_libraries_in_progress )
    {
        return;
    }

    DefineCustomIntVariable(
        "cachemgr.worker_count",
        "Number of workers to start.",
        NULL,
        &cachemgr_worker_count,
        1,
        1,
        50,
        PGC_POSTMASTER,
        0,
        NULL,
        NULL,
        NULL
    );

    memset( &worker, 0, sizeof( worker ) );

    /* Setup our access flags - We'll need a connection and access to SHM */
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;

    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    sprintf( worker.bgw_library_name, "cachemgr" );
    sprintf( worker.bgw_function_name, "cachemgr_main" );
    worker.bgw_notify_pid = 0;

    for( i = 1; i <= cachemgr_worker_count; i++ )
    {
        snprintf( worker.bgw_name, BGW_MAXLEN, "cachemgr_%d", i );
        worker.bgw_main_Arg = Int32GetDatum( i );

        RegisterBackgroundWorker( &worker );
    }
}

Datum cachemgr_launch( PG_FUNCTION_ARGS )
{
    int32 i = PG_GETARG_INT32( 0 );
    BackgroundWorker worker;
    BackgroundWorkerHandle * handle;
    BgwHandleStatus status;
    pid_t pid;

    memset( &worker, 0, sizeof( worker ) );
    worker.bgw_flags = BGW_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    sprintf( worker.bgw_library_name, "cachemgr" );
    sprintf( worker.bgw_function_name, "cachemgr_main" );
    snprintf( worker.bgw_name, BGW_MAXLEN, "cachemgr_%d", i );
    worker.bgw_main_arg = Int32GetDatum( i );

    worker.bgw_notify_pid = MyProcPid;

    if( !RegisterDynamicBackgroundWorker( &worker, &handle ) )
    {
        PG_RETURN_NULL();
    }

    status = WaitForBackgroundWorkerStartup( handle, &pid );

    if( status == BGWH_STOPPED )
    {
        ereport(
            ERROR,
            (
                errcode( ERRCODE_INSUFFICIENT_RESOURCES ),
                errmsg( "Could not start background process" ),
                errhint( "Check server logs." )
            )
        );
    }

    if( status == BGWH_POSTMASTER_DIED )
    {
        /* RIP. Gone but not forgotten. */
        ereport(
            ERROR
            (
                errcode( ERRCODE_INSUFFICIENT_RESOURCES ),
                errmsg( "Cannot start background process without postmaster" ),
                errhint( "Kill all remaining database processes and restart the database" )
            )
        );
    }

    Assert( status == BGWH_STARTED );

    PG_RETURN_INT32( pid );
}
