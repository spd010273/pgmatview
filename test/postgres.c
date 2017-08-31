/*

*/
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <libpq-fe.h>
#include <math.h>
#include <string.h>

#include <errno.h>
#include <sys/time.h>
#include <sys/types.h>

/*
    Compile with:

gcc this_file.c -I /usr/include/postgresql/ -L /usr/include/postgresql/ -lm -lpq -o this_file 
*/

char * parse_args( int argc, char ** argv )
{
    /* Parse command line arguments */
    char * conninfo = NULL;
    char * username = NULL;
    char * dbname   = NULL;
    char * port     = NULL;
    char * hostname = NULL;

    int index;
    int c;

    opterr = 0;
    
    while( ( c = getopt( argc, argv, "U:p:d:h:" ) ) != -1 )
    {
        switch( c )
        {
            case 'U':
                username = optarg;
                break;
            case 'p':
                port = optarg;
                break;
            case 'd':
                dbname = optarg;
                break;
            case 'h':
                hostname = optarg;
                break;
            case '?':
                break;
            default:
                printf( "Invalid argument: %c\n", (char) c );
                return;
        }
    }
    
    if( port == NULL )
    {
        port = "5432";
    }

    if( username == NULL )
    {
        username = "postgres";
    }

    if( hostname == NULL )
    {
        hostname = "localhost";
    }

    if( dbname == NULL )
    {
        dbname = username;
    }
    
    /*
        malloc connection string 
        accounting for
        'username=? host=? port=? dbname=?'
    */
    
    int malloc_size = strlen( username )
                    + strlen( port )
                    + strlen( dbname )
                    + strlen( hostname );

    /* add 26 bytes to account for const strings + null terminator */
    conninfo = ( char * ) malloc( sizeof( char ) * ( malloc_size + 26 ) );
    
    /* Assemble the connections string */ 
    strcpy( conninfo, "user=" );
    strcat( conninfo, username );
    strcat( conninfo, " host=" );
    strcat( conninfo, hostname );
    strcat( conninfo, " port=" );
    strcat( conninfo, port );
    strcat( conninfo, " dbname=" );
    strcat( conninfo, dbname );
    
    /* make sure string is null terminated */
    conninfo[malloc_size + 26] = '\0';
    
    return conninfo;
}

int main( int argc, char ** argv )
{
    char * conninfo = parse_args( argc, argv );
    if( conninfo == NULL )
    {
        return 1;
    }
    
    /* Connect to the database */ 
    PGconn * conn = PQconnectdb( conninfo );
    free( conninfo );

    if( PQstatus( conn ) != CONNECTION_OK )
    {
        fprintf(
            stderr,
            "Connection to database failed: %s\n",
            PQerrorMessage( conn )
        );
        
        PQfinish( conn );
        return 1;
    }

    PGresult * result;

    result = PQexec( conn, "LISTEN test_notify" );

    if( PQresultStatus( result ) != PGRES_COMMAND_OK )
    {
        fprintf(
            stderr,
            "LISTEN on channel test_notify failed: %s\n",
            PQerrorMessage( conn )
        );

        PQclear( result );
        PQfinish( conn );
        return 1;
    }

    PQclear( result );

    int notify_count = 0;
    PGnotify * notify = NULL;

    while( notify_count == 0 )
    {
        int sock;
        fd_set input_mask;
        sock = PQsocket( conn );

        if( sock < 0 )
        {
            break;
        }

        FD_ZERO( &input_mask );
        FD_SET( sock, &input_mask );

        if( select( sock + 1, &input_mask, NULL, NULL, NULL ) < 0 )
        {
            fprintf( stderr, "select() failed: %s\n", strerror( errno ) );
            PQfinish( conn );
            return 1;
        }

        PQconsumeInput( conn );

        while( ( notify = PQnotifies( conn ) ) != NULL )
        {
            fprintf(
               stderr,
               "ASYNCRONOUS NOTIFY of '%s' received from backend PID %d\n",
               notify->relname,
               notify->be_pid
            );

            PQfreemem( notify );
            notify_count++;
        }
    }

    PQfinish( conn );
    return 0;
}
