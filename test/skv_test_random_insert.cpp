/************************************************
 * Copyright (c) IBM Corp. 2007-2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     arayshu, lschneid - initial implementation
 *************************************************/

#include <time.h>
#include <mpi.h>
#include <FxLogger.hpp>
#include <Trace.hpp>
#include <client/skv_client.hpp>
#include <math.h>

#include <map>

skv_client_t Client;

#define DATA_SIZE ( 4096 )  // required for test_skv_utils.hpp - no use in this context otherwise

#include "test_skv_utils.hpp"
#include <server/skv_server_heap_manager.hpp>   // to get the server space per snode!

#define PURESTORAGE_FACTOR ( (double)0.5 )                                                      // factor to reflect space overhead in server space, represents the available fraction of space per server
#define STORAGE_SPACE_PER_SNODE     ( (int)(PERSISTENT_IMAGE_MAX_LEN * PURESTORAGE_FACTOR) )    // space available per server (reduced by an arbitrary space-overhead factor
#define MAX_TEST_SIZE ( 1024 )                   // max length of inserted buffer
#define INFLIGHT_COMMANDS ( 1024 )                // number of outstanding commands ( memory requirement = MAX_TEST_SIZE * INFLIGHT_COMMANDS )
#define INFLIGHT_WAIT ( 128 )                      // min number of commands to complete when INFLIGHT_COMMANDS limit is reached

#define PRINT_INTERVAL ( 10000000 )

#ifndef SKV_TEST_LOG
#define SKV_TEST_LOG ( 0 )
#endif

//#define SLOWDOWN 10000
//#define SKV_TEST_MAPPED_HANDELS
#define DONT_RETRIEVE
#define DONT_REMOVE
//#define DO_CHECK

struct skv_async_command_helper_t
{
  int                       mBufferSize;
  char*                     mBuffer;
  int                       mRetrieveBufferSize;

  skv_client_cmd_ext_hdl_t mCommandHdl;
};

#define STL_MAP( Key, Data ) std::map< Key, Data >

typedef STL_MAP( skv_client_cmd_ext_hdl_t, skv_async_command_helper_t * ) command_handle_to_timer_map_t;


static inline
int
calculateValue( int r, int a, int b )
{
  return (int)((a + b) & 0xFF);
}

static inline
int
calculateKey( int rank,
              int si,
              int t,
              int num_tries )
{
  return ( rank * num_tries ) + t;
}



int 
main(int argc, char **argv) 
{  
  printf( "skv_client::entering main \n" ); fflush( stdout );

  FxLogger_Init( argv[ 0 ] );
  pkTraceServer::Init();

  StrongAssertLogLine( argc == 2 )
    << "skv_test_n_inserts_retrieves::main():: ERROR:: "
    << " argc: " << argc
    << EndLogLine; 

  char* ServerAddress = argv[ 1 ];  

  int Rank = 0;
  int NodeCount = 1;


  /*****************************************************************************
   * Init MPI
   ****************************************************************************/ 
  MPI_Init( &argc, &argv );
  MPI_Comm_rank( MPI_COMM_WORLD, &Rank );
  MPI_Comm_size( MPI_COMM_WORLD, &NodeCount );
  printf(" %d: MPI_Init complete\n", Rank);
  /****************************************************************************/ 



  /*****************************************************************************
   * Init the SKV Client
   ****************************************************************************/ 
  skv_status_t status = Client.Init( 0,
#ifndef SKV_CLIENT_UNI
                                      MPI_COMM_WORLD,
#endif
                                      Rank );

  if( status == SKV_SUCCESS )
    {
      BegLogLine( SKV_TEST_LOG )
        << "skv_test_n_inserts_retrieves::main():: SKV Client Init succeded "
        << EndLogLine;
    }
  else
    {
      BegLogLine( SKV_TEST_LOG )
        << "skv_test_n_inserts_retrieves::main():: SKV Client Init FAILED "
        << " status: " << skv_status_to_string( status )
        << EndLogLine;
    }  
  /****************************************************************************/ 



  /*****************************************************************************
   * Connect to the SKV Server
   ****************************************************************************/ 
  BegLogLine( SKV_TEST_LOG )
    << "skv_test_n_inserts_retrieves::main():: About to connect "
    << " ServerAddress: " << ServerAddress
    << EndLogLine;

  status = Client.Connect( ServerAddress, 0 );

  if( status == SKV_SUCCESS )
    {
      BegLogLine( SKV_TEST_LOG )
        << "skv_test_n_inserts_retrieves::main():: SKV Client connected to "
        << " server at { " << ServerAddress << " }"
        << EndLogLine;
    }
  else
    {
      BegLogLine( SKV_TEST_LOG )
        << "skv_test_n_inserts_retrieves::main():: SKV Client FAILED to connect "
        << " server at { " << ServerAddress << " }"
        << " status: " << skv_status_to_string( status )
        << EndLogLine;
    }
  /****************************************************************************/ 




  /*****************************************************************************
   * Open a test PDS
   ****************************************************************************/
  char MyTestPdsName[ SKV_MAX_PDS_NAME_SIZE ];
  bzero( MyTestPdsName, SKV_MAX_PDS_NAME_SIZE );

  sprintf( MyTestPdsName, "RandomPds" );

  BegLogLine( SKV_TEST_LOG )
    << "skv_test_n_inserts_retrieves::main():: About to open pds name: "
    << " MyTestPdsName: " << MyTestPdsName
    << EndLogLine;

  skv_pds_id_t  MyPDSId;
  status = Client.Open( MyTestPdsName,
                        (skv_pds_priv_t) (SKV_PDS_READ | SKV_PDS_WRITE),
                        (skv_cmd_open_flags_t) SKV_COMMAND_OPEN_FLAGS_CREATE,
                        & MyPDSId );

  if( status == SKV_SUCCESS )
    {
      BegLogLine( SKV_TEST_LOG )
        << "skv_test_n_inserts_retrieves::main():: SKV Client successfully opened: "
        << MyTestPdsName
        << " MyPDSId: " << MyPDSId
        << EndLogLine;
    }
  else
    {
      BegLogLine( SKV_TEST_LOG )
        << "skv_test_n_inserts_retrieves::main():: SKV Client FAILED to open: "
        << MyTestPdsName
        << " status: " << skv_status_to_string( status )
        << EndLogLine;
    }
  /****************************************************************************/

  MPI_Barrier( MPI_COMM_WORLD );

  uint64_t NUMBER_OF_TRIES = 0;
  uint64_t NUMBER_OF_SNODES = SKVTestGetServerCount();

  uint64_t writtenSize = 0;
  uint64_t CLIENT_BYTES_TO_WRITE = (STORAGE_SPACE_PER_SNODE * NUMBER_OF_SNODES) / NodeCount;
  // uint64_t CLIENT_BYTES_TO_WRITE = 100000;

  BegLogLine( 1 )
    << "Each client going to write: " << CLIENT_BYTES_TO_WRITE
    << " bytes" 
    << EndLogLine;

  uint64_t testDataSize;
  uint64_t InsertCount = 0;
  int      t = 0;               // command handle index
  int      t_cmpl = 0;          // completed command handle index
  char    *dataBuffer = (char *)malloc( MAX_TEST_SIZE * INFLIGHT_COMMANDS );
  int      statusPrinted = 1;
  uint64_t immediate_count = 0; // counting completed hdls beyond minimal number set by INFLIGHT_WAIT

  skv_async_command_helper_t commandHelpers[ INFLIGHT_COMMANDS ];
  skv_client_cmd_ext_hdl_t CommandHdl;


  srandom(Rank);

  double InsertTimeStart = MPI_Wtime();
  while( writtenSize < CLIENT_BYTES_TO_WRITE )
    {
    if( ( writtenSize % PRINT_INTERVAL < MAX_TEST_SIZE ) && (statusPrinted==0) )
      {
      statusPrinted = 1;
      BegLogLine( 1 | SKV_TEST_LOG )
        << "SKV_TEST: servers: " << NUMBER_OF_SNODES
        << " clients: "           << NodeCount
        << " totalBytes: "        << CLIENT_BYTES_TO_WRITE
        << " writtenSize: "       << writtenSize
        << " inserts: "           << InsertCount
        << EndLogLine;
      }
   if( writtenSize % PRINT_INTERVAL > MAX_TEST_SIZE )
      statusPrinted = 0;

    // printf( "running test with data size: %d\n", testDataSize );

    /*****************************************************************************
     * Allocate Insert/Retrieve data arrays
     ****************************************************************************/
    testDataSize = random() % MAX_TEST_SIZE;
#ifdef DO_CHECK
    commandHelpers[ t ].mBufferSize = testDataSize;
    commandHelpers[ t ].mBuffer = &(dataBuffer[ t * MAX_TEST_SIZE ]);
  
    StrongAssertLogLine( commandHelpers[ t ].mBuffer != NULL )    
      << "ERROR:: "
      << " testDataSize: " << testDataSize
      << EndLogLine;

    /*****************************************************************************
     * Insert Key / Value
     ****************************************************************************/
    for( int i=0; i < testDataSize; i++ )
      {
      char ch = i;
      commandHelpers[ t ].mBuffer[ i ] = calculateValue( Rank, ch, t );
      }
#else
    char* Buffer = &(dataBuffer[ t * MAX_TEST_SIZE ]);
    StrongAssertLogLine( Buffer != NULL )    
      << "ERROR:: "
      << " testDataSize: " << testDataSize
      << EndLogLine;      
#endif

    int Key = calculateKey( Rank, 0, t, 0 );

    BegLogLine( SKV_TEST_LOG )
      << "skv_test_n_inserts_retrieves::main():: About to Insert "
      << " into MyPDSId: " << MyPDSId
      << " key: " << Key
      << EndLogLine;      

    status = Client.iInsert( &MyPDSId,
                             (char *) &Key,
                             sizeof( int ),
#ifdef DO_CHECK
                             commandHelpers[ t ].mBuffer,
                             commandHelpers[ t ].mBufferSize,
#else
                             Buffer, 
                             testDataSize,
#endif
                             random() % 2000,
                             (skv_cmd_RIU_flags_t)(SKV_COMMAND_RIU_INSERT_EXPANDS_VALUE | 
                                                    SKV_COMMAND_RIU_INSERT_OVERLAPPING | 
                                                    SKV_COMMAND_RIU_INSERT_USE_RECORD_LOCKS ),
                             & ( commandHelpers[ t ].mCommandHdl ) );

    if( status == SKV_SUCCESS )
      {
#ifdef DO_CHECK
      uintptr_t *buffer = (uintptr_t*)commandHelpers[ t ].mBuffer;
#else
      uintptr_t *buffer = (uintptr_t*)Buffer;
#endif
      BegLogLine( SKV_TEST_LOG )
        << "insert key: " << Key
        << " " << HEXLOG(buffer[ 0 ])
        << " " << HEXLOG(buffer[ 1 ])
        << " " << HEXLOG(buffer[ 2 ])
        << " " << HEXLOG(buffer[ 3 ])
        << " " << HEXLOG(buffer[ 4 ])
        << EndLogLine;

      BegLogLine( SKV_TEST_LOG )
        << "skv_test_n_inserts_retrieves::main():: SKV Client successfully posted Insert command "
        << " into MyPDSId: " << MyPDSId
        << " key: " << Key
        << " idx: " << t
        << EndLogLine;

      }
    else
      {
      BegLogLine( SKV_TEST_LOG )
        << "skv_test_n_inserts_retrieves::main():: SKV Client FAILED to Insert: "
        << " into MyPDSId: " << MyPDSId
        << " status: " << skv_status_to_string( status )
        << EndLogLine;
      }  

    InsertCount++;
    writtenSize += testDataSize;

    t = ( t+1 ) % INFLIGHT_COMMANDS; // go to next slot

    /****************************************************************************
     * Wait for insert commands to finish in case we need free slots
     ****************************************************************************/
    if( t == t_cmpl )
    {
      for( int n=0; n < INFLIGHT_WAIT; n++ )
        {
        CommandHdl = commandHelpers[ t_cmpl ].mCommandHdl;
        status = Client.Wait( CommandHdl );

        BegLogLine( SKV_TEST_LOG )
          << "Insert command completed: "
          << " t_cmpl: " << n
          << " CommandHdl: " << (void *) CommandHdl
          << EndLogLine;
          
        t_cmpl = (t_cmpl + 1) % INFLIGHT_COMMANDS;
        }
      // while( ( status == SKV_SUCCESS ) && ( t_cmpl != t ) );
      // {
      //   CommandHdl = commandHelpers[ t_cmpl ].mCommandHdl;
      //   status = Client.Test( CommandHdl );
      //   BegLogLine( SKV_TEST_LOG )
      //     << "Subsequent hdl complete: "
      //     << " t_cmpl: " << t_cmpl
      //     << " CommandHdl: " << (void *) CommandHdl
      //     << EndLogLine;
      //   if( status == SKV_SUCCESS )
      //   {
      //     t_cmpl = (t_cmpl + 1) % INFLIGHT_COMMANDS;
      //     immediate_count++;
      //   }
      // }
    }

    /****************************************************************************/

#ifdef SLOWDOWN
    usleep(SLOWDOWN);
#endif

    /****************************************************************************/

    }
  // step back one slot because current t points to empty slot
  t = ( t-1 ) % INFLIGHT_COMMANDS;

  BegLogLine(1)
    << "inserts complete: "
    << " t: " << t
    << " t_cmpl: " << t_cmpl
    << EndLogLine;

  // wait for remaining inflight commands
  while( t_cmpl != t )
    {
    CommandHdl = commandHelpers[ t_cmpl ].mCommandHdl;
    status = Client.Wait( CommandHdl );

    BegLogLine( 1 | SKV_TEST_LOG )
      << "Insert command completed: "
      << " CommandHdl: " << (void *) CommandHdl
      << " t: " << t
      << " t_cmpl: " << t_cmpl
      << EndLogLine;

    t_cmpl = (t_cmpl + 1) % INFLIGHT_COMMANDS;
    }

  BegLogLine(1)
    << "All waits/inserts complete: "
    << EndLogLine;

  double InsertTime = MPI_Wtime() - InsertTimeStart;
  double InsertBytes = (double)writtenSize;
  double InsertCountDBL = (double)InsertCount;

  double GlobalInsertTime = 0.0;
  double GlobalWrittenSize = 0.0;
  double GlobalInsertCount = 0.0;

  MPI_Reduce( & InsertTime, & GlobalInsertTime, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD );
  MPI_Reduce( & InsertBytes, & GlobalWrittenSize, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD );
  MPI_Reduce( & InsertCountDBL, & GlobalInsertCount, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD );

  BegLogLine( 1 )
    << "Timings: "
    << " tsec: "   << GlobalInsertTime
    << " tsize: "  << GlobalWrittenSize
    << "s avg: "   << GlobalInsertTime / (1. * NodeCount )
    << "s bw: "    << GlobalWrittenSize / GlobalInsertTime / (1024*1024)
    << "MiB/s IOPS: "  << (double)GlobalInsertCount / GlobalInsertTime
    << " local immediate cmpl: " << immediate_count
    << EndLogLine;

  MPI_Barrier( MPI_COMM_WORLD );
  free( dataBuffer );

  pkTraceServer::FlushBuffer();

  Client.Disconnect();
  Client.Finalize();

  MPI_Finalize();

  return 0;
}
