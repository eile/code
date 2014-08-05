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

#include <common/skv_types.hpp>
#include <common/skv_client_server_headers.hpp>
#include <client/skv_client_server_conn.hpp>
#include <common/skv_client_server_protocol.hpp>
#include <server/skv_server_types.hpp>

#include <server/skv_server_network_event_manager.hpp>

#include <server/skv_server_event_source.hpp>

// include the implementations of the local kv backend
#include <server/skv_local_kv_types.hpp>
#include <server/skv_local_kv_interface.hpp>

#include <server/skv_server.hpp>

skv_server_t Server;

// #ifndef SKV_SERVER_READY_FILE
// #define SKV_SERVER_READY_FILE "/var/run/skv_server.ready"
// #endif

int 
main(int argc, char **argv) 
{ 
  /************************************************************
   * Initialize MPI
   ***********************************************************/
  MPI_Init( &argc, &argv );

  int Rank;
  int NodeCount;
  MPI_Comm_rank( MPI_COMM_WORLD, &Rank );
  MPI_Comm_size( MPI_COMM_WORLD, &NodeCount );
  /***********************************************************/

  FxLogger_Init( argv[ 0 ], Rank );

  pkTraceServer::Init();

  skv_configuration_t *config = skv_configuration_t::GetSKVConfiguration();

  // Clear the skv server ready file
  if( Rank == 0 )
  {
    int rc = unlink( config->GetServerReadyFile() );

    BegLogLine( rc == 0 )
      << "skv_server_main::main():: Cleared skv server ready file: "
      << config->GetServerReadyFile()
      << EndLogLine;
  }

  BegLogLine( 1 )
    << "skv_server_main::main():: Entering... "
    << " argc: " << argc
    << " Rank: " << Rank
    << " NodeCount: " << NodeCount
    << EndLogLine;

  if( argc == 2 )
  {
    Server.Init( Rank, NodeCount, 0, argv[1] );
  }
  else
    Server.Init( Rank, NodeCount, 0, NULL );

  BegLogLine( 1 )
    << "skv_server_main::main():: Finished with Init(), calling Run() "
    << EndLogLine;

  MPI_Barrier( MPI_COMM_WORLD );

  BegLogLine( 1 )
    << "skv_server_main::main():: Server is ready!"
    << EndLogLine;

  if( Rank == 0 )
  {
    int rc = open( config->GetServerReadyFile(),
    O_CREAT | O_TRUNC | O_RDWR,
                   S_IROTH | S_IWOTH );

    StrongAssertLogLine( rc != -1 )
      << "skv_server_main::main():: ERROR: Failed to create skv server ready file: "
      << config->GetServerReadyFile()
      << " errno: " << errno
      << EndLogLine;

    BegLogLine( 1 )
      << "skv_server_main::main():: Created skv server ready file: "
      << config->GetServerReadyFile()
      << EndLogLine;
  }

  Server.Run();

  return 0;
};
