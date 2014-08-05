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

#include <stdlib.h>
#include <stdio.h>
#include <FxLogger.hpp>

#include <common/skv_types_ext.hpp>
#include <client/skv_client.hpp>

static 
inline 
int
read_from_file( FILE* file, char * buff, int len, int* rlen )
{
  int BytesRead = 0;
  int ReadCount = 0;

  for( ; BytesRead < len; )
  {
    int read_rc = fread( (((char *) buff) + BytesRead),
                         sizeof(char),
                         len - BytesRead,
                         file );
    if( read_rc < 0 )
    {
      // printf( "errno: %d\n", errno );
      if( errno == EAGAIN )
        continue;
      else if( errno == ECONNRESET )
      {
        return -1;
      }
      else
      StrongAssertLogLine( 0 )
        << "read_from_socket:: ERROR:: "
        << "failed to write to file: " << file
        << " errno: " << errno
        << EndLogLine;
    }
    else if( read_rc == 0 )
      return 0;
	
    ReadCount++;
    BytesRead += read_rc;
  }

  *rlen = BytesRead;
  return 1;
}

skv_status_t
skv_client_t::
Init( skv_client_group_id_t  aCommGroupId,  
      MPI_Comm                aComm,
      int                     aFlags )
{
  return SKV_SUCCESS;
}

skv_status_t 
skv_client_t::
Connect( const char* aServerGroupName, int aFlags )
{
  return SKV_SUCCESS;
}

skv_status_t 
skv_client_t::
Disconnect()
{
  return SKV_SUCCESS;
}

// Blocking interface
skv_status_t 
skv_client_t::
Open( char*                 aPDSName, 
      skv_pds_priv_t       aPrivs, 
      skv_cmd_open_flags_t aFlags, 
      skv_pds_id_t*        aPDSId )
{
  FILE* fd = fopen( aPDSName, "r");

  StrongAssertLogLine( fd != NULL )
    << "skv_client_t::Open(): ERROR: "
    << " aPDSName: " << aPDSName
    << EndLogLine;
  
  aPDSId->mPDSFileFd = fd;

  return SKV_SUCCESS;
}

skv_status_t 
skv_client_t::
Close( skv_pds_id_t* aPDSId )
{
  fclose( aPDSId->mPDSFileFd );
  
  return SKV_SUCCESS;
}

/******************************************************************************
 * Relational Cursor Interface
 *****************************************************************************/
skv_status_t
skv_client_t::
CreateIndex( skv_pds_id_t*                       aPDSId,
             TupleStash*                          aTableDesc,
             int32_t*                             aColDesc,
             int                                  aColDescCount, 
             relational_postfix_token_manager_t*  aPostfixTokenManager,
             skv_index_flags_t                   aFlags,
             skv_client_index_ext_hdl_t*         aIndexHandle )
{
  StrongAssertLogLine( aPostfixTokenManager == NULL )
    << "skv_client_t::CreateIndex(): ERROR: aPostfixTokenManager is not NULL"
    << EndLogLine;
  
  *aIndexHandle = (void *) aPDSId;
  
  return SKV_SUCCESS;
}
      
// Ordered cursor
skv_status_t 
skv_client_t::
CreateCursor( skv_client_index_ext_hdl_t              aIndexHandle,
              int32_t*                                 aProjColDesc,
              int                                      aProjColDescCount, 
              relational_postfix_token_manager_t*      aPostfixTokenManager,
              skv_cursor_flags_t                      aFlags,
              int                                      aMaxRowSize,
              skv_client_relational_cursor_ext_hdl_t* aCursorHdl )
{

  *aCursorHdl = aIndexHandle;
  
  return SKV_SUCCESS;
}


skv_status_t 
skv_client_t::
CloseIndex( skv_client_index_ext_hdl_t                    aIndexHdl )
{
  return SKV_SUCCESS;  
}

skv_status_t 
skv_client_t::
CloseRelationalCursor( skv_client_relational_cursor_ext_hdl_t       aCursorHdl )
{
  return SKV_SUCCESS;
}

skv_status_t 
skv_client_t::
GetFirstElement( skv_client_relational_cursor_ext_hdl_t   aCursorHdl,
                 char*                                     aRetrievedBuffer,
                 int*                                      aRetrievedBufferSize,
                 int                                       aRetrievedBufferMaxSize,
                 skv_cursor_flags_t                       aFlags )
{
  return GetNextElement( aCursorHdl,
                         aRetrievedBuffer,
                         aRetrievedBufferSize,
                         aRetrievedBufferMaxSize,
                         aFlags );
}

skv_status_t 
skv_client_t::
GetNextElement( skv_client_relational_cursor_ext_hdl_t   aCursorHdl,
                char*                                     aRetrievedBuffer,
                int*                                      aRetrievedBufferSize,
                int                                       aRetrievedBufferMaxSize,
                skv_cursor_flags_t                       aFlags )
{
  FILE* fp = ((skv_pds_id_t *)aCursorHdl)->mPDSFileFd;
  
  int BytesRead;
  int readrc = read_from_file( fp,
                               (char *) aRetrievedBuffer,
                               sizeof(int),
                               &BytesRead );

  if( (readrc == 0) && feof( fp ) )
  {
    return SKV_ERRNO_END_OF_RECORDS;
  }

  StrongAssertLogLine( readrc > 0 && (BytesRead == sizeof(int)) )
    << "skv_client_t::GetFirstElement(): ERROR: "
    << " readrc: " << readrc
    << " BytesRead: " << BytesRead
    << EndLogLine;
  
  int RowSize = *((int*) aRetrievedBuffer );
  int SizeToRead = RowSize - sizeof( int );

  readrc = read_from_file( fp, 
                           aRetrievedBuffer, 
                           SizeToRead,
                           & BytesRead );

  StrongAssertLogLine( readrc > 0 && (BytesRead == SizeToRead) )
    << "skv_client_t::GetFirstElement(): ERROR: "
    << " readrc: " << readrc
    << " BytesRead: " << BytesRead
    << " SizeToRead: " << SizeToRead
    << EndLogLine;

  return SKV_SUCCESS;
}

