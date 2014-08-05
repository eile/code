/************************************************
 * Copyright (c) IBM Corp. 2013-2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *     lschneid - initial implementation
 *
 *  Created on: Jan 21, 2014
 *************************************************/

#include <common/skv_errno.hpp>
#include <common/skv_types.hpp>
#include <utils/skv_trace_clients.hpp>

#include <common/skv_client_server_headers.hpp>
#include <client/skv_client_server_conn.hpp>
#include <common/skv_client_server_protocol.hpp>
#include <server/skv_server_types.hpp>

#include <server/skv_local_kv_types.hpp>
#include <server/skv_local_kv_leveldb.hpp>


skv_status_t
skv_local_kv_leveldb::Init( int aRank,
                            int aNodeCount,
                            skv_server_internal_event_manager_if_t *aInternalEventMgr,
                            it_pz_handle_t aPZ,
                            char* aCheckpointPath )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_leveldb::Exit()
{
  return SKV_SUCCESS;
}

skv_local_kv_event_t*
skv_local_kv_leveldb::GetEvent()
{
  return NULL;
}

skv_status_t
skv_local_kv_leveldb::AckEvent( skv_local_kv_event_t *aEvent )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_leveldb::CancelContext( skv_local_kv_req_ctx_t *aReqCtx )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_leveldb::GetDistribution(skv_distribution_t **aDist,
                                      skv_local_kv_cookie_t *aCookie )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_leveldb::PDS_Open( char *aPDSName,
                                skv_pds_priv_t aPrivs,
                                skv_cmd_open_flags_t aFlags,
                                skv_pds_id_t *aPDSId,
                                skv_local_kv_cookie_t *aCookie )
{
  return SKV_SUCCESS;
}
skv_status_t
skv_local_kv_leveldb::PDS_Stat( skv_pdscntl_cmd_t aCmd,
                                skv_pds_attr_t *aPDSAttr,
                                skv_local_kv_cookie_t *aCookie )
{
  return SKV_SUCCESS;
}
skv_status_t
skv_local_kv_leveldb::PDS_Close( skv_pds_attr_t *aPDSAttr,
                                 skv_local_kv_cookie_t *aCookie )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_leveldb::Lookup( skv_pds_id_t aPDSId,
                              char *aKeyPtr,
                              int aKeySize,
                              skv_cmd_RIU_flags_t aFlags,
                              skv_lmr_triplet_t *aStoredValueRep,
                              skv_local_kv_cookie_t *aCookie )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_leveldb::Insert( skv_cmd_RIU_req_t *aReq,
                              skv_status_t aCmdStatus,
                              skv_lmr_triplet_t *aStoredValueRep,
                              skv_lmr_triplet_t *aValueRDMADest,
                              skv_local_kv_cookie_t *aCookie )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_leveldb::Insert( skv_pds_id_t& aPDSId,
                              char* aRecordRep,
                              int aKeySize,
                              int aValueSize,
                              skv_local_kv_cookie_t *aCookie )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_leveldb::InsertPostProcess( skv_local_kv_req_ctx_t *aReqCtx,
                                         skv_lmr_triplet_t *aValueRDMADest,
                                         skv_local_kv_cookie_t *aCookie )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_leveldb::BulkInsert( skv_pds_id_t aPDSId,
                                  skv_lmr_triplet_t *aLocalBuffer,
                                  skv_local_kv_cookie_t *aCookie )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_leveldb::Retrieve( skv_pds_id_t aPDSId,
                                char* aKeyData,
                                int aKeySize,
                                int aValueOffset,
                                int aValueSize,
                                skv_cmd_RIU_flags_t aFlags,
                                skv_lmr_triplet_t* aStoredValueRep,
                                int *aTotalSize,
                                skv_local_kv_cookie_t *aCookie )
{
  return SKV_SUCCESS;
}
skv_status_t
skv_local_kv_leveldb::RetrievePostProcess( skv_local_kv_req_ctx_t *aReqCtx )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_leveldb::RetrieveNKeys( skv_pds_id_t aPDSId,
                                     char * aStartingKeyData,
                                     int aStartingKeySize,
                                     skv_lmr_triplet_t* aRetrievedKeysSizesSegs,
                                     int* aRetrievedKeysCount,
                                     int* aRetrievedKeysSizesSegsCount,
                                     int aListOfKeysMaxCount,
                                     skv_cursor_flags_t aFlags,
                                     skv_local_kv_cookie_t *aCookie )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_leveldb::Remove( skv_pds_id_t aPDSId,
                              char* aKeyData,
                              int aKeySize,
                              skv_local_kv_cookie_t *aCookie )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_leveldb::Lock( skv_pds_id_t *aPDSId,
                            skv_key_value_in_ctrl_msg_t *aKeyValue,
                            skv_rec_lock_handle_t *aRecLock )
{
  return SKV_SUCCESS;
}
skv_status_t
skv_local_kv_leveldb::Unlock( skv_rec_lock_handle_t aLock )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_leveldb::RDMABoundsCheck( const char* aContext,
                                       char* aMem,
                                       int aSize )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_leveldb::Allocate( int aBuffSize,
                                skv_lmr_triplet_t *aRDMARep )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_leveldb::Deallocate( skv_lmr_triplet_t *aRDMARep )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_leveldb::CreateCursor( char* aBuff,
                                    int aBuffSize,
                                    skv_server_cursor_hdl_t* aServCursorHdl,
                                    skv_local_kv_cookie_t* aCookie )
{
  return SKV_SUCCESS;
}

skv_status_t
skv_local_kv_leveldb::DumpImage( char* aCheckpointPath )
{
  return SKV_SUCCESS;
}

