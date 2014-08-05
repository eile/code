/************************************************
 * Copyright (c) IBM Corp. 2007-2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     lschneid - initial implementation
 *************************************************/

/*
 * This file contains the interface definition for interaction with
 * a local KV storage.
 *
 * Note: The storage operations might be asynchronous in the sense
 * that they return with an error indicating that the operation requires
 * further internal processing.  Any user of the API must be able to handle
 * these types of return code and wait for a storage event that signals the
 * completion of the internal operation.
 * However, some local KV backends like the in-memory backend, are synchronous
 * in the sense that they will return with the operation immediately completed
 *
 * Note: Operations that read or write client data will not provide the actual
 * data but an RDMA address to read or write for the user of this API. This
 * allows the implementation of the local KV API to decide about buffering or
 * direct access.
 */


#ifndef SKV_LOCAL_KV_INTERFACE_HPP_
#define SKV_LOCAL_KV_INTERFACE_HPP_

#ifndef SKV_SERVER_LOCAL_KV
#warning "No local back-end defined. Using default IN-MEMORY back-end."
#define SKV_SERVER_LOCAL_KV skv_local_kv_inmem
#endif

#include <server/skv_local_kv_types.hpp>

// Include the list of defined local kv back ends
#include <server/skv_local_kv_inmem.hpp>
#include <server/skv_local_kv_asyncmem.hpp>
#include <server/skv_local_kv_leveldb.hpp>


template<class skv_local_kv_manager>
class skv_local_kv_interface
{
  skv_local_kv_manager mLocalKVManager;

public:
  skv_local_kv_interface() {}


  /**
   * Initializsation of the local KV manager
   * Potential tasks that can be done in an implementation
   * - setting up paths and nodes
   * - create storage event source
   * - create threads to perform asynchronous tasks
   * - ...
   *
   * \param[in] aNodeId              rank of the server node in server group
   * \param[in] aNodeCount           number of nodes in the server group
   * \param[in] aInternalEventQueue  event queue for server cursor events
   * \param[in] aPZ_Hdl              protection zone handle to register rdma memory
   * \param[in] aCheckpointPath      path for a persistent (or external) checkpoint
   *
   * \return SKV_SUCCESS if successful
   *
   * \todo: need to check if internaleventqueue really needed to pass
   */
  skv_status_t Init( int aNodeId,
                     int aNodeCount,
                     skv_server_internal_event_manager_if_t *aInternalEventQueue,
                     it_pz_handle_t aPZ_Hdl,
                     char *aCheckpointPath )
  {
    return mLocalKVManager.Init( aNodeId, aNodeCount, aInternalEventQueue, aPZ_Hdl, aCheckpointPath );
  }

  /**
   * Cleanup of the local KV manager
   * Potential tasks:
   * - stop background threads
   * - cleanup storage state, sync
   * - remove intermediate memory registrations
   * ...
   *
   * \return SKV_SUCCESS if successful
   */
  skv_status_t Exit()
  {
    return mLocalKVManager.Exit();
  }

  /**
   * Event retrieval function
   * This function will be called by the main server routine to ask for
   * any pending events that might wait for processing
   *
   * \return pointer to the event that's to be processed,
   *         the pointer can only be reused after a corresponding
   *         AckEvent() has been called
   */
  skv_local_kv_event_t* GetEvent()
  {
    return mLocalKVManager.GetEvent();
  }

  /**
   * Event Acknowledgement function
   * This function will be called by the main server routine to ack an Event
   * and allow the back end to release/invalidate the event pointer
   *
   * \param[in] aEvent  Event pointer to release
   *
   * \return  SKV_SUCCESS if event successfully released
   */
  skv_status_t AckEvent( skv_local_kv_event_t *aEvent )
  {
    return mLocalKVManager.AckEvent( aEvent );
  }

  /**
   * Cancellation of a request context
   * Allows the SKV framework to cancel a request context that was
   * previously created via a local kv cookie
   */
  skv_status_t CancelContext( skv_local_kv_req_ctx_t *aReqCtx )
  {
    return mLocalKVManager.CancelContext( aReqCtx );
  }

  skv_status_t GetDistribution( skv_distribution_t **aDist,
                                skv_local_kv_cookie_t *aCookie )
  {
    return mLocalKVManager.GetDistribution( aDist, aCookie );
  }

  /* insertion of data
   * - prepare the insertion of data (some backend might support direct rdma access and we want to know where the data should go)
   * - do an insert attempt and try to complete immediately or return with special status for deferred action
   * - continue a multi-stage/large insertion that was deferred (or just check if we're complete already)
   */
  //  skv_status_t InsertPrepare();

  /* Request-based insertion
   * parses a request according to the CmdStatus and either does the insert
   * immediately or returns the target memory destination for the insert
   */
  skv_status_t Insert( skv_cmd_RIU_req_t *aReq,
                       skv_status_t aCmdStatus,
                       skv_lmr_triplet_t *aStoredValueRep,
                       skv_lmr_triplet_t *aValueRDMADest,
                       skv_local_kv_cookie_t *aCookie )
  {
    return mLocalKVManager.Insert( aReq, aCmdStatus, aStoredValueRep, aValueRDMADest, aCookie );
  }

  skv_status_t InsertPostProcess( skv_local_kv_req_ctx_t *aReqCtx,
                                  skv_lmr_triplet_t *aValueRDMADest,
                                  skv_local_kv_cookie_t *aCookie )
  {
    return mLocalKVManager.InsertPostProcess( aReqCtx, aValueRDMADest, aCookie );
  }


  /* Buffer-based insertion */
  skv_status_t Insert( skv_pds_id_t& aPDSId,
                       char* aRecordRep,
                       int aKeySize,
                       int aValueSize,
                       skv_local_kv_cookie_t *aCookie )
  {
    return mLocalKVManager.Insert( aPDSId, aRecordRep, aKeySize, aValueSize, aCookie );
  }

  skv_status_t BulkInsert( skv_pds_id_t aPDSId,
                           skv_lmr_triplet_t *aLocalBuffer,
                           skv_local_kv_cookie_t *aCookie )
  {
    return mLocalKVManager.BulkInsert( aPDSId, aLocalBuffer, aCookie );
  }


  /* Retrieval of data
   * - prepare the retrieval of data (some backend might support direct rdma access and we want to know where the data should be picked)
   * - do an actual retrieve and try to complete immediately (e.g. inline data) or return with special status for deferred action
   * - continue a multi-stage/large retrieval that was deferred (or just check if we're complete already)
   */
  // skv_status_t RetrievePrepare();
  skv_status_t Retrieve( skv_pds_id_t aPDSId,
                         char* aKeyData,
                         int aKeySize,
                         int aValueOffset,
                         int aValueSize,
                         skv_cmd_RIU_flags_t aFlags,
                         skv_lmr_triplet_t* aStoredValueRep,
                         int *aTotalSize,
                         skv_local_kv_cookie_t *aCookie )
  {
    return mLocalKVManager.Retrieve( aPDSId, aKeyData, aKeySize, aValueOffset, aValueSize, aFlags, aStoredValueRep, aTotalSize, aCookie );
  }

  skv_status_t RetrievePostProcess(   skv_local_kv_req_ctx_t *aReqCtx )
  {
    return mLocalKVManager.RetrievePostProcess( aReqCtx );
  }

  skv_status_t RetrieveNKeys( skv_pds_id_t aPDSId,
                              char * aStartingKeyData,
                              int aStartingKeySize,
                              skv_lmr_triplet_t* aRetrievedKeysSizesSegs,
                              int* aRetrievedKeysCount,
                              int* aRetrievedKeysSizesSegsCount,
                              int aListOfKeysMaxCount,
                              skv_cursor_flags_t aFlags,
                              skv_local_kv_cookie_t *aCookie )
  {
    return mLocalKVManager.RetrieveNKeys( aPDSId, aStartingKeyData, aStartingKeySize,
                                          aRetrievedKeysSizesSegs, aRetrievedKeysCount,
                                          aRetrievedKeysSizesSegsCount, aListOfKeysMaxCount,
                                          aFlags,
                                          aCookie );
  }



  /* Lookup a key/partial key
   * - flag to return RDMAable location of record for RDMA read and/or write access
   *   could also be staging buffer address for large records
   */
  skv_status_t Lookup( skv_pds_id_t aPDSId,
                       char *aKeyPtr,
                       int aKeySize,
                       skv_cmd_RIU_flags_t aFlags,
                       skv_lmr_triplet_t *aStoredValueRep,
                       skv_local_kv_cookie_t *aCookie )
  {
    return mLocalKVManager.Lookup( aPDSId, aKeyPtr, aKeySize, aFlags, aStoredValueRep, aCookie );
  }

  /* Remove */
  skv_status_t Remove( skv_pds_id_t aPDSId,
                       char* aKeyData,
                       int aKeySize,
                       skv_local_kv_cookie_t *aCookie )
  {
    return mLocalKVManager.Remove( aPDSId, aKeyData, aKeySize, aCookie );
  }


  /* PDS Access */
  skv_status_t PDS_Open( char *aPDSName,
                         skv_pds_priv_t aPrivs,
                         skv_cmd_open_flags_t aFlags,
                         skv_pds_id_t *aPDSId,
                         skv_local_kv_cookie_t *aCookie )
  {
    return mLocalKVManager.PDS_Open( aPDSName, aPrivs, aFlags, aPDSId, aCookie );
  }

  skv_status_t PDS_Stat( skv_pdscntl_cmd_t aCmd,
                         skv_pds_attr_t *aPDSAttr,
                         skv_local_kv_cookie_t *aCookie )
  {
    return mLocalKVManager.PDS_Stat( aCmd, aPDSAttr, aCookie );
  }

  skv_status_t PDS_Close( skv_pds_attr_t *aPDSAttr, skv_local_kv_cookie_t *aCookie )
  {
    return mLocalKVManager.PDS_Close( aPDSAttr, aCookie );
  }

  // skv_status_t PDS_Remove();

  skv_status_t CreateCursor( char* aBuff,
                             int aBuffSize,
                             skv_server_cursor_hdl_t *aCursorHandle,
                             skv_local_kv_cookie_t *aCookie )
  {
    return mLocalKVManager.CreateCursor( aBuff, aBuffSize, aCursorHandle, aCookie );
  }

  /*************************************************************
   * !!!! SYNCHRONOUS FUNCTIONS !!!!
   *************************************************************/

  /* Lock/Unlock */
  skv_status_t Lock( skv_pds_id_t *aPDSId,
                     skv_key_value_in_ctrl_msg_t *aKeyValue,
                     skv_rec_lock_handle_t *aRecLock )
  {
    return mLocalKVManager.Lock( aPDSId, aKeyValue, aRecLock );
  }
  skv_status_t Unlock( skv_rec_lock_handle_t aLock )
  {
    return mLocalKVManager.Unlock( aLock );
  }

  /************************************************************/
  /* Special/Other functionality */

  /*
   * Allocation of temporary buffer space, e.g. for transfer of bulk-data
   * THIS IS A SYNC METHOD
   */
  skv_status_t Allocate( int aBuffSize,
                         skv_lmr_triplet_t *aRDMARep )
  {
    return mLocalKVManager.Allocate( aBuffSize, aRDMARep );
  }

  /*
   * Deallocation of temporary buffer space, e.g. for transfer of bulk-data
   * THIS IS A SYNC METHOD
   */
  skv_status_t Deallocate ( skv_lmr_triplet_t *aRDMARep )
  {
    return mLocalKVManager.Deallocate( aRDMARep );
  }

  skv_status_t RDMABoundsCheck( const char* aContext,
                                char* aMem,
                                int aSize )
  {
    return mLocalKVManager.RDMABoundsCheck( aContext, aMem, aSize );
  }

  /*
   * Store an image of the current local KV data to the given checkpointpath
   */
  skv_status_t DumpImage( char* aCheckpointPath )
  {
    return mLocalKVManager.DumpImage( aCheckpointPath );
  }
};



/*
 * Type specification of the local-kv type
 */
typedef skv_local_kv_interface<SKV_SERVER_LOCAL_KV> skv_local_kv_t;


#endif /* SKV_LOCAL_KV_INTERFACE_HPP_ */
