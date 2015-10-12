/************************************************
 * Copyright (c) IBM Corp. 2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/

/*
 * skv_local_kv_request_queue.hpp
 *
 *  Created on: May 20, 2014
 *      Author: lschneid
 */

#ifndef SKV_LOCAL_KV_REQUEST_QUEUE_HPP_
#define SKV_LOCAL_KV_REQUEST_QUEUE_HPP_

#ifndef SKV_LOCAL_KV_QUEUES_LOG
#define SKV_LOCAL_KV_QUEUES_LOG ( 0 | SKV_LOGGING_ALL )
#endif

#define SKV_LOCAL_KV_MAX_REQUESTS ( 1048576 )

#include <algorithm>

class skv_local_kv_request_queue_t {
  skv_array_stack_t< skv_local_kv_request_t*, SKV_LOCAL_KV_MAX_REQUESTS > mFreeRequests;
  skv_array_queue_t< skv_local_kv_request_t*, SKV_LOCAL_KV_MAX_REQUESTS > mActiveRequests;
  skv_local_kv_request_t *mRequestPool;
  size_t mLenght;
  skv_mutex_t mLock;
  pthread_cond_t  mCond;

  volatile int mFreeSlots;

public:
  skv_local_kv_request_queue_t( const size_t aSize = SKV_LOCAL_KV_MAX_REQUESTS ) : mFreeSlots( 0 ), mLenght( std::min( (size_t)aSize, (size_t)SKV_LOCAL_KV_MAX_REQUESTS ) )
  {
    mRequestPool = new skv_local_kv_request_t[ mLenght ];
    pthread_cond_init( &mCond, 0 );
  }
  ~skv_local_kv_request_queue_t()
  {
    pthread_cond_destroy( &mCond );
    if( mRequestPool != NULL )
      delete [] mRequestPool;
  }

  skv_status_t Init()
  {
    if( mRequestPool == NULL )
      mRequestPool = new skv_local_kv_request_t[ mLenght ];

    for( int i=0; i < mLenght; i++ )
    {
      mRequestPool[ i ].mType = SKV_LOCAL_KV_REQUEST_TYPE_UNKNOWN;
      mFreeRequests.push( &(mRequestPool[ i ]) );
    }
    mFreeSlots = mLenght;

    return SKV_SUCCESS;
  }

  skv_status_t Exit()
  {
    return SKV_SUCCESS;
  }

  skv_local_kv_request_t* AcquireRequestEntry()
  {
    if( mFreeRequests.empty() || (mActiveRequests.size() > mLenght-2) )
      return NULL;

    mLock.lock();
    skv_local_kv_request_t *Req = mFreeRequests.top();
    --mFreeSlots;
    mFreeRequests.pop();
    mLock.unlock();

    return Req;
  }
  void QueueRequest( skv_local_kv_request_t *aReq )
  {
    mLock.lock();
    mActiveRequests.push( aReq );
    mLock.unlock();
    pthread_cond_signal( &mCond );
  }
  bool IsEmpty()
  {
    return mActiveRequests.empty();
  }
  skv_local_kv_request_t* GetRequest()
  {
    skv_local_kv_request_t *Request = NULL;

    mLock.lock();
    while( mActiveRequests.empty( ))
        pthread_cond_wait( &mCond, &mLock.mMutex );

    Request = mActiveRequests.front();
    mActiveRequests.pop();

    BegLogLine( SKV_LOCAL_KV_QUEUES_LOG )
        << "skv_local_kv_request_queue_t::GetRequest() Request fetched"
        << " @" << (void*)Request
        << EndLogLine;
    mLock.unlock();

    return Request;
  }

  skv_status_t AckRequest( skv_local_kv_request_t* aRequest )
  {
    mLock.lock();
    mFreeRequests.push( aRequest );
    ++mFreeSlots;
    mLock.unlock();

    BegLogLine( SKV_LOCAL_KV_QUEUES_LOG )
      << "skv_local_kv_request_queue_t::AckRequest() Request returned"
      << " @" << (void*)aRequest
      << EndLogLine;

    return SKV_SUCCESS;
  }

  inline int GetFreeSlots() const
  {
    return mFreeSlots;
  }
};

// for now, just start with a round-robin implementation of a list of worker-request queues
// later we might consider a "shortest-queue-first" scheme or other scheduling too
class skv_local_kv_request_queue_list_t
{
  skv_local_kv_request_queue_t **mSortedRequestQueueList;
  skv_local_kv_request_queue_t *mBestQueue;
  uint64_t mCurrentIndex;
  unsigned int mActiveQueueCount;
  unsigned int mMaxQueueCount;

public:
  skv_local_kv_request_queue_list_t( const unsigned int aMaxQueueCount )
  {
    mMaxQueueCount = aMaxQueueCount;
    mSortedRequestQueueList = new skv_local_kv_request_queue_t* [ mMaxQueueCount ];
    mCurrentIndex = 0;
    mActiveQueueCount = 0;
    mBestQueue = NULL;
  }
  ~skv_local_kv_request_queue_list_t() {}

  void sort()
  {
    // sort the list start with highest number of FreeSlots
  }

  void AddQueue( const skv_local_kv_request_queue_t * aQueue )
  {
    AssertLogLine( mActiveQueueCount < mMaxQueueCount )
      << "Queue Limit exceeded: current=" << mActiveQueueCount
      << " limit=" << mMaxQueueCount
      << EndLogLine;

    mSortedRequestQueueList[ mActiveQueueCount ] = (skv_local_kv_request_queue_t*)aQueue;
    ++mActiveQueueCount;

    if( mActiveQueueCount == 1 )
      mBestQueue = mSortedRequestQueueList[ 0 ];
  }

  skv_local_kv_request_queue_t * GetBestQueue()
  {
    skv_local_kv_request_queue_t *retval = mBestQueue;

    ++mCurrentIndex;
    mBestQueue = mSortedRequestQueueList[ mCurrentIndex % mActiveQueueCount ];

    return retval;
  }
};


#endif /* SKV_LOCAL_KV_REQUEST_QUEUE_HPP_ */
