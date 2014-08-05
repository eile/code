/************************************************
 * Copyright (c) IBM Corp. 2011-2014
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     lschneid - initial implementation
 *************************************************/

#ifndef __SKV_ARRAY_QUEUE_HPP__
#define __SKV_ARRAY_QUEUE_HPP__

#include <FxLogger.hpp>

template<class T, size_t SKV_ARRAY_QUEUE_SIZE>
class skv_array_queue_t
{
  int len;
  T *Memory;
  int head;
  int tail;

public:
  skv_array_queue_t()
  {
    len = 0;
    Memory = new T[SKV_ARRAY_QUEUE_SIZE + 1];
    head = 0;
    tail = 0;
  }

  ~skv_array_queue_t()
  {
    delete[] Memory;
  }

  void push( const T element )
  {
    Memory[head] = element;
    len++;

    AssertLogLine( len < SKV_ARRAY_QUEUE_SIZE )
      << "skv_array_queue_t::push():  Queue overflow"
      << " size: " << len
      << " max: " << SKV_ARRAY_QUEUE_SIZE
      << EndLogLine;

    head = (head + 1) % SKV_ARRAY_QUEUE_SIZE;
  }

  T front()
  {
    return Memory[tail];
  }

  void pop()
  {
    // AssertLogLine( len > 0 )
    //   << "skv_array_queue_t::pop(): queue underflow"
    //   << EndLogLine;

    if( len > 0 )
    {
      tail = (tail + 1) % SKV_ARRAY_QUEUE_SIZE;
      len--;
    }
  }

  int size()
  {
    return len;
  }

  bool empty()
  {
    return (len == 0);
  }

};

#endif // __SKV_ARRAY_QUEUE_HPP__
