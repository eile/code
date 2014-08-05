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

#ifndef __SKV_ARRAY_STACK_HPP__
#define __SKV_ARRAY_STACK_HPP__

#include <FxLogger.hpp>

// #ifndef SKV_ARRAY_STACK_SIZE
// #error "Required to set SKV_ARRAY_STACK_SIZE before including skv_stack.hpp"
// #endif

template<class T, size_t SKV_ARRAY_STACK_SIZE>
class skv_array_stack_t
{
  int len;
  T *Memory;

public:
  skv_array_stack_t()
  {
    len = 0;
    Memory = new T[SKV_ARRAY_STACK_SIZE + 1];
  }

  ~skv_array_stack_t()
  {
    delete[] Memory;
  }

  void push( const T element )
  {
    Memory[len] = element;

    AssertLogLine( len < SKV_ARRAY_STACK_SIZE )
      << "skv_array_stack_t::push():  Stack overflow"
      << " size: " << len
      << " max: " << SKV_ARRAY_STACK_SIZE
      << EndLogLine;

    len++;
  }

  T top()
  {
    return Memory[len - 1];
  }

  void pop()
  {
    AssertLogLine( len > 0 )
      << "skv_array_stack_t::pop(): Stack underflow"
      << EndLogLine;

    len--;
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

#endif // __SKV_ARRAY_STACK_HPP__
