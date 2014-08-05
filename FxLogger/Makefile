########################################
# Copyright (c) IBM Corp. 1997-2014
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v1.0
# which accompanies this distribution, and is available at
# http://www.eclipse.org/legal/epl-v10.html
#
########################################

CXXFLAGS = -I. -IPk \
	-DPKTRACE_WORKING \
	-D NO_SETPRI \
	-D GFE_MPI \
	-g \
	-D PKFXLOG \
        -D PKTRACE_WORKING \
        -D PKTRACE_ON \
	-D _THREAD_SAFE=1 \
	-fpermissive \
	-DPK_BGP \
	-DPK_LINUX \
	-DPKFXLOG \
	-DPK_BGP \
	-DPK_LINUX \
	-g \
  -DONLY_MSPACES \


SRCS = FxLogger.cpp \
	Trace.cpp



CFLAGS=-I. -IPk \
	-DPKTRACE_WORKING \
	-D NO_SETPRI \
	-D GFE_MPI \
	-g \
	-D PKFXLOG \
        -D PKTRACE_WORKING \
        -D PKTRACE_ON \
	-D _THREAD_SAFE=1 \
	-DPK_BGP \
	-DPK_LINUX \
	-DPKFXLOG \
	-DPK_BGP \
	-DPK_LINUX \
	-g \
  -DONLY_MSPACES \


C_SRCS=dlmalloc.c

OBJS=$(SRCS:%.cpp=%.o) $(C_SRCS:%.c=%.o)

libPkLinux.a: $(OBJS)
	ar r $@ $^

install: libPkLinux.a


clean:
	rm $(OBJS) $(SRCS:.cpp=.d) libPkLinux.a


#
# generic dependency rules
#
%.d: %.c
	$(SHELL) -ec '$(CC) -M $(CFLAGS) $< | sed "s,$*.o,& $@,g" > $@'

%.d: %.cpp
	$(SHELL) -ec '$(CC) -M $(CXXFLAGS) $< | sed "s,$*.o,& $@,g" > $@'

# include dependency files
-include $(SRCS:.cpp=.d)
