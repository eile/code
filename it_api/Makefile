########################################
# Copyright (c) IBM Corp. 2007-2014
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v1.0
# which accompanies this distribution, and is available at
# http://www.eclipse.org/legal/epl-v10.html
#
# Contributors:
#     arayshu, lschneid - initial implementation
########################################

ifndef TOP
    TOP = $(shell cd ..; pwd)
    export TOP
endif

include $(TOP)/it_api/make.conf

MAX_JOBS=$(shell grep -c '^processor' /proc/cpuinfo)

BUILD=${IT_API_DEST}

.PHONY: $(BUILD) all

all:
	@set -e; for i in $(BUILD); do \
		make -C $$i; \
	done

install:
	@set -e; for i in $(BUILD); do \
		make -C $$i install; \
	done



.PHONY: clean distclean
clean:
	@set -e; for subdest in $(BUILD); do \
		${MAKE} --directory $$subdest $@; \
	done

distclean:
	@set -e; for subdest in $(BUILD); do \
		${MAKE} --directory $$subdest $@; \
	done
	-rm -f make.conf


$(TOP)/it_api/make.conf:
	./bootstrap.sh

