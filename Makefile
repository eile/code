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
# skv top level makefile to BUILD AND INSTALL all components including test

ifndef TOP
    TOP = $(shell while ! test -e make.conf.in; do cd ..; done; pwd)
    export TOP
endif

# use skv/build as a central build dir for all skv components
export BUILD_DIR=$(CURDIR)/build

# picks up SKV-specific settings
# e.g.: SKV_DEST, SKV_BASE_DIR, SKV_GLOBAL_*FLAGS, ...
include ${TOP}/make.conf


ifndef SKV_MAKE_PROCESSES
	SKV_MAKE_PROCESSES = $(shell grep processor /proc/cpuinfo | wc -l)
	export SKV_MAKE_PROCESSES
endif



.PHONY: install build
all: build

install: build
	@set -e; \
	for subdest in ${SKV_DEST}; do \
		${MAKE} ${DIR_VERBOSE} --directory $$subdest -j ${SKV_MAKE_PROCESSES} $@; \
	done

build: ${SKV_DEST} ${TOP}/FxLogger/libPkLinux.a
	@set -e; \
	for subdest in ${SKV_DEST}; do \
		${MAKE} ${DIR_VERBOSE} --directory $$subdest -j ${SKV_MAKE_PROCESSES} $@; \
	done




.PHONY: clean distclean
clean:
	@set -e; \
	for subdest in ${SKV_DEST}; do \
		${MAKE} ${DIR_VERBOSE} --directory $$subdest $@; \
	done

distclean:
	@set -e; \
	for subdest in ${SKV_DEST}; do \
		${MAKE} ${DIR_VERBOSE} --directory $$subdest $@; \
	done
	-rm -f make.conf


${TOP}/make.conf:
	./bootstrap.sh

${TOP}/FxLogger/libPkLinux.a:
	$(MAKE) -C $(TOP)/FxLogger/

