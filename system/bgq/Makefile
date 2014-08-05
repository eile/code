########################################
#Copyright (c) IBM Corp. 2007-2014
#All rights reserved. This program and the accompanying materials
#are made available under the terms of the Eclipse Public License v1.0
#which accompanies this distribution, and is available at
#http://www.eclipse.org/legal/epl-v10.html
#
#Contributors:
#    arayshu, lschneid - initial implementation
########################################
#skv top level makefile to BUILD AND INSTALL all components including test

ifndef TOP
    TOP = $(shell while ! test -e make.conf.in; do cd ..; done; pwd)
    export TOP
endif

# picks up SKV-specific settings
# e.g.: SKV_DEST, SKV_BASE_DIR, SKV_GLOBAL_*FLAGS, ...
include ${TOP}/make.conf


SKV_FILES=skv.env skv skv_server.conf tests



.PHONY: install ${SKV_FILES}
all: install

build:
	@echo "nothing to be done for build in ${CURDIR}"


skv.env:
	${VV}echo ". /opt/${SKV_MPI_IMPL}/etc/env${SKV_VERBS_PROVIDER}" > ${SKV_BUILD_DIR}/$@
	${VV}cat etc/skv.env.in >> ${SKV_BUILD_DIR}/$@

	install -D -m 644 ${SKV_BUILD_DIR}/$@ ${SKV_INSTALL_DIR}/etc/skv.env


skv:
	install -D -m 755 etc/init.d/skv ${ASF_ROOTFS_DIR}/etc/init.d/skv 

skv_server.conf: etc/skv_server.conf
	install -D -m 644 etc/skv_server.conf ${ASF_ROOTFS_DIR}/etc/skv_server.conf


tests:
	install -v -D -m 755 ${SKV_BASE_DIR}/scripts/insert_retrieve_test_async.sh ${SKV_INSTALL_DIR}/scripts/insert_retrieve_test_async.sh
	install -v -D -m 755 ${SKV_BASE_DIR}/scripts/short_command_test.sh 				 ${SKV_INSTALL_DIR}/scripts/short_command_test.sh
	install -v -D -m 755 ${SKV_BASE_DIR}/scripts/data_get_and_check.sh 				 ${SKV_INSTALL_DIR}/scripts/data_get_and_check.sh
	install -v -D -m 755 ${SKV_BASE_DIR}/scripts/run_ior_test.sh 			         ${SKV_INSTALL_DIR}/scripts/run_ior_test.sh
	install -v -D -m 644 ${SKV_BASE_DIR}/scripts/gen_ior_runs.awk 			         ${SKV_INSTALL_DIR}/scripts/gen_ior_runs.awk
	install -v -D -m 644 ${SKV_BASE_DIR}/scripts/bench_skv.ior 			           ${SKV_INSTALL_DIR}/testcases/bench_skv.ior
	install -v -D -m 644 ${SKV_BASE_DIR}/scripts/bench_gpfs.ior 			           ${SKV_INSTALL_DIR}/../gpfs/testcases/bench_gpfs.ior




install: ${SKV_FILES}




.PHONY: clean distclean
clean distclean:
	${VV}-rm -f ${SKV_BUILD_DIR}/skv.env
