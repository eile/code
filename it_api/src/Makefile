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

#set default TOP-level dir not set yet
ifndef TOP
    TOP = $(shell cd ../../; pwd)
    export TOP
endif

# picks up IT-API-specific settings
# e.g.: IT_API_DEST, IT_API_BASE_DIR, IT_API_GLOBAL_*FLAGS, ...
include ${TOP}/it_api/make.conf


SRC_CPP=it_api_o_verbs.cpp it_api_o_verbs_thread.cpp

IT_API_CXXFLAGS=-O2 -g ${ASF_GLOBAL_CXXFLAGS} -I${IT_API_INCLUDE_DIR} ${IT_API_GLOBAL_CXXFLAGS}

OBJ=$(SRC_CPP:%.cpp=${IT_API_BUILD_DIR}/%.o)
DEP=$(SRC_CPP:%.cpp=${IT_API_BUILD_DIR}/%.d)

all: build

prebuild:
	@echo "Creating build dir: ${IT_API_BUILD_DIR}"
	@if [ ! -e ${IT_API_BUILD_DIR} ]; then \
		mkdir -p ${IT_API_BUILD_DIR}; \
	fi


build: prebuild ${OBJ} ${IT_API_BUILD_DIR}/libit_api_o_verbs.a
	@echo "Finished common build..."


install: build
	@echo "Starting install now..."


${IT_API_BUILD_DIR}/libit_api_o_verbs.a: ${OBJ}
	ar r $@ $^



${IT_API_BUILD_DIR}/%.o: %.cpp
	$(CXX) -c $(IT_API_CXXFLAGS) $< -o $@




.PHONY: clean distclean
clean distclean:
	-rm ${OBJ} ${DEP} ${IT_API_BUILD_DIR}/libit_api_o_verbs.a





# rule to create automatic dependency list for all files
${IT_API_BUILD_DIR}/%.d: %.cpp
	-$(SHELL) -ec '$(CXX) -MM $(IT_API_CXXFLAGS) $< \
                      | sed "s#\($*\)\.o[ :]*#${IT_API_BUILD_DIR}/\1.o $@ : #g" > $@'


-include ${DEP}
