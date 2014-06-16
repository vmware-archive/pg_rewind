# Makefile for pg_rewind
#
# Copyright (c) 2013 VMware, Inc. All Rights Reserved.
#

PGFILEDESC = "pg_rewind - repurpose an old master server as standby"
PGAPPICON = win32

PROGRAM = pg_rewind
OBJS	= pg_rewind.o parsexlog.o xlogreader.o util.o datapagemap.o timeline.o \
	fetch.o copy_fetch.o libpq_fetch.o filemap.o

REGRESS = basictest extrafiles databases
REGRESS_OPTS=--use-existing --launcher=./launcher

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport)

override CPPFLAGS := -DFRONTEND $(CPPFLAGS)

EXTRA_CLEAN = $(RMGRDESCSOURCES) xlogreader.c

all: pg_rewind checksrcdir

# This rule's only purpose is to give the user instructions on how to pass
# the path to PostgreSQL source tree to the makefile.
.PHONY: checksrcdir check-local check-remote check-all
checksrcdir:
ifdef USE_PGXS
ifndef top_srcdir
	@echo "You must have PostgreSQL source tree available to compile."
	@echo "There are two ways to make that available:"
	@echo "1. Put pg_rewind project directory inside PostgreSQL source tree as"
	@echo "contrib/pg_rewind, and use \"make\" to compile"
	@echo "or"
	@echo "2. Pass the path to the PostgreSQL source tree to make, in the top_srcdir"
	@echo "variable: \"make USE_PGXS=1 top_srcdir=<path to PostgreSQL source tree>\""
	@exit 1
endif
endif

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_rewind
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# xlogreader.c is symlinked from the PostgreSQL sources.
xlogreader.c: % : $(top_srcdir)/src/backend/access/transam/%
	rm -f $@ && $(LN_S) $< .

check-local:
	echo "Running tests against local data directory, in copy-mode"
	bindir=$(bindir) TEST_SUITE="local" $(MAKE) installcheck

check-remote:
	echo "Running tests against a running standby, via libpq"
	bindir=$(bindir) TEST_SUITE="remote" $(MAKE) installcheck

check-both: check-local check-remote
