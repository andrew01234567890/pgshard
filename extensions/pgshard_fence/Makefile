MODULE_big = pgshard_fence
OBJS = pgshard_fence.o
EXTENSION = pgshard_fence
DATA = pgshard_fence--1.0.sql
SHLIB_LINK = -lrt

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
