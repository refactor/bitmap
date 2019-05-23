PROJECT = ebitmap
PROJECT_DESCRIPTION = bitmap for erlang
PROJECT_VERSION = 0.1.0

SHELL=/bin/bash
SHELL_DEPS = kjell recon
SHELL_ERL = $(DEPS_DIR)/kjell/bin/kjell

dep_recon_commit = 2.5.0

CFLAGS += -DNIF_DEBUG

include erlang.mk
