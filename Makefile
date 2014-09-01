MAIN   = cql.rs
CRATES = src/lib.rs

TO_LIB = $(addprefix lib/, $(shell rustc --print-file-name $(1)))

LIBS   = $(foreach crate, $(CRATES), $(call TO_LIB, $(crate)))

all: exe

exe: $(MAIN) $(LIBS)
	rustc -g --out-dir bin -L lib $(MAIN)


define COMPILE_CRATE
$(call TO_LIB, $(1)): $(1)
	rustc -g --crate-type=lib --out-dir lib $(1)
endef

$(foreach crate, $(CRATES), $(eval $(call COMPILE_CRATE, $(crate))))
