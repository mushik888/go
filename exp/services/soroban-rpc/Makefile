
ifndef CARGO_BUILD_TARGET
export CARGO_BUILD_TARGET=$(shell rustc -vV | sed -n 's|host: ||p')
endif

# TODO: is it worth trying to compile statically with CGO?
#       https://github.com/2opremio/soroban-go-rust-preflight-poc/issues/3
ifeq ($(OS),Windows_NT)
# This assumes that the Rust compiler should be using a -gnu target (i.e. MinGW compiler) in Windows
# (I (fons) am not even sure if CGo supports MSVC, see https://github.com/golang/go/issues/20982 )
ARCH_LDFLAGS=-static -lws2_32 -lbcrypt -luserenv
else
	UNAME_S := $(shell uname -s)
	# you cannot compile with -static in macos
	ifneq ($(UNAME_S),Darwin)
		ARCH_LDFLAGS=-static
	endif
endif

export CGO_LDFLAGS=./lib/preflight/target/${CARGO_BUILD_TARGET}/release/libpreflight.a -ldl -lm ${ARCH_LDFLAGS}

.PHONY: all
all: soroban-rpc

lib/preflight/target/${CARGO_BUILD_TARGET}/release/libpreflight.a: lib/preflight.h lib/preflight/Cargo.toml lib/preflight/src/lib.rs
	cd lib/preflight && cargo build --release

soroban-rpc: ./lib/preflight/target/${CARGO_BUILD_TARGET}/release/libpreflight.a lib/preflight.h
	go build -buildvcs=false .

.PHONY: clean
clean:
	rm -rf soroban-rpc lib/preflight/target
