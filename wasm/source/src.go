package source

import (
	. "github.com/takanoriyanagitani/go-avro-add-primitive-col-wasm/util"
)

type ModuleName string

type WasmBytes []byte

type WasmSource func(ModuleName) IO[WasmBytes]
