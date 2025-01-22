package w0

import (
	"context"

	. "github.com/takanoriyanagitani/go-avro-add-primitive-col-wasm/util"
	wz "github.com/tetratelabs/wazero"
)

type Compiled struct {
	wz.CompiledModule
	wz.ModuleConfig
	FunctionName string
}

func (c Compiled) ToModule(
	ctx context.Context,
	rtm wz.Runtime,
) (Module, error) {
	mdl, e := rtm.InstantiateModule(ctx, c.CompiledModule, c.ModuleConfig)
	return Module{
		Module:       mdl,
		FunctionName: c.FunctionName,
	}, e
}

func (c Compiled) Instantiate(rtm wz.Runtime) IO[Module] {
	return func(ctx context.Context) (Module, error) {
		return c.ToModule(ctx, rtm)
	}
}
