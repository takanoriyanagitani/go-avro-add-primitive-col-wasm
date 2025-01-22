package w0

import (
	"context"

	wz "github.com/tetratelabs/wazero"
)

type Config struct {
	wz.ModuleConfig
	wz.RuntimeConfig
	FunctionName string
}

func (c Config) ToRuntime(ctx context.Context) Runtime {
	var rtm wz.Runtime = wz.NewRuntimeWithConfig(
		ctx,
		c.RuntimeConfig,
	)
	return Runtime{
		Runtime:      rtm,
		ModuleConfig: c.ModuleConfig,
		FunctionName: c.FunctionName,
	}
}

func (c Config) WithFunctionName(name string) Config {
	c.FunctionName = name
	return c
}

var ModuleConfigDefault wz.ModuleConfig = wz.NewModuleConfig().
	WithName("")

var ConfigDefault Config = Config{
	ModuleConfig:  ModuleConfigDefault,
	RuntimeConfig: wz.NewRuntimeConfig(),
	FunctionName:  "",
}
