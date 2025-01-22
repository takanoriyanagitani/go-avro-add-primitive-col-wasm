package w0

import (
	"context"
	"log"

	. "github.com/takanoriyanagitani/go-avro-add-primitive-col-wasm/util"
	wz "github.com/tetratelabs/wazero"
)

type Runtime struct {
	wz.Runtime
	wz.ModuleConfig
	FunctionName string
}

func (r Runtime) Close(ctx context.Context) error {
	return r.Runtime.Close(ctx)
}

func (r Runtime) CloseLater(
	ctx context.Context,
	done <-chan struct{},
	err chan<- error,
) {
	go func() {
		defer close(err)

		<-done
		e := r.Close(ctx)
		err <- e
	}()
}

func (r Runtime) CloseOnCancel(ctx context.Context) {
	go func() {
		<-ctx.Done()

		e := r.Close(context.Background())
		if nil != e {
			log.Printf("error on close: %v\n", e)
		}
	}()
}

func (r Runtime) ToCompiled(
	ctx context.Context,
	wasm []byte,
) (Compiled, error) {
	compiled, e := r.Runtime.CompileModule(ctx, wasm)
	return Compiled{
		CompiledModule: compiled,
		ModuleConfig:   r.ModuleConfig,
		FunctionName:   r.FunctionName,
	}, e
}

func (r Runtime) Compiled(wasm []byte) IO[Compiled] {
	return func(ctx context.Context) (Compiled, error) {
		return r.ToCompiled(ctx, wasm)
	}
}
