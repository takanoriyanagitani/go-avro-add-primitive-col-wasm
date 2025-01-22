package w0

import (
	"context"
	"errors"

	. "github.com/takanoriyanagitani/go-avro-add-primitive-col-wasm/util"
	wa "github.com/tetratelabs/wazero/api"
)

var ErrInvalidResults error = errors.New("invalid results")

type Function struct {
	wa.Function
}

func (f Function) Call(ctx context.Context, params ...uint64) (uint64, error) {
	return Bind(
		func(ctx context.Context) ([]uint64, error) {
			return f.Function.Call(ctx, params...)
		},
		Lift(func(results []uint64) (uint64, error) {
			if 1 != len(results) {
				return 0, ErrInvalidResults
			}
			return results[0], nil
		}),
	)(ctx)
}
