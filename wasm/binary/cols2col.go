package bin

import (
	"context"

	. "github.com/takanoriyanagitani/go-avro-add-primitive-col-wasm/util"
	ws "github.com/takanoriyanagitani/go-avro-add-primitive-col-wasm/wasm"
)

type ColumnsToColumn func(any, any) IO[any]

type Config struct {
	ColumnNameX string
	ColumnNameY string

	ColumnNameZ string
}

func (c Config) ToMapToMap(c2c ColumnsToColumn) ws.MapToMap {
	buf := map[string]any{}
	return func(original map[string]any) IO[map[string]any] {
		return func(ctx context.Context) (map[string]any, error) {
			clear(buf)

			for key, val := range original {
				buf[key] = val
			}

			var ax any = original[c.ColumnNameX]
			var ay any = original[c.ColumnNameY]

			az, e := c2c(ax, ay)(ctx)

			if nil != e {
				return buf, e
			}

			buf[c.ColumnNameZ] = az

			return buf, nil
		}
	}
}
