package bin2col

import (
	"context"
	"database/sql"
	"errors"

	. "github.com/takanoriyanagitani/go-avro-add-primitive-col-wasm/util"
	wb "github.com/takanoriyanagitani/go-avro-add-primitive-col-wasm/wasm/binary"
	w0 "github.com/takanoriyanagitani/go-avro-add-primitive-col-wasm/wasm/wazero"
)

type ColumnsToColumn func(w0.TypeAlias, w0.TypeAlias) IO[w0.TypeAlias]

type ColumnsToColumnN func(
	sql.Null[w0.TypeAlias],
	sql.Null[w0.TypeAlias],
) IO[sql.Null[w0.TypeAlias]]

func (c ColumnsToColumn) ToNullable() ColumnsToColumnN {
	return func(
		x sql.Null[w0.TypeAlias],
		y sql.Null[w0.TypeAlias],
	) IO[sql.Null[w0.TypeAlias]] {
		return func(ctx context.Context) (sql.Null[w0.TypeAlias], error) {
			if x.Valid && y.Valid {
				var vx w0.TypeAlias = x.V
				var vy w0.TypeAlias = y.V
				vz, e := c(vx, vy)(ctx)
				return sql.Null[w0.TypeAlias]{
					Valid: nil == e,
					V:     vz,
				}, e
			}
			return sql.Null[w0.TypeAlias]{
				Valid: false,
				V:     0,
			}, nil
		}
	}
}

type Config struct {
	w0.NullableToAny
	w0.AnyToTypeAlias
}

func (c Config) AnyToNullable(a any) (sql.Null[w0.TypeAlias], error) {
	return c.AnyToTypeAlias(a)
}

func (c Config) Null2any(n sql.Null[w0.TypeAlias]) (any, error) {
	var i w0.NullType = w0.NullType(n)
	return c.NullableToAny(i)
}

func (c Config) ToAny(conv ColumnsToColumnN) wb.ColumnsToColumn {
	return func(x any, y any) IO[any] {
		return func(ctx context.Context) (any, error) {
			nx, ex := c.AnyToNullable(x)
			ny, ey := c.AnyToNullable(y)
			nz, ez := conv(nx, ny)(ctx)
			az, ea := c.Null2any(nz)
			return az, errors.Join(ex, ey, ez, ea)
		}
	}
}

type BinaryFunc w0.Function

func (b BinaryFunc) Call2(
	ctx context.Context,
	x w0.TypeAlias,
	y w0.TypeAlias,
) (w0.TypeAlias, error) {
	var ux uint64 = uint64(x)
	var uy uint64 = uint64(y)
	var f w0.Function = w0.Function(b)
	uz, e := f.Call(ctx, ux, uy)
	return w0.TypeAlias(uz), e
}

func (b BinaryFunc) ToColumnsToColumn() ColumnsToColumn {
	return func(x, y w0.TypeAlias) IO[w0.TypeAlias] {
		return func(ctx context.Context) (w0.TypeAlias, error) {
			return b.Call2(ctx, x, y)
		}
	}
}
