package w0

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	ap "github.com/takanoriyanagitani/go-avro-add-primitive-col-wasm"
	wa "github.com/tetratelabs/wazero/api"
)

var ErrInvalidType error = errors.New("invalid type")

type TypeAlias uint64

type Converter[T, U any] func(T) U

func (c Converter[T, U]) ConvertNullable(i sql.Null[T]) sql.Null[U] {
	var ret sql.Null[U]
	ret.Valid = i.Valid
	if ret.Valid {
		var t T = i.V
		var u U = c(t)
		ret.V = u
	}
	return ret
}

type GenericNullType[T any] sql.Null[T]

func (g GenericNullType[T]) ToAny() any {
	switch g.Valid {
	case true:
		return g.V
	default:
		return nil
	}
}

type NullType sql.Null[TypeAlias]

func (n NullType) ToInt() sql.Null[int32] {
	var i sql.Null[TypeAlias] = sql.Null[TypeAlias](n)
	return ConvertInt.ConvertNullable(i)
}

func (n NullType) ToLong() sql.Null[int64] {
	var i sql.Null[TypeAlias] = sql.Null[TypeAlias](n)
	return ConvertLong.ConvertNullable(i)
}

func (n NullType) ToFloat() sql.Null[float32] {
	var i sql.Null[TypeAlias] = sql.Null[TypeAlias](n)
	return ConvertFloat.ConvertNullable(i)
}

func (n NullType) ToDouble() sql.Null[float64] {
	var i sql.Null[TypeAlias] = sql.Null[TypeAlias](n)
	return ConvertDouble.ConvertNullable(i)
}

func (n NullType) ToTime() sql.Null[time.Time] {
	var i sql.Null[TypeAlias] = sql.Null[TypeAlias](n)
	return ConvertTime.ConvertNullable(i)
}

type NullableToAny func(NullType) (any, error)

func NullableToInt(n NullType) (any, error) {
	return GenericNullType[int32](n.ToInt()).ToAny(), nil
}

func NullableToLong(n NullType) (any, error) {
	return GenericNullType[int64](n.ToLong()).ToAny(), nil
}

func NullableToFloat(n NullType) (any, error) {
	return GenericNullType[float32](n.ToFloat()).ToAny(), nil
}

func NullableToDouble(n NullType) (any, error) {
	return GenericNullType[float64](n.ToDouble()).ToAny(), nil
}

func NullableToTime(n NullType) (any, error) {
	return GenericNullType[time.Time](n.ToTime()).ToAny(), nil
}

func (t TypeAlias) ToFloat32() float32 { return wa.DecodeF32(uint64(t)) }
func (t TypeAlias) ToFloat64() float64 { return wa.DecodeF64(uint64(t)) }

func (t TypeAlias) ToInt32() int32   { return wa.DecodeI32(uint64(t)) }
func (t TypeAlias) ToUint32() uint32 { return wa.DecodeU32(uint64(t)) }
func (t TypeAlias) ToUint64() uint64 { return uint64(t) }
func (t TypeAlias) ToInt64() int64   { return int64(t) }

func (t TypeAlias) ToLong() int64     { return t.ToInt64() }
func (t TypeAlias) ToInt() int32      { return t.ToInt32() }
func (t TypeAlias) ToFloat() float32  { return t.ToFloat32() }
func (t TypeAlias) ToDouble() float64 { return t.ToFloat64() }

func (t TypeAlias) ToTime() time.Time {
	var us int64 = t.ToLong()
	return time.UnixMicro(us)
}

func AliasToInt(t TypeAlias) int32      { return t.ToInt() }
func AliasToLong(t TypeAlias) int64     { return t.ToLong() }
func AliasToFloat(t TypeAlias) float32  { return t.ToFloat() }
func AliasToDouble(t TypeAlias) float64 { return t.ToDouble() }

var ConvertInt Converter[TypeAlias, int32] = AliasToInt

var ConvertLong Converter[TypeAlias, int64] = AliasToLong

var ConvertFloat Converter[TypeAlias, float32] = AliasToFloat

var ConvertDouble Converter[TypeAlias, float64] = AliasToDouble

var ConvertTime Converter[TypeAlias, time.Time] = AliasToTime

func AliasToTime(t TypeAlias) time.Time { return t.ToTime() }

func (t TypeAlias) ToNullable() sql.Null[TypeAlias] {
	return sql.Null[TypeAlias]{
		Valid: true,
		V:     t,
	}
}

func EncodeF32(i float32) TypeAlias { return TypeAlias(wa.EncodeF32(i)) }
func EncodeF64(i float64) TypeAlias { return TypeAlias(wa.EncodeF64(i)) }

func EncodeI32(i int32) TypeAlias  { return TypeAlias(wa.EncodeI32(i)) }
func EncodeI64(i int64) TypeAlias  { return TypeAlias(wa.EncodeI64(i)) }
func EncodeU32(i uint32) TypeAlias { return TypeAlias(wa.EncodeU32(i)) }

var EncodeInt func(int32) TypeAlias = EncodeI32

var EncodeLong func(int64) TypeAlias = EncodeI64

var EncodeFloat func(float32) TypeAlias = EncodeF32

var EncodeDouble func(float64) TypeAlias = EncodeF64

type AnyToTypeAlias func(any) (sql.Null[TypeAlias], error)

func AnyToAlias(a any) (sql.Null[TypeAlias], error) {
	switch typ := a.(type) {

	case nil:
		return sql.Null[TypeAlias]{Valid: false, V: 0}, nil

	case float32:
		return EncodeFloat(typ).ToNullable(), nil

	case float64:
		return EncodeDouble(typ).ToNullable(), nil

	case int:
		return EncodeLong(int64(typ)).ToNullable(), nil

	case int64:
		return EncodeLong(typ).ToNullable(), nil

	case uint32:
		return EncodeLong(int64(typ)).ToNullable(), nil

	case uint8:
		return EncodeInt(int32(typ)).ToNullable(), nil

	case uint16:
		return EncodeInt(int32(typ)).ToNullable(), nil

	case uint64:
		return TypeAlias(typ).ToNullable(), nil

	case time.Time:
		var us int64 = typ.UnixMicro()
		return EncodeLong(us).ToNullable(), nil

	case map[string]any:
		for _, val := range typ {
			return AnyToAlias(val)
		}
		return sql.Null[TypeAlias]{Valid: false, V: 0}, fmt.Errorf(
			"%w: map len=%v",
			ErrInvalidType,
			len(typ),
		)

	default:
		return sql.Null[TypeAlias]{Valid: false, V: 0}, fmt.Errorf(
			"%w: typ=%v",
			ErrInvalidType,
			typ,
		)

	}
}

var AnyToTypeAliasDefault AnyToTypeAlias = AnyToAlias

type PrimTyp ap.PrimitiveType

func (t PrimTyp) ToNullableToAny() NullableToAny {
	switch ap.PrimitiveType(t) {

	case ap.PrimitiveInt:
		return NullableToInt

	case ap.PrimitiveLong:
		return NullableToLong

	case ap.PrimitiveFloat:
		return NullableToFloat

	case ap.PrimitiveDouble:
		return NullableToDouble

	case ap.PrimitiveTime:
		return NullableToTime

	default:
		return func(_ NullType) (any, error) {
			return nil, ErrInvalidType
		}

	}
}
