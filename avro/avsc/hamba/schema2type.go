package avsc

import (
	"errors"
	"fmt"

	ha "github.com/hamba/avro/v2"
	ap "github.com/takanoriyanagitani/go-avro-add-primitive-col-wasm"
)

var (
	ErrInvalidField  error = errors.New("invalid field")
	ErrInvalidSchema error = errors.New("invalid schema")
)

func PrimitiveToType(p *ha.PrimitiveSchema) (ap.PrimitiveType, error) {
	var typ ha.Type = p.Type()

	switch typ {

	case ha.Int:
		return ap.PrimitiveInt, nil

	case ha.Long:
		return ap.PrimitiveLong, nil

	case ha.Float:
		return ap.PrimitiveFloat, nil

	case ha.Double:
		return ap.PrimitiveDouble, nil

	default:
		return ap.PrimitiveUnspecified, fmt.Errorf(
			"%w: Type=%v", ErrInvalidField, typ,
		)

	}
}

func UnionToType(u *ha.UnionSchema) (ap.PrimitiveType, error) {
	var typs []ha.Schema = u.Types()
	for _, typ := range typs {
		switch sch := typ.(type) {
		case *ha.PrimitiveSchema:
			return PrimitiveToType(sch)
		default:
			continue
		}
	}
	return ap.PrimitiveUnspecified, fmt.Errorf(
		"%w: schemas=%v",
		ErrInvalidField,
		typs,
	)
}

func FieldToType(f *ha.Field) (ap.PrimitiveType, error) {
	var typ ha.Schema = f.Type()

	switch s := typ.(type) {

	case *ha.PrimitiveSchema:
		return PrimitiveToType(s)

	case *ha.UnionSchema:
		return UnionToType(s)

	default:
		return ap.PrimitiveUnspecified, fmt.Errorf(
			"%w: Schema=%v", ErrInvalidField, s,
		)

	}
}

func FieldsToType(
	fields []*ha.Field,
	colname string,
) (ap.PrimitiveType, error) {
	for _, field := range fields {
		var name string = field.Name()
		if name == colname {
			return FieldToType(field)
		}
	}
	return ap.PrimitiveUnspecified, fmt.Errorf(
		"%w: colname=%s",
		ErrInvalidField,
		colname,
	)
}

func RecordSchemaToType(
	r *ha.RecordSchema,
	colname string,
) (ap.PrimitiveType, error) {
	return FieldsToType(r.Fields(), colname)
}

func SchemaToTypeHamba(
	s ha.Schema,
	colname string,
) (ap.PrimitiveType, error) {
	switch typ := s.(type) {
	case *ha.RecordSchema:
		return RecordSchemaToType(typ, colname)
	default:
		return ap.PrimitiveUnspecified, ErrInvalidSchema
	}
}

func SchemaToType(
	schema string,
	colname string,
) (ap.PrimitiveType, error) {
	parsed, e := ha.Parse(schema)
	if nil != e {
		return ap.PrimitiveUnspecified, e
	}
	return SchemaToTypeHamba(parsed, colname)
}
