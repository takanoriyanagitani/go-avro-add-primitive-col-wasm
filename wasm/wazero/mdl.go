package w0

import (
	"errors"
	"fmt"

	wa "github.com/tetratelabs/wazero/api"
)

var ErrInvalidModule error = errors.New("invalid module")

type Module struct {
	wa.Module
	FunctionName string
}

func (m Module) ToFunction() (Function, error) {
	var f wa.Function = m.Module.ExportedFunction(m.FunctionName)
	switch f {
	case nil:
		return Function{}, fmt.Errorf(
			"%w: name=%s",
			ErrInvalidModule,
			m.FunctionName,
		)
	default:
		return Function{Function: f}, nil
	}
}
