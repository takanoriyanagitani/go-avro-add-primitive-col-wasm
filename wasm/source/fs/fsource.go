package fsource

import (
	"bytes"
	"io"
	"os"
	"path/filepath"

	. "github.com/takanoriyanagitani/go-avro-add-primitive-col-wasm/util"
	ws "github.com/takanoriyanagitani/go-avro-add-primitive-col-wasm/wasm/source"
)

type FsConfig struct {
	ModuleDir       string
	MaxWasmByteSize int64
}

func FilenameToBytesLimited(limit int64) func(string) IO[[]byte] {
	return Lift(func(filename string) ([]byte, error) {
		f, e := os.Open(filename)
		if nil != e {
			return nil, e
		}

		limited := &io.LimitedReader{
			R: f,
			N: limit,
		}

		var buf bytes.Buffer
		_, e = io.Copy(&buf, limited)
		return buf.Bytes(), e
	})
}

func (f FsConfig) ToWasmSource() ws.WasmSource {
	return func(name ws.ModuleName) IO[ws.WasmBytes] {
		var basename string = string(name) + ".wasm"
		var fullpath string = filepath.Join(f.ModuleDir, basename)
		return Bind(
			FilenameToBytesLimited(f.MaxWasmByteSize)(fullpath),
			Lift(func(b []byte) (ws.WasmBytes, error) {
				return b, nil
			}),
		)
	}
}
