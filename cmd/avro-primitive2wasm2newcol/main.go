package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strconv"
	"strings"

	ap "github.com/takanoriyanagitani/go-avro-add-primitive-col-wasm"
	sh "github.com/takanoriyanagitani/go-avro-add-primitive-col-wasm/avro/avsc/hamba"
	dh "github.com/takanoriyanagitani/go-avro-add-primitive-col-wasm/avro/dec/hamba"
	eh "github.com/takanoriyanagitani/go-avro-add-primitive-col-wasm/avro/enc/hamba"
	. "github.com/takanoriyanagitani/go-avro-add-primitive-col-wasm/util"
	wm "github.com/takanoriyanagitani/go-avro-add-primitive-col-wasm/wasm"
	wb "github.com/takanoriyanagitani/go-avro-add-primitive-col-wasm/wasm/binary"
	bw "github.com/takanoriyanagitani/go-avro-add-primitive-col-wasm/wasm/binary/wazero"
	ws "github.com/takanoriyanagitani/go-avro-add-primitive-col-wasm/wasm/source"
	sf "github.com/takanoriyanagitani/go-avro-add-primitive-col-wasm/wasm/source/fs"
	w0 "github.com/takanoriyanagitani/go-avro-add-primitive-col-wasm/wasm/wazero"
)

var EnvValByKey func(string) IO[string] = Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var stdin2maps IO[iter.Seq2[map[string]any, error]] = dh.
	StdinToMapsDefault

var moduleDir IO[string] = EnvValByKey("ENV_WASM_MODULE_DIR")

const WasmModuleMaxSizeDefault int = 16777216

var wasmModuleMaxSize IO[int] = Bind(
	EnvValByKey("ENV_WASM_MAX_MODULE_SIZE"),
	Lift(strconv.Atoi),
).Or(Of(WasmModuleMaxSizeDefault))

var fsConfig IO[sf.FsConfig] = Bind(
	moduleDir,
	func(s string) IO[sf.FsConfig] {
		return Bind(
			wasmModuleMaxSize,
			Lift(func(i int) (sf.FsConfig, error) {
				return sf.FsConfig{
					ModuleDir:       s,
					MaxWasmByteSize: int64(i),
				}, nil
			}),
		)
	},
)

var wasmSource IO[ws.WasmSource] = Bind(
	fsConfig,
	Lift(func(cfg sf.FsConfig) (ws.WasmSource, error) {
		return cfg.ToWasmSource(), nil
	}),
)

var moduleName IO[ws.ModuleName] = Bind(
	EnvValByKey("ENV_WASM_MODULE_NAME"),
	Lift(func(s string) (ws.ModuleName, error) {
		return ws.ModuleName(s), nil
	}),
)

var wasmBytes IO[ws.WasmBytes] = Bind(
	wasmSource,
	func(s ws.WasmSource) IO[ws.WasmBytes] {
		return Bind(
			moduleName,
			s,
		)
	},
)

var funcName IO[string] = EnvValByKey("ENV_WASM_FUNC_NAME")

var w0Config IO[w0.Config] = Bind(
	funcName,
	Lift(func(fname string) (w0.Config, error) {
		return w0.ConfigDefault.WithFunctionName(fname), nil
	}),
)

var runtime IO[w0.Runtime] = Bind(
	w0Config,
	func(cfg w0.Config) IO[w0.Runtime] {
		return func(ctx context.Context) (w0.Runtime, error) {
			var rtm w0.Runtime = cfg.ToRuntime(ctx)

			var echan chan error = make(chan error)

			go func() {
				e := <-echan
				if nil != e {
					log.Printf("error on close: %v\n", e)
				}
			}()

			rtm.CloseLater(
				context.Background(),
				ctx.Done(),
				echan,
			)

			return rtm, nil
		}
	},
)

var mdl IO[w0.Module] = Bind(
	runtime,
	func(r w0.Runtime) IO[w0.Module] {
		return Bind(
			wasmBytes,
			func(wasm ws.WasmBytes) IO[w0.Module] {
				return Bind(
					r.Compiled(wasm),
					func(c w0.Compiled) IO[w0.Module] {
						return c.Instantiate(r.Runtime)
					},
				)
			},
		)
	},
)

var fnc IO[w0.Function] = Bind(
	mdl,
	Lift(func(m w0.Module) (w0.Function, error) { return m.ToFunction() }),
)

var bfn IO[bw.BinaryFunc] = Bind(
	fnc,
	Lift(func(f w0.Function) (bw.BinaryFunc, error) {
		return bw.BinaryFunc(f), nil
	}),
)

var cols2coln IO[bw.ColumnsToColumnN] = Bind(
	bfn,
	Lift(func(f bw.BinaryFunc) (bw.ColumnsToColumnN, error) {
		return f.
			ToColumnsToColumn().
			ToNullable(), nil
	}),
)

var target1 IO[string] = EnvValByKey("ENV_TARGET_COL1")

var target2 IO[string] = EnvValByKey("ENV_TARGET_COL2")

var newcolname IO[string] = EnvValByKey("ENV_NEW_COL_NAME")

var cfgxyz IO[wb.Config] = Bind(
	All(
		target1,
		target2,
		newcolname,
	),
	Lift(func(s []string) (wb.Config, error) {
		return wb.Config{
			ColumnNameX: s[0],
			ColumnNameY: s[1],
			ColumnNameZ: s[2],
		}, nil
	}),
)

var schemaFilename IO[string] = EnvValByKey("ENV_SCHEMA_FILENAME")

func FilenameToStringLimited(limit int64) func(string) IO[string] {
	return Lift(func(filename string) (string, error) {
		f, e := os.Open(filename)
		if nil != e {
			return "", e
		}

		limited := &io.LimitedReader{
			R: f,
			N: limit,
		}

		var buf strings.Builder
		_, e = io.Copy(&buf, limited)
		return buf.String(), e
	})
}

const SchemaFileSizeMaxDefault int64 = 1048576

var schemaContent IO[string] = Bind(
	schemaFilename,
	FilenameToStringLimited(SchemaFileSizeMaxDefault),
)

var newcolType IO[ap.PrimitiveType] = Bind(
	All(
		schemaContent,
		newcolname,
	),
	Lift(func(s []string) (ap.PrimitiveType, error) {
		return sh.SchemaToType(s[0], s[1])
	}),
)

var nullable2any IO[w0.NullableToAny] = Bind(
	newcolType,
	Lift(func(t ap.PrimitiveType) (w0.NullableToAny, error) {
		return w0.PrimTyp(t).ToNullableToAny(), nil
	}),
)

var any2alias w0.AnyToTypeAlias = w0.AnyToTypeAliasDefault

var bincfg IO[bw.Config] = Bind(
	nullable2any,
	Lift(func(n w0.NullableToAny) (bw.Config, error) {
		return bw.Config{
			NullableToAny:  n,
			AnyToTypeAlias: any2alias,
		}, nil
	}),
)

var col2cola IO[wb.ColumnsToColumn] = Bind(
	bincfg,
	func(c bw.Config) IO[wb.ColumnsToColumn] {
		return Bind(
			cols2coln,
			Lift(func(conv bw.ColumnsToColumnN) (wb.ColumnsToColumn, error) {
				return c.ToAny(conv), nil
			}),
		)
	},
)

var map2map IO[wm.MapToMap] = Bind(
	col2cola,
	func(conv wb.ColumnsToColumn) IO[wm.MapToMap] {
		return Bind(
			cfgxyz,
			Lift(func(cfg wb.Config) (wm.MapToMap, error) {
				return cfg.ToMapToMap(conv), nil
			}),
		)
	},
)

var mapd IO[iter.Seq2[map[string]any, error]] = Bind(
	map2map,
	func(m wm.MapToMap) IO[iter.Seq2[map[string]any, error]] {
		return Bind(
			stdin2maps,
			m.MapsToMaps,
		)
	},
)

var stdin2avro2maps2mapd2avro2stdout IO[Void] = Bind(
	schemaContent,
	func(s string) IO[Void] {
		return Bind(
			mapd,
			eh.SchemaToMapsToStdoutDefault(s),
		)
	},
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return stdin2avro2maps2mapd2avro2stdout(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
