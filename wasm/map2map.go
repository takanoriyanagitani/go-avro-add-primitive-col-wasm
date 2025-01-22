package wsm

import (
	"context"
	"iter"

	. "github.com/takanoriyanagitani/go-avro-add-primitive-col-wasm/util"
)

type MapToMap func(map[string]any) IO[map[string]any]

func (m MapToMap) MapsToMaps(
	original iter.Seq2[map[string]any, error],
) IO[iter.Seq2[map[string]any, error]] {
	return func(ctx context.Context) (iter.Seq2[map[string]any, error], error) {
		return func(yield func(map[string]any, error) bool) {
			for row, e := range original {
				if nil != e {
					yield(nil, e)
					return
				}

				mapd, e := m(row)(ctx)
				if !yield(mapd, e) {
					return
				}
			}
		}, nil
	}
}
