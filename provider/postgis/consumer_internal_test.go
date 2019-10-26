package postgis

import (
	"text/template"
	"testing"
	"strings"

	"github.com/go-spatial/geom"
	"github.com/go-spatial/tegola/provider"
)

func TestTemplate(t *testing.T) {
	type tcase struct {
		tmpl    string
		feature provider.Feature
		res     string
	}

	fn := func(tc tcase) func(t *testing.T) {
		return func(t *testing.T) {
			tmpl := template.Must(compileTemplate(tc.tmpl))
			wr := &strings.Builder{}
			err := tmpl.Execute(wr, tc.feature)
			if err != nil {
				t.Fatalf("unexpected error %v", err)
			}

			if tc.res != wr.String() {
				t.Fatalf("incorrect output\n\t%v\nexpected\n\t%v",
					wr.String(), tc.res)
			}
		}
	}

	tcases := map[string]tcase{
		"1": {
			tmpl: "{{ AsText .Geometry }}",
			feature: provider.Feature{
				Geometry: geom.Point{1, 1},
			},
			res: "POINT (1 1)",
		},
		"2": {
			tmpl: "{{ AsBinary .Geometry }}",
			feature: provider.Feature{
				Geometry: geom.Point{2, 4},
			},
			res: "010100000000000000000000400000000000001040",
		},
		"3": {
			tmpl: "{{ .Tags.some_tag }}",
			feature: provider.Feature {
				Tags: map[string]interface{}{
					"some_tag": "some_value",
				},
			},
			res: "some_value",
		},
	}

	for k, v := range tcases {
		t.Run(k, fn(v))
	}
}
