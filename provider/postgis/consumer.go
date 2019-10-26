package postgis


import (
	"log"
	"context"
	"strings"
	"reflect"
	"encoding/hex"
	"fmt"
	"text/template"

	"github.com/jackc/pgx"

	"github.com/go-spatial/geom"
	"github.com/go-spatial/geom/encoding/wkb"
	"github.com/go-spatial/geom/encoding/wkt"

	"github.com/go-spatial/tegola/provider"
	"github.com/go-spatial/tegola/dict"
)


func init() {
	provider.RegisterConsumer("postgis", NewConsumer, CleanupConsumers)
}

func NewConsumer(config dict.Dicter) (provider.Consumer, error) {
	host, err := config.String(ConfigKeyHost, nil)
	if err != nil {
		return nil, err
	}

	db, err := config.String(ConfigKeyDB, nil)
	if err != nil {
		return nil, err
	}

	user, err := config.String(ConfigKeyUser, nil)
	if err != nil {
		return nil, err
	}

	password, err := config.String(ConfigKeyPassword, nil)
	if err != nil {
		return nil, err
	}

	port := DefaultPort
	if port, err = config.Int(ConfigKeyPort, &port); err != nil {
		return nil, err
	}

	maxcon := DefaultMaxConn
	if maxcon, err = config.Int(ConfigKeyMaxConn, &maxcon); err != nil {
		return nil, err
	}

	srid := DefaultSRID
	if srid, err = config.Int(ConfigKeySRID, &srid); err != nil {
		return nil, err
	}

	connConfig := pgx.ConnConfig{
		Host:     host,
		Port:     uint16(port),
		Database: db,
		User:     user,
		Password: password,
		LogLevel: pgx.LogLevelWarn,
		RuntimeParams: map[string]string{
			"application_name":              "tegola",
		},
	}

	cons := Consumer {
		srid: uint64(srid),
		config: pgx.ConnPoolConfig{
			ConnConfig: connConfig,
			MaxConnections: int(maxcon),
		},
	}

	if cons.pool, err = pgx.NewConnPool(cons.config); err != nil {
		return nil, fmt.Errorf("Failed while creating connection pool: %v", err)
	}

	layers, err := config.MapSlice(ConfigKeyLayers)
	if err != nil {
		return nil, err
	}

	cons.layers = map[string]ConsumerLayer{}

	for _, layer := range layers {
		lname, err := layer.String(ConfigKeyLayerName, nil)
		if err != nil {
			return nil, err
		}

		tmplStr, err := layer.String("sql", nil)
		if err != nil {
			return nil, err
		}

		tmpl, err := compileTemplate(tmplStr)
		if err != nil {
			return nil, err
		}

		geomTypeName, err := layer.String("geom_type", nil)
		if err != nil {
			return nil, err
		}

		var geomType reflect.Type
		switch geomTypeName {
		case "point":
			geomType = reflect.TypeOf(geom.Point{})
		case "multipoint":
			geomType = reflect.TypeOf(geom.MultiPoint{})
		case "linestring":
			geomType = reflect.TypeOf(geom.LineString{})
		case "multilinesyring":
			geomType = reflect.TypeOf(geom.MultiLineString{})
		case "polygon":
			geomType = reflect.TypeOf(geom.Polygon{})
		case "multipolygon":
			geomType = reflect.TypeOf(geom.MultiPolygon{})
		case "collection":
			geomType = reflect.TypeOf(geom.Collection{})
		default:
			return nil, fmt.Errorf("invalid value for geom_type, %q", geomTypeName)
		}

		cons.layers[lname] = ConsumerLayer{
			srid: cons.srid,
			name: lname,
			geomType: geomType,
			tmpl: tmpl,
		}
	}

	return &cons, nil
}

var consumers []Consumer

func CleanupConsumers() {
	if len(consumers) > 0 {
		log.Printf("cleaning up postgis consumers")
	}

	for _, v := range consumers {
		v.Close()
	}

	consumers = consumers[:0]
}

type Consumer struct {
	config pgx.ConnPoolConfig
	pool *pgx.ConnPool

	layers map[string]ConsumerLayer
	srid uint64
}

func (cons Consumer) Close() {
	cons.pool.Close()
}

type ConsumerLayer struct {
	srid uint64
	name string
	geomType reflect.Type

	tmpl *template.Template
}

func (cl ConsumerLayer) Name() string {
	return cl.name
}

func (cl ConsumerLayer) GeomType() geom.Geometry {
	return reflect.Indirect(reflect.New(cl.geomType)).Interface()
}

func (cl ConsumerLayer) SRID() uint64 {
	return cl.srid
}

func (cons *Consumer) Layers() ([]provider.LayerInfo, error) {
	ret := make([]provider.LayerInfo, len(cons.layers))
	i := 0
	for _, v := range cons.layers {
		ret[i] = v
		i++
	}

	return ret, nil
}

func (cons *Consumer) InsertFeatures(ctx context.Context, layerName string, feature []provider.Feature) error {

	layer, ok := cons.layers[layerName]
	if !ok {
		return fmt.Errorf("layer %q does not exist", layerName)
	}



	for _, v := range feature {
		if reflect.TypeOf(v.Geometry) != layer.geomType {
			return fmt.Errorf("layer %s only accepts %T, got %T",
				layerName, layer.GeomType(), v.Geometry)
		}

		wr := &strings.Builder{}
		err := layer.tmpl.Execute(wr, v)
		if err != nil {
			return err
		}

		sql := wr.String()

		log.Println(sql)

		tag, err := cons.pool.Exec(sql)
		log.Println(tag, err)
		if err != nil {
			return err
		}
	}

	return nil
}

var fnMap = template.FuncMap{
	"AsBinary": func(g geom.Geometry) (string, error) {
		byt, err := wkb.EncodeBytes(g)
		if err != nil {
			return "", err
		}

		return hex.EncodeToString(byt), nil
	},
	"AsText": func(g geom.Geometry) (string, error) {
		return wkt.EncodeString(g)
	},
}

func compileTemplate(tmplStr string) (*template.Template, error) {
	return template.New("").Funcs(fnMap).Parse(tmplStr)
}

