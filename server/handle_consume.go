package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
	"log"
	"fmt"

	"github.com/go-spatial/geom/encoding/geojson"

	"github.com/go-spatial/tegola/atlas"
	"github.com/go-spatial/tegola/provider"
)

type HandleConsumers struct{}

// POST /map/:map/:consumer/:layer
func (HandleConsumers) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		// REST-y
		panic("unrechable")
	}

	arr := strings.Split(r.URL.Path, "/")
	if len(arr) != 5 {
		http.Error(w,
			"path must be /map_name/consumer_name",
			http.StatusBadRequest)
		return
	}

	var geoJsonFeature geojson.Feature
	err := json.NewDecoder(r.Body).Decode(&geoJsonFeature)
	r.Body.Close()
	if err != nil {
		http.Error(w,
			"could not unmarshall geojson",
			http.StatusBadRequest)
		return
	}

	m, err := atlas.GetMap(arr[2])
	if err != nil {
		log.Println(err)
		http.Error(w,
			"map not found",
			http.StatusNotFound)
		return
	}

	cons, ok := m.Consumers[arr[3]]
	if !ok {
		http.Error(w,
			"consumer not found",
			http.StatusNotFound)
		return
	}

	srid, ok := geoJsonFeature.Properties["SRID"].(float64)
	if !ok {
		log.Printf("coud not coerce float64 for SRID (%T) %v",
		geoJsonFeature.Properties["SRID"],
		geoJsonFeature.Properties)
	}

	pfeature := provider.Feature{
		Geometry: geoJsonFeature.Geometry.Geometry,
		SRID:     uint64(srid),
		Tags:     geoJsonFeature.Properties,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err = cons.InsertFeatures(ctx, arr[4], []provider.Feature{pfeature})
	if err != nil {
		log.Println(err)
		http.Error(w, "could not insert feature", http.StatusInternalServerError)
	}

	fmt.Fprintf(w, "{}")
}
