package provider

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-spatial/tegola/dict"
)

type Consumer interface {
	InsertFeatures(ctx context.Context, layer string, feature []Feature) error
	Layers() ([]LayerInfo, error)
}

type InitFuncConsumer func(dicter dict.Dicter) (Consumer, error)

type cfns struct {
	init InitFuncConsumer
	cleanup CleanupFunc
}

var consumers map[string]cfns

func RegisterConsumer(name string, init InitFuncConsumer, cleanup CleanupFunc) error {
	if consumers == nil {
		consumers = make(map[string]cfns)
	}

	if _, ok := consumers[name]; ok {
		return fmt.Errorf("consumer %v already exists", name)
	}

	consumers[name] = cfns {
		init: init,
		cleanup: cleanup,
	}

	return nil
}

func ForConsumer(name string, config dict.Dicter) (Consumer, error) {
	if len(consumers)  == 0 {
		return nil, fmt.Errorf("no consumers registered")
	}

	cons, ok := consumers[name]
	if !ok {
		return nil, fmt.Errorf("no consumers registered by the name: %v, known consumers: %v", name, strings.Join(ConsumerDrivers(), ", "))
	}

	return cons.init(config)
}


func ConsumerDrivers() []string {
	ret := make([]string, len(consumers))
	i := 0
	for k := range consumers {
		ret[i] = k
		i++
	}

	return ret
}
