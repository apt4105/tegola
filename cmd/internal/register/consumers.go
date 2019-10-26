package register

import (
	"errors"
	"fmt"

	"github.com/go-spatial/tegola/dict"
	"github.com/go-spatial/tegola/provider"
)

var (
	ErrConsumerNameMissing = errors.New("register: consumer 'name' parameter missing")
	ErrConsumerNameInvalid = errors.New("register: consumer 'name' value must be a string")
)

type ErrConsumerAlreadyRegistered string

func (e ErrConsumerAlreadyRegistered) Error() string {
	return fmt.Sprintf("register: consumer (%v) already registered", string(e))
}

type ErrConsumerTypeMissing string

func (e ErrConsumerTypeMissing) Error() string {
	return fmt.Sprintf("register: consumer 'type' parameter missing for consumer (%v)", string(e))
}

type ErrConsumerTypeInvalid string

func (e ErrConsumerTypeInvalid) Error() string {
	return fmt.Sprintf("register: consumer 'type' must be a string for consumer (%v)", string(e))
}

// Consumers registers data consumers backends
func Consumers(consumers []dict.Dicter) (map[string]provider.Consumer, error) {
	// holder for registered consumers
	registeredConsumers := map[string]provider.Consumer{}

	// iterate consumers
	for _, p := range consumers {
		// lookup our proivder name
		pname, err := p.String("name", nil)
		if err != nil {
			switch err.(type) {
			case dict.ErrKeyRequired:
				return registeredConsumers, ErrConsumerNameMissing
			case dict.ErrKeyType:
				return registeredConsumers, ErrConsumerNameInvalid
			default:
				return registeredConsumers, err
			}
		}

		// check if a consumer with this name is alrady registered
		_, ok := registeredConsumers[pname]
		if ok {
			return registeredConsumers, ErrConsumerAlreadyRegistered(pname)
		}

		// lookup our consumer type
		ptype, err := p.String("type", nil)
		if err != nil {
			switch err.(type) {
			case dict.ErrKeyRequired:
				return registeredConsumers, ErrConsumerTypeMissing(pname)
			case dict.ErrKeyType:
				return registeredConsumers, ErrConsumerTypeInvalid(pname)
			default:
				return registeredConsumers, err
			}
		}

		// register the consumer
		prov, err := provider.ForConsumer(ptype, p)
		if err != nil {
			return registeredConsumers, err
		}

		// add the consumer to our map of registered consumers
		registeredConsumers[pname] = prov
	}

	return registeredConsumers, nil
}
