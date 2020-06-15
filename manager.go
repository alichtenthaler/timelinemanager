package timelinemanager

import (
	"fmt"
	"os"
	"time"

	"github.com/uol/logh"
	jsonSerializer "github.com/uol/serializer/json"
	"github.com/uol/timeline"
)

//
// Manages the timeline instances.
// @author: rnojiri
//

// backendManager - internal type
type backendManager struct {
	manager    *timeline.Manager
	commonTags []interface{}
	ttype      TransportType
}

// Instance - manages the configured number of timeline manager instances
type Instance struct {
	backendMap    map[StorageType]backendManager
	logger        *logh.ContextualLogger
	hostName      string
	configuration *Configuration
	ready         bool
}

// New - creates a new instance
func New(configuration *Configuration) (*Instance, error) {

	logger := logh.CreateContextualLogger("pkg", "stats")

	if configuration == nil {
		return nil, fmt.Errorf("configuration is null")
	}

	if err := configuration.Validate(); err != nil {
		return nil, err
	}

	hostName, err := os.Hostname()
	if err != nil {
		if logh.ErrorEnabled {
			logger.Error().Msg("error getting host's name")
		}

		return nil, err
	}

	return &Instance{
		logger:        logger,
		hostName:      hostName,
		configuration: configuration,
	}, nil
}

// storageTypeNotFound - logs the storage type not found error
func (tm *Instance) storageTypeNotFound(function string, stype StorageType) error {

	if logh.ErrorEnabled {
		ev := tm.logger.Error()
		if len(function) > 0 {
			ev = ev.Str(cFunction, function)
		}

		ev.Msgf("storage type is not configured: %s", stype)
	}

	return ErrStorageNotFound
}

// Start - starts the timeline manager
func (tm *Instance) Start() error {

	tm.backendMap = map[StorageType]backendManager{}

	for i := 0; i < len(tm.configuration.Backends); i++ {

		b := &tm.configuration.Backends[i].Backend

		dtc := timeline.DataTransformerConfig{
			CycleDuration:    tm.configuration.Backends[i].CycleDuration,
			HashSize:         tm.configuration.HashSize,
			HashingAlgorithm: tm.configuration.HashingAlgorithm,
		}

		f := timeline.NewFlattener(&dtc)
		a := timeline.NewAccumulator(&dtc)

		var manager *timeline.Manager
		var err error

		if tm.configuration.Backends[i].Type == OpenTSDB {

			conf := *tm.configuration.OpenTSDBTransport

			opentsdbTransport, err := timeline.NewOpenTSDBTransport(&conf)
			if err != nil {
				return err
			}

			manager, err = timeline.NewManager(opentsdbTransport, f, a, b)

		} else if tm.configuration.Backends[i].Type == HTTP {

			httpTransport, err := timeline.NewHTTPTransport(&tm.configuration.HTTPTransport.HTTPTransportConfig)
			if err != nil {
				return err
			}

			httpTransport.AddJSONMapping(
				cHTTPNumberFormat,
				jsonSerializer.NumberPoint{},
				cMetric,
				cValue,
				cTimestamp,
				cTags,
			)

			httpTransport.AddJSONMapping(
				cHTTPTextFormat,
				jsonSerializer.TextPoint{},
				cMetric,
				cText,
				cTimestamp,
				cTags,
			)

			if len(tm.configuration.HTTPTransport.JSONMappings) > 0 {
				for _, mapping := range tm.configuration.HTTPTransport.JSONMappings {
					httpTransport.AddJSONMapping(
						mapping.MappingName,
						mapping.Instance,
						mapping.Variables...,
					)
				}
			}

			manager, err = timeline.NewManager(httpTransport, f, a, b, cLoggerStorage, string(tm.configuration.Backends[i].Storage))

		} else {

			err = fmt.Errorf("transport type %s is undefined", tm.configuration.Backends[i].Type)
		}

		if err != nil {
			return err
		}

		numHostTags := 0
		if tm.configuration.Backends[i].AddHostTag {
			numHostTags = 2
		}

		tags := make([]interface{}, numHostTags+len(tm.configuration.Backends[i].CommonTags)*2)

		tagIndex := 0
		for k, v := range tm.configuration.Backends[i].CommonTags {
			tags[tagIndex] = k
			tagIndex++
			tags[tagIndex] = v
			tagIndex++
		}

		if tm.configuration.Backends[i].AddHostTag {
			tags[tagIndex] = cHost
			tagIndex++
			tags[tagIndex] = tm.hostName
		}

		if _, exists := tm.backendMap[tm.configuration.Backends[i].Storage]; exists {
			return fmt.Errorf(`backend named "%s" is registered more than one time`, tm.configuration.Backends[i].Storage)
		}

		tm.backendMap[tm.configuration.Backends[i].Storage] = backendManager{
			manager:    manager,
			commonTags: tags,
			ttype:      tm.configuration.Backends[i].Type,
		}

		err = manager.Start()
		if err != nil {
			return err
		}

		if logh.InfoEnabled {
			tm.logger.Info().Str(cType, string(tm.configuration.Backends[i].Type)).Msgf("timeline manager created: %s:%d (%+v)", b.Host, b.Port, tags)
		}
	}

	if logh.InfoEnabled {
		tm.logger.Info().Msg("timeline manager was started")
	}

	tm.ready = true

	return nil
}

// Shutdown - shuts down the timeline manager
func (tm *Instance) Shutdown() {

	for _, v := range tm.backendMap {
		v.manager.Shutdown()
	}

	if logh.InfoEnabled {
		tm.logger.Info().Msg("timeline manager was shutdown")
	}
}

// GetConfiguredDataTTL - returns the configured data ttl
func (tm *Instance) GetConfiguredDataTTL() time.Duration {

	return tm.configuration.DataTTL.Duration
}
