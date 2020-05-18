package tlmanager

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/uol/funks"
	"github.com/uol/hashing"
	"github.com/uol/logh"
	"github.com/uol/timeline"
)

//
// Manages the timeline instances.
// @author: rnojiri
//

// StorageType - the storage type constante
type StorageType string

const (
	// Normal - normal storage backend
	Normal StorageType = "normal"

	// Archive - archive storage backend
	Archive StorageType = "archive"

	cFunction string = "func"
)

// BackendItem - one backend configuration
type BackendItem struct {
	timeline.Backend
	Type          StorageType
	CycleDuration funks.Duration
	AddHostTag    bool
	CommonTags    map[string]string
}

// backendManager - internal type
type backendManager struct {
	manager    *timeline.Manager
	commonTags []interface{}
}

// Configuration - configuration
type Configuration struct {
	Backends             []BackendItem
	HashingAlgorithm     hashing.Algorithm
	HashSize             int
	TransportBufferSize  int
	SerializerBufferSize int
	ReadBufferSize       int
	DebugInput           bool
	DebugOutput          bool
	BatchSendInterval    funks.Duration
	RequestTimeout       funks.Duration
	MaxReadTimeout       funks.Duration
	ReconnectionTimeout  funks.Duration
	DataTTL              funks.Duration
}

// TimelineManager - manages the configured number of timeline manager instances
type TimelineManager struct {
	backendMap    map[StorageType]backendManager
	logger        *logh.ContextualLogger
	hostName      string
	configuration *Configuration
	ready         bool
}

// New - creates a new instance
func New(configuration *Configuration) (*TimelineManager, error) {

	if len(configuration.Backends) == 0 {
		return nil, fmt.Errorf("no backends configured")
	}

	logger := logh.CreateContextualLogger("pkg", "stats")

	hostName, err := os.Hostname()
	if err != nil {
		if logh.ErrorEnabled {
			logger.Error().Msg("error getting host's name")
		}

		return nil, err
	}

	return &TimelineManager{
		logger:        logger,
		hostName:      hostName,
		configuration: configuration,
	}, nil
}

// storageTypeNotFound - logs the storage type not found error
func (tm *TimelineManager) storageTypeNotFound(function string, stype StorageType, logErr, returnErr bool) error {

	msg := fmt.Sprintf("storage type is not configured: %s", stype)

	if logErr && logh.ErrorEnabled {
		ev := tm.logger.Error()
		if len(function) > 0 {
			ev = ev.Str(cFunction, function)
		}

		ev.Msg(msg)
	}

	if returnErr {
		return errors.New(msg)
	}

	return nil
}

// Flatten - performs a flatten operation
func (tm *TimelineManager) Flatten(caller string, stype StorageType, op timeline.FlatOperation, value float64, metric string, tags ...interface{}) {

	if !tm.ready {
		return
	}

	backend, ok := tm.backendMap[stype]
	if !ok {
		tm.storageTypeNotFound(caller, stype, true, false)
		return
	}

	tags = append(tags, backend.commonTags...)

	err := backend.manager.FlattenOpenTSDB(op, value, time.Now().Unix(), metric, tags...)
	if err != nil {
		if logh.ErrorEnabled {
			ev := tm.logger.Error().Err(err)
			if len(caller) > 0 {
				ev = ev.Str(cFunction, caller)
			}
			ev.Msg("flattening operation error")
		}
	}
}

// AccumulateHashedData - accumulates a hashed data
func (tm *TimelineManager) AccumulateHashedData(stype StorageType, hash string) (bool, error) {

	backend, ok := tm.backendMap[stype]
	if !ok {
		return false, tm.storageTypeNotFound("", stype, false, true)
	}

	err := backend.manager.IncrementAccumulatedData(hash)
	if err != nil {
		if err == timeline.ErrNotStored {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

// StoreHashedData - stores the hashed data
func (tm *TimelineManager) StoreHashedData(stype StorageType, hash string, ttl time.Duration, metric string, tags ...interface{}) error {

	backend, ok := tm.backendMap[stype]
	if !ok {
		return tm.storageTypeNotFound("", stype, false, true)
	}

	tags = append(tags, backend.commonTags...)

	return backend.manager.StoreHashedDataToAccumulateOpenTSDB(
		hash,
		ttl,
		1,
		time.Now().Unix(),
		metric,
		tags...,
	)
}

// Start - starts the timeline manager
func (tm *TimelineManager) Start() error {

	tc := timeline.OpenTSDBTransportConfig{
		DefaultTransportConfiguration: timeline.DefaultTransportConfiguration{
			SerializerBufferSize: tm.configuration.SerializerBufferSize,
			TransportBufferSize:  tm.configuration.TransportBufferSize,
			BatchSendInterval:    tm.configuration.BatchSendInterval.Duration,
			RequestTimeout:       tm.configuration.RequestTimeout.Duration,
			DebugInput:           tm.configuration.DebugInput,
			DebugOutput:          tm.configuration.DebugOutput,
		},
		ReadBufferSize:      tm.configuration.ReadBufferSize,
		MaxReadTimeout:      tm.configuration.MaxReadTimeout.Duration,
		ReconnectionTimeout: tm.configuration.ReconnectionTimeout.Duration,
	}

	tm.backendMap = map[StorageType]backendManager{}

	t, err := timeline.NewOpenTSDBTransport(&tc)
	if err != nil {
		return err
	}

	for i := 0; i < len(tm.configuration.Backends); i++ {

		b := timeline.Backend{
			Host: tm.configuration.Backends[i].Host,
			Port: tm.configuration.Backends[i].Port,
		}

		dtc := timeline.DataTransformerConf{
			CycleDuration:    tm.configuration.Backends[i].CycleDuration.Duration,
			HashSize:         tm.configuration.HashSize,
			HashingAlgorithm: tm.configuration.HashingAlgorithm,
		}

		f := timeline.NewFlattener(&dtc)
		a := timeline.NewAccumulator(&dtc)

		manager, err := timeline.NewManager(t, f, a, &b)
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
			tags[tagIndex] = "host"
			tagIndex++
			tags[tagIndex] = tm.hostName
		}

		err = manager.Start()
		if err != nil {
			return err
		}

		tm.backendMap[tm.configuration.Backends[i].Type] = backendManager{
			manager:    manager,
			commonTags: tags,
		}

		if logh.InfoEnabled {
			tm.logger.Info().Str("type", string(tm.configuration.Backends[i].Type)).Msgf("timeline manager created: %s:%d (%+v)", b.Host, b.Port, tags)
		}
	}

	if logh.InfoEnabled {
		tm.logger.Info().Msg("timeline manager was started")
	}

	tm.ready = true

	return nil
}

// Shutdown - shuts down the timeline manager
func (tm *TimelineManager) Shutdown() {

	for _, v := range tm.backendMap {
		v.manager.Shutdown()
	}

	if logh.InfoEnabled {
		tm.logger.Info().Msg("timeline manager was shutdown")
	}
}
