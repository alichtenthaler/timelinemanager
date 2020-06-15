package timelinemanager

import (
	"fmt"

	"github.com/uol/funks"
	"github.com/uol/hashing"
	"github.com/uol/timeline"
)

// StorageType - the storage type constant
type StorageType string

// TransportType - the transport type constant
type TransportType string

const (
	// Normal - normal storage backend
	Normal StorageType = "normal"

	// Archive - archive storage backend
	Archive StorageType = "archive"

	// HTTP - http transport type
	HTTP TransportType = "http"

	// OpenTSDB - opentsdb transport type
	OpenTSDB TransportType = "opentsdb"

	cFunction  string = "func"
	cType      string = "type"
	cOperation string = "operation"
	cHost      string = "host"

	cHTTPNumberFormat string = "httpNumberFormat"
	cHTTPTextFormat   string = "httpTextFormat"

	cLoggerStorage string = "storage"
)

// ErrStorageNotFound - raised when a storage type was not found
var ErrStorageNotFound error = fmt.Errorf("storage type not found")

// ErrTransportNotSupported - raised when a transport is not supported for the specified storage
var ErrTransportNotSupported error = fmt.Errorf("transport not supported")

// BackendItem - one backend configuration
type BackendItem struct {
	timeline.Backend
	Storage       StorageType       `json:"storage,omitempty"`
	Type          TransportType     `json:"type,omitempty"`
	CycleDuration funks.Duration    `json:"cycleDuration,omitempty"`
	AddHostTag    bool              `json:"addHostTag,omitempty"`
	CommonTags    map[string]string `json:"commonTags,omitempty"`
}

// CustomJSONMapping - a custom json mapping to be added
type CustomJSONMapping struct {
	MappingName string      `json:"mappingName,omitempty"`
	Instance    interface{} `json:"instance,omitempty"`
	Variables   []string    `json:"variables,omitempty"`
}

// HTTPTransportConfigExt - an extension to the timeline.HTTPTransportConfig
type HTTPTransportConfigExt struct {
	timeline.HTTPTransportConfig
	JSONMappings []CustomJSONMapping `json:"jsonMappings,omitempty"`
}

// Configuration - configuration
type Configuration struct {
	Backends         []BackendItem     `json:"backends,omitempty"`
	HashingAlgorithm hashing.Algorithm `json:"hashingAlgorithm,omitempty"`
	HashSize         int               `json:"hashSize,omitempty"`
	DataTTL          funks.Duration    `json:"dataTTL,omitempty"`
	timeline.DefaultTransportConfig
	OpenTSDBTransport *timeline.OpenTSDBTransportConfig `json:"openTSDBTransport,omitempty"`
	HTTPTransport     *HTTPTransportConfigExt           `json:"httpTransport,omitempty"`
}

// Validate - validates the configuration
func (c *Configuration) Validate() error {

	if len(c.Backends) == 0 {
		return fmt.Errorf("no backends configured")
	}

	var hasOpenTSDB, hasHTTP bool

	if hasOpenTSDB = c.OpenTSDBTransport != nil; hasOpenTSDB {
		c.OpenTSDBTransport.DefaultTransportConfig = c.DefaultTransportConfig
	}

	if hasHTTP = c.HTTPTransport != nil; hasHTTP {
		c.HTTPTransport.DefaultTransportConfig = c.DefaultTransportConfig
	}

	if !hasOpenTSDB && !hasHTTP {
		return fmt.Errorf("no transports configured")
	}

	return nil
}
