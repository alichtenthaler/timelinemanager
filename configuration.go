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
	CycleDuration funks.Duration    `json:"cycleDuration,omitempty"`
	AddHostTag    bool              `json:"addHostTag,omitempty"`
	CommonTags    map[string]string `json:"commonTags,omitempty"`
	Transport     string            `json:"transport,omitempty"`
	transportType TransportType
}

// CustomJSONMapping - a custom json mapping to be added
type CustomJSONMapping struct {
	MappingName string      `json:"mappingName,omitempty"`
	Instance    interface{} `json:"instance,omitempty"`
	Variables   []string    `json:"variables,omitempty"`
}

// TransportExt - an transport extension
type TransportExt struct {
	Text bool `json:"text,omitempty"`
}

// HTTPTransportConfigExt - an extension to the timeline.HTTPTransportConfig
type HTTPTransportConfigExt struct {
	TransportExt
	timeline.HTTPTransportConfig
	JSONMappings []CustomJSONMapping `json:"jsonMappings,omitempty"`
}

// OpenTSDBTransportConfigExt - an extension to the timeline.OpenTSDBTransportConfig
type OpenTSDBTransportConfigExt struct {
	TransportExt
	timeline.OpenTSDBTransportConfig
}

// Configuration - configuration
type Configuration struct {
	Backends         []BackendItem     `json:"backends,omitempty"`
	HashingAlgorithm hashing.Algorithm `json:"hashingAlgorithm,omitempty"`
	HashSize         int               `json:"hashSize,omitempty"`
	DataTTL          funks.Duration    `json:"dataTTL,omitempty"`
	timeline.DefaultTransportConfig
	OpenTSDBTransports map[string]OpenTSDBTransportConfigExt `json:"openTSDBTransports,omitempty"`
	HTTPTransports     map[string]HTTPTransportConfigExt     `json:"httpTransports,omitempty"`
}

// Validate - validates the configuration
func (c *Configuration) Validate() error {

	if len(c.Backends) == 0 {
		return fmt.Errorf("no backends configured")
	}

	var hasOpenTSDB, hasHTTP bool

	if hasOpenTSDB = len(c.OpenTSDBTransports) > 0; hasOpenTSDB {
		for k := range c.OpenTSDBTransports {
			v := c.OpenTSDBTransports[k]
			v.OpenTSDBTransportConfig.DefaultTransportConfig = c.DefaultTransportConfig
			c.OpenTSDBTransports[k] = v
		}
	}

	if hasHTTP = len(c.HTTPTransports) > 0; hasHTTP {
		for k := range c.HTTPTransports {
			v := c.HTTPTransports[k]
			v.HTTPTransportConfig.DefaultTransportConfig = c.DefaultTransportConfig
			c.HTTPTransports[k] = v
		}
	}

	if !hasOpenTSDB && !hasHTTP {
		return fmt.Errorf("no transports configured")
	}

	return nil
}
