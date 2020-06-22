package tests

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/assert"
	"github.com/uol/funks"
	"github.com/uol/gofiles"
	"github.com/uol/hashing"
	"github.com/uol/timeline"
	"github.com/uol/timelinemanager"
)

const customStorage timelinemanager.StorageType = "custom"

var expectedConf = timelinemanager.Configuration{

	HashingAlgorithm: hashing.SHAKE128,
	HashSize:         6,
	DataTTL:          *funks.ForceNewStringDuration("2m"),

	DefaultTransportConfig: timeline.DefaultTransportConfig{
		TransportBufferSize:  1024,
		BatchSendInterval:    *funks.ForceNewStringDuration("30s"),
		RequestTimeout:       *funks.ForceNewStringDuration("5s"),
		SerializerBufferSize: 2048,
		DebugInput:           false,
		DebugOutput:          true,
		TimeBetweenBatches:   *funks.ForceNewStringDuration("10ms"),
		PrintStackOnError:    true,
	},

	OpenTSDBTransports: map[string]timelinemanager.OpenTSDBTransportConfigExt{
		"opentsdb": {
			OpenTSDBTransportConfig: timeline.OpenTSDBTransportConfig{
				ReadBufferSize:         64,
				MaxReadTimeout:         *funks.ForceNewStringDuration("100ms"),
				ReconnectionTimeout:    *funks.ForceNewStringDuration("3s"),
				MaxReconnectionRetries: 5,
				DisconnectAfterWrites:  true,
			},
		},
	},

	HTTPTransports: map[string]timelinemanager.HTTPTransportConfigExt{
		"number": {
			HTTPTransportConfig: timeline.HTTPTransportConfig{
				ServiceEndpoint:        "/api/put",
				Method:                 "PUT",
				ExpectedResponseStatus: 204,
				TimestampProperty:      "timestamp",
				ValueProperty:          "value",
			},
		},
		"text": {
			HTTPTransportConfig: timeline.HTTPTransportConfig{
				ServiceEndpoint:        "/api/text/put",
				Method:                 "POST",
				ExpectedResponseStatus: 204,
				TimestampProperty:      "timestamp",
				ValueProperty:          "text",
			},
			TransportExt: timelinemanager.TransportExt{
				Text: true,
			},
		},
	},

	Backends: []timelinemanager.BackendItem{

		{
			AddHostTag:    true,
			CycleDuration: *funks.ForceNewStringDuration("15s"),
			Backend: timeline.Backend{
				Host: "host1",
				Port: 8123,
			},
			Storage:   timelinemanager.Normal,
			Transport: "opentsdb",
			CommonTags: map[string]string{
				"tag1": "val1",
				"tag2": "val2",
				"tag3": "val3",
			},
		},

		{
			AddHostTag:    true,
			CycleDuration: *funks.ForceNewStringDuration("25s"),
			Backend: timeline.Backend{
				Host: "host2",
				Port: 8124,
			},
			Storage:   timelinemanager.Archive,
			Transport: "number",
			CommonTags: map[string]string{
				"tag4": "val4",
				"tag5": "val5",
				"tag6": "val6",
			},
		},

		{
			AddHostTag:    false,
			CycleDuration: *funks.ForceNewStringDuration("35s"),
			Backend: timeline.Backend{
				Host: "host3",
				Port: 8125,
			},
			Storage:   customStorage,
			Transport: "text",
			CommonTags: map[string]string{
				"tag7": "val7",
				"tag8": "val8",
				"tag9": "val9",
			},
		},
	},
}

// TestTOMLConfiguration - tests loading the configuration as TOML
func TestTOMLConfiguration(t *testing.T) {

	conf := timelinemanager.Configuration{}

	_, err := toml.DecodeFile("./config.toml", &conf)
	if !assert.NoError(t, err, "no error expected reading the toml") {
		return
	}

	jsonStr, err := json.Marshal(&conf)
	if err != nil {
		panic(err)
	}

	assert.True(t, reflect.DeepEqual(expectedConf, conf), "expected same configuration: %s", jsonStr)
}

// TestJSONConfiguration - tests loading the configuration as JSON
func TestJSONConfiguration(t *testing.T) {

	jsonBytes, err := gofiles.ReadFileBytes("./config.json")
	if !assert.NoError(t, err, "expected no error loading json file") {
		return
	}

	tmc := timelinemanager.Configuration{}

	err = json.Unmarshal(jsonBytes, &tmc)
	if !assert.NoError(t, err, "expected no error unmarshalling json") {
		return
	}

	assert.True(t, reflect.DeepEqual(expectedConf, tmc), "expected same configuration: %s", string(jsonBytes))
}
