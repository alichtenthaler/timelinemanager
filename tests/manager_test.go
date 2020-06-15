package timelinemanager_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/uol/gofiles"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/assert"
	"github.com/uol/funks"
	gotesthttp "github.com/uol/gotest/http"
	gotesttelnet "github.com/uol/gotest/telnet"
	gotest "github.com/uol/gotest/utils"
	"github.com/uol/hashing"
	"github.com/uol/timeline"
	"github.com/uol/timelinemanager"
)

const (
	testHost        string        = "localhost"
	channelSize     int           = 5
	bufferSize      int           = 256
	cycleDurationMS time.Duration = 200
	requestTimeoutS time.Duration = 5
)

type storageConfig struct {
	stype        timelinemanager.StorageType
	ttype        timelinemanager.TransportType
	httpServer   *gotesthttp.Server
	telnetServer *gotesttelnet.Server
	port         int
}

func (sc *storageConfig) Close() {

	if sc.httpServer != nil {
		sc.httpServer.Close()
	}

	if sc.telnetServer != nil {
		sc.telnetServer.Stop()
	}
}

func createTestConf(customJSONMapings []timelinemanager.CustomJSONMapping, configs ...*storageConfig) *timelinemanager.Configuration {

	backends := []timelinemanager.BackendItem{}

	for _, conf := range configs {

		if conf.ttype == timelinemanager.HTTP {

			headers := http.Header{}
			headers.Add("Content-type", "text/plain; charset=utf-8")

			responses := []gotesthttp.ResponseData{
				{
					RequestData: gotesthttp.RequestData{
						URI:     "/post",
						Body:    "",
						Method:  "POST",
						Headers: headers,
					},
					Status: http.StatusOK,
				},
			}

			conf.port = gotest.GeneratePort()

			conf.httpServer = gotesthttp.NewServer(testHost, conf.port, channelSize, responses)

		} else if conf.ttype == timelinemanager.OpenTSDB {

			conf.telnetServer, conf.port = gotesttelnet.NewServer(
				&gotesttelnet.Configuration{
					Host:               testHost,
					MessageChannelSize: channelSize,
					ReadBufferSize:     bufferSize,
					ReadTimeout:        requestTimeoutS * time.Second,
				},
				true,
			)

		} else {
			panic("transport type is not defined")
		}

		backends = append(backends, timelinemanager.BackendItem{
			Backend: timeline.Backend{
				Host: testHost,
				Port: conf.port,
			},
			CycleDuration: funks.Duration{Duration: cycleDurationMS * time.Millisecond},
			Storage:       conf.stype,
			Type:          conf.ttype,
		})
	}

	c := &timelinemanager.Configuration{
		Backends:         backends,
		HashingAlgorithm: hashing.SHAKE128,
		HashSize:         12,
		DataTTL:          funks.Duration{Duration: time.Minute},
		DefaultTransportConfig: timeline.DefaultTransportConfig{
			SerializerBufferSize: bufferSize,
			BatchSendInterval:    funks.Duration{Duration: cycleDurationMS * time.Millisecond},
			RequestTimeout:       funks.Duration{Duration: requestTimeoutS * time.Second},
			TransportBufferSize:  bufferSize,
			TimeBetweenBatches:   funks.Duration{Duration: 10 * time.Millisecond},
		},
		HTTPTransport: &timelinemanager.HTTPTransportConfigExt{
			HTTPTransportConfig: timeline.HTTPTransportConfig{
				ExpectedResponseStatus: 200,
				Method:                 "POST",
				ServiceEndpoint:        "/post",
				TimestampProperty:      "timestamp",
				ValueProperty:          "value",
			},
			JSONMappings: customJSONMapings,
		},
		OpenTSDBTransport: &timeline.OpenTSDBTransportConfig{
			MaxReadTimeout:         funks.Duration{Duration: requestTimeoutS * time.Second},
			MaxReconnectionRetries: 3,
			ReadBufferSize:         bufferSize,
			ReconnectionTimeout:    funks.Duration{Duration: 100 * time.Millisecond},
		},
	}

	return c
}

func createTimelineManager(t *testing.T, customJSONMapings []timelinemanager.CustomJSONMapping, configs ...*storageConfig) (*timelinemanager.Instance, bool) {

	tm, err := timelinemanager.New(createTestConf(customJSONMapings, configs...))
	if !assert.NoError(t, err, "expected no error creating the timeline manager") {
		return nil, false
	}

	if !assert.NotNil(t, tm, "expected a valid instance") {
		return nil, false
	}

	err = tm.Start()
	if !assert.NoError(t, err, "expected no error starting the timeline manager") {
		return nil, false
	}

	return tm, true
}

func closeAll(tm *timelinemanager.Instance, confs []*storageConfig) {

	<-time.After(requestTimeoutS * time.Second)

	if tm != nil {
		tm.Shutdown()
	}

	for _, conf := range confs {
		conf.Close()
	}
}

func testSendOpenTSDBMessage(
	t *testing.T,
	function string,
	tm *timelinemanager.Instance,
	stype timelinemanager.StorageType,
	op timeline.FlatOperation,

) (metric, tag1K, tag1V, tag2K, tag2V string, value int, testOk bool) {

	value = gotest.RandomInt(1, 100)
	metric = fmt.Sprintf("metric_%d", gotest.RandomInt(1, 100))

	tag1K = fmt.Sprintf("tag1_%d", gotest.RandomInt(1, 100))
	tag2K = fmt.Sprintf("tag2_%d", gotest.RandomInt(1, 100))

	tag1V = fmt.Sprintf("val1_%d", gotest.RandomInt(1, 100))
	tag2V = fmt.Sprintf("val2_%d", gotest.RandomInt(1, 100))

	err := tm.Send(
		function,
		stype,
		op,
		float64(value),
		metric,
		tag1K, tag1V,
		tag2K, tag2V,
	)

	testOk = assert.NoError(t, err, "expected no error")
	return
}

func testOpenTSDBMessage(t *testing.T, function string, tm *timelinemanager.Instance, stype timelinemanager.StorageType, op timeline.FlatOperation, conf *storageConfig) bool {

	metric, tag1K, tag1V, tag2K, tag2V, value, ok := testSendOpenTSDBMessage(t, function, tm, stype, op)
	if !ok {
		return false
	}

	message := <-conf.telnetServer.MessageChannel()

	return assert.True(t,
		regexp.MustCompile(
			fmt.Sprintf(`put %s [0-9]{10} %d %s=%s %s=%s`,
				metric, value, tag1K, tag1V, tag2K, tag2V)).
			MatchString(message.Message),
		"expected same message",
	)
}

// TestOpenTSDB - creates a new manager telnet only
func TestOpenTSDB(t *testing.T) {

	configs := []*storageConfig{
		{
			stype: timelinemanager.Normal,
			ttype: timelinemanager.OpenTSDB,
		},
	}

	tm, ok := createTimelineManager(t, nil, configs...)
	if !ok {
		return
	}

	defer closeAll(tm, configs)

	testOpenTSDBMessage(t, "TestOpenTSDB", tm, timelinemanager.Normal, timelinemanager.RawOpenTSDB, configs[0])
}

func testSendHTTPMessage(
	t *testing.T,
	function string,
	tm *timelinemanager.Instance,
	stype timelinemanager.StorageType,
	op timeline.FlatOperation,
	number bool,

) (metric string, value interface{}, testOk bool) {

	if number {
		value = gotest.RandomInt(1, 100)
	} else {
		value = fmt.Sprintf("text%d", gotest.RandomInt(1, 100))
	}

	metric = fmt.Sprintf("metric_%d", gotest.RandomInt(1, 100))

	tag1K := fmt.Sprintf("tag1K_%d", gotest.RandomInt(1, 100))
	tag2K := fmt.Sprintf("tag2K_%d", gotest.RandomInt(1, 100))

	tag1V := fmt.Sprintf("tag1V_%d", gotest.RandomInt(1, 100))
	tag2V := fmt.Sprintf("tag2V_%d", gotest.RandomInt(1, 100))

	var err error

	if number {

		err = tm.Send(
			function,
			stype,
			op,
			float64(value.(int)),
			metric,
			tag1K, tag1V,
			tag2K, tag2V,
		)

	} else {

		err = tm.SendText(
			function,
			stype,
			value.(string),
			metric,
			tag1K, tag1V,
			tag2K, tag2V,
		)
	}

	testOk = assert.NoError(t, err, "expected no error")
	return
}

func testHTTPMessage(t *testing.T, function string, tm *timelinemanager.Instance, stype timelinemanager.StorageType, op timeline.FlatOperation, conf *storageConfig, number bool) bool {

	metric, value, ok := testSendHTTPMessage(t, function, tm, stype, op, number)
	if !ok {
		return false
	}

	message := <-conf.httpServer.RequestChannel()
	if !assert.NotNil(t, message, "expected a valid request message") {
		return false
	}

	if number {

		return assert.True(t,
			regexp.MustCompile(
				fmt.Sprintf(`\[\{"metric":"%s","tags":\{"tag[1-2]{1}K_[0-9]+":"tag[1-2]{1}V_[0-9]+","tag[1-2]{1}K_[0-9]+":"tag[1-2]{1}V_[0-9]+"\},"timestamp":[0-9]{10},"value":%d?(\.[0]+)\}\]`,
					metric, value.(int))).
				MatchString(message.Body),
			"expected same message",
		)

	} else {

		return assert.True(t,
			regexp.MustCompile(
				fmt.Sprintf(`\[\{"metric":"%s","tags":\{"tag[1-2]{1}K_[0-9]+":"tag[1-2]{1}V_[0-9]+","tag[1-2]{1}K_[0-9]+":"tag[1-2]{1}V_[0-9]+"\},"timestamp":[0-9]{10},"text":"%s"\}\]`,
					metric, value.(string))).
				MatchString(message.Body),
			"expected same message",
		)
	}
}

// TestHTTP - creates a new manager http only
func TestHTTP(t *testing.T) {

	configs := []*storageConfig{
		{
			stype: timelinemanager.Normal,
			ttype: timelinemanager.HTTP,
		},
	}

	tm, ok := createTimelineManager(t, nil, configs...)
	if !ok {
		return
	}

	defer closeAll(tm, configs)

	testHTTPMessage(t, "TestHTTP", tm, timelinemanager.Normal, timelinemanager.RawHTTP, configs[0], true)
	testHTTPMessage(t, "TestHTTP", tm, timelinemanager.Normal, timelinemanager.RawHTTP, configs[0], false)
}

func testUnknownStorage(t *testing.T, function string, tm *timelinemanager.Instance, stype timelinemanager.StorageType, op timeline.FlatOperation) {

	err := tm.Send(function, stype, op, 1.0, "metric", "tag1", "val1", "tag2", "val2")
	assert.Error(t, err, "expected an error")

	assert.Equal(t, timelinemanager.ErrStorageNotFound, err, "expected timelinemanager.ErrStorageNotFound error type")
}

func testUnknownTransport(t *testing.T, function string, tm *timelinemanager.Instance, stype timelinemanager.StorageType, op timeline.FlatOperation, http bool, number bool) {

	var err error
	if !number {
		err = tm.SendText(function, stype, "test", "metric", "tag1", "val1", "tag2", "val2")
	} else {
		err = tm.Send(function, stype, op, 1, "metric", "tag1", "val1", "tag2", "val2")
	}

	assert.Error(t, err, "expected an error")

	assert.Equal(t, timelinemanager.ErrTransportNotSupported, err, "expected timelinemanager.ErrTransportNotSupported error type")
}

// TestStorageNotFound - creates a new manager and tests for a unknown storage
func TestStorageNotFound(t *testing.T) {

	configs := []*storageConfig{
		{
			stype: timelinemanager.Normal,
			ttype: timelinemanager.HTTP,
		},
	}

	tm, ok := createTimelineManager(t, nil, configs...)
	if !ok {
		return
	}

	defer closeAll(tm, configs)

	testUnknownStorage(t, "TestStorageNotFound", tm, timelinemanager.Archive, timelinemanager.RawHTTP)
}

// TestTransportNotSupported - creates a new manager and tests for a unknown transport
func TestTransportNotSupported(t *testing.T) {

	configs := []*storageConfig{
		{
			stype: timelinemanager.Normal,
			ttype: timelinemanager.OpenTSDB,
		},
	}

	tm, ok := createTimelineManager(t, nil, configs...)
	if !ok {
		return
	}

	defer closeAll(tm, configs)

	testUnknownTransport(t, "TestTransportNotSupported", tm, timelinemanager.Normal, timelinemanager.RawHTTP, true, false)
}

// TestBothTransports - creates a new manager and tests http and opentsdb integration (no errors)
func TestBothTransports(t *testing.T) {

	configs := []*storageConfig{
		{
			stype: timelinemanager.Normal,
			ttype: timelinemanager.HTTP,
		},
		{
			stype: timelinemanager.Archive,
			ttype: timelinemanager.OpenTSDB,
		},
	}

	tm, ok := createTimelineManager(t, nil, configs...)
	if !ok {
		return
	}

	defer closeAll(tm, configs)

	testHTTPMessage(t, "TestBothTransports", tm, timelinemanager.Normal, timelinemanager.RawHTTP, configs[0], true)
	testHTTPMessage(t, "TestBothTransports", tm, timelinemanager.Normal, timelinemanager.RawHTTP, configs[0], false)
	testOpenTSDBMessage(t, "TestBothTransports", tm, timelinemanager.Archive, timelinemanager.RawOpenTSDB, configs[1])
}

// TestBothTransportsWithErrors - creates a new manager and tests http and opentsdb integration (some errors)
func TestBothTransportsWithErrors(t *testing.T) {

	configs := []*storageConfig{
		{
			stype: timelinemanager.Archive,
			ttype: timelinemanager.HTTP,
		},
		{
			stype: timelinemanager.Normal,
			ttype: timelinemanager.OpenTSDB,
		},
	}

	tm, ok := createTimelineManager(t, nil, configs...)
	if !ok {
		return
	}

	defer closeAll(tm, configs)

	funcName := "TestBothTransportsWithErrors"

	testUnknownStorage(t, funcName, tm, customStorage, timelinemanager.RawHTTP)
	testHTTPMessage(t, funcName, tm, timelinemanager.Archive, timelinemanager.RawHTTP, configs[0], true)
	testUnknownTransport(t, funcName, tm, timelinemanager.Archive, timelinemanager.RawOpenTSDB, true, true)
	testHTTPMessage(t, funcName, tm, timelinemanager.Archive, timelinemanager.RawHTTP, configs[0], false)
	testOpenTSDBMessage(t, funcName, tm, timelinemanager.Normal, timelinemanager.RawOpenTSDB, configs[1])
	testUnknownTransport(t, funcName, tm, timelinemanager.Normal, timelinemanager.RawHTTP, true, true)
}

// TestSameBackendConfiguration - creates a new manager duplicating some backend
func TestSameBackendConfiguration(t *testing.T) {

	configs := []*storageConfig{
		{
			stype: timelinemanager.Archive,
			ttype: timelinemanager.HTTP,
		},
		{
			stype: timelinemanager.Normal,
			ttype: timelinemanager.OpenTSDB,
		},
		{
			stype: timelinemanager.Archive,
			ttype: timelinemanager.OpenTSDB,
		},
	}

	tm, err := timelinemanager.New(createTestConf(nil, configs...))
	if !assert.NoError(t, err, "expected no error creating the timeline manager") {
		return
	}

	if !assert.NotNil(t, tm, "expected a valid instance") {
		return
	}

	err = tm.Start()
	if !assert.Error(t, err, "expected an error starting the timeline manager") {
		return
	}

	assert.Equal(t, `backend named "archive" is registered more than one time`, err.Error(), "expected a specific error")
}

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

	OpenTSDBTransport: &timeline.OpenTSDBTransportConfig{
		ReadBufferSize:         64,
		MaxReadTimeout:         *funks.ForceNewStringDuration("100ms"),
		ReconnectionTimeout:    *funks.ForceNewStringDuration("3s"),
		MaxReconnectionRetries: 5,
		DisconnectAfterWrites:  true,
	},

	HTTPTransport: &timelinemanager.HTTPTransportConfigExt{
		HTTPTransportConfig: timeline.HTTPTransportConfig{
			ServiceEndpoint:        "/api/put",
			Method:                 "POST",
			ExpectedResponseStatus: 204,
			TimestampProperty:      "timestamp",
			ValueProperty:          "value",
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
			Storage: timelinemanager.Normal,
			Type:    timelinemanager.OpenTSDB,
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
			Storage: timelinemanager.Archive,
			Type:    timelinemanager.OpenTSDB,
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
			Storage: customStorage,
			Type:    timelinemanager.HTTP,
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

// TestCustomJSONMapping - creates a new manager with custom json mapping
func TestCustomJSONMapping(t *testing.T) {

	configs := []*storageConfig{
		{
			stype: timelinemanager.Normal,
			ttype: timelinemanager.HTTP,
		},
	}

	type CustomJSON struct {
		Metric string            `json:"metric"`
		Value  float64           `json:"value"`
		Tags   map[string]string `json:"tags"`
	}

	cjson := CustomJSON{
		Metric: "custom.metric",
		Value:  10,
		Tags: map[string]string{
			"tag1": "$",
			"tag2": "val2",
			"tag3": "$",
		},
	}

	instance := cjson

	customMappings := []timelinemanager.CustomJSONMapping{
		{
			MappingName: "custom",
			Instance:    instance,
			Variables: []string{
				"tags.tag1",
				"tags.tag3",
			},
		},
	}

	tm, ok := createTimelineManager(t, customMappings, configs...)
	if !ok {
		return
	}

	defer closeAll(tm, configs)

	err := tm.SendCustomJSON(
		"TestCustomJSONMapping",
		timelinemanager.Normal,
		"custom",
		"tags.tag1", "customVal1",
		"tags.tag3", "3",
	)

	if !assert.NoError(t, err, "expected no error sending custom json") {
		return
	}

	message := <-configs[0].httpServer.RequestChannel()
	if !assert.NotNil(t, message, "expected a valid request message") {
		return
	}

	expected := []CustomJSON{}
	err = json.Unmarshal([]byte(`[{"metric":"custom.metric","value":10,"tags":{"tag3":"3","tag1":"customVal1","tag2":"val2"}}]`), &expected)
	if !assert.NoError(t, err, "expected no error unmarshalling expected json") {
		return
	}

	actual := []CustomJSON{}
	err = json.Unmarshal([]byte(message.Body), &actual)
	if !assert.NoError(t, err, "expected no error unmarshalling actual json") {
		return
	}

	assert.Equal(t,
		expected,
		actual,
		"expected same message",
	)
}
