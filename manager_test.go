package timelinemanager_test

import (
	"fmt"
	"net/http"
	"regexp"
	"testing"
	"time"

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

func createTimelineManager(t *testing.T, configs ...*storageConfig) (*timelinemanager.Instance, bool) {

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

			conf.telnetServer, conf.port = gotesttelnet.NewServer(testHost, channelSize, bufferSize, time.Second, true)

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

	dtc := timeline.DefaultTransportConfiguration{
		SerializerBufferSize: bufferSize,
		BatchSendInterval:    funks.Duration{Duration: cycleDurationMS * time.Millisecond},
		RequestTimeout:       funks.Duration{Duration: requestTimeoutS * time.Second},
		TransportBufferSize:  bufferSize,
	}

	c := &timelinemanager.Configuration{
		Backends:         backends,
		HashingAlgorithm: hashing.SHAKE128,
		HashSize:         12,
		DataTTL:          funks.Duration{Duration: time.Minute},
		HTTPTransportConfig: timeline.HTTPTransportConfig{
			DefaultTransportConfiguration: dtc,
			ExpectedResponseStatus:        200,
			Method:                        "POST",
			ServiceEndpoint:               "/post",
			TimestampProperty:             "timestamp",
			ValueProperty:                 "value",
		},
		OpenTSDBTransportConfig: timeline.OpenTSDBTransportConfig{
			DefaultTransportConfiguration: dtc,
			MaxReadTimeout:                funks.Duration{Duration: requestTimeoutS * time.Second},
			MaxReconnectionRetries:        3,
			ReadBufferSize:                bufferSize,
			ReconnectionTimeout:           funks.Duration{Duration: 100 * time.Millisecond},
		},
	}

	tm, err := timelinemanager.New(c)
	if assert.NoError(t, err, "expected no error creating the timeline manager") {
		return nil, false
	}

	if !assert.NotNil(t, tm, "expected a valid instance") {
		return nil, false
	}

	err = tm.Start()
	if assert.NoError(t, err, "expected no error starting the timeline manager") {
		return nil, false
	}

	return tm, true
}

func closeAll(tm *timelinemanager.Instance, confs []*storageConfig) {

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

	tm, ok := createTimelineManager(t, configs...)
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

	tag1K := fmt.Sprintf("tag1_%d", gotest.RandomInt(1, 100))
	tag2K := fmt.Sprintf("tag2_%d", gotest.RandomInt(1, 100))

	tag1V := fmt.Sprintf("val1_%d", gotest.RandomInt(1, 100))
	tag2V := fmt.Sprintf("val2_%d", gotest.RandomInt(1, 100))

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
				fmt.Sprintf(`\[\{"metric":"%s","tags":\{"tag[1-2]{1}K_[0-9]+":"tag[1-2]{1}V[0-9]+","tag[1-2]{1}K_[0-9]+":"tag[1-2]{1}V[0-9]+"\},"timestamp":[0-9]{10},"value":%d\}\]`,
					metric, value)).
				MatchString(message.Body),
			"expected same message",
		)

	} else {

		return assert.True(t,
			regexp.MustCompile(
				fmt.Sprintf(`\[\{"metric":"%s","tags":\{"tag[1-2]{1}K_[0-9]+":"tag[1-2]{1}V[0-9]+","tag[1-2]{1}K_[0-9]+":"tag[1-2]{1}V[0-9]+"\},"timestamp":[0-9]{10},"text":"%s"\}\]`,
					metric, value)).
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

	tm, ok := createTimelineManager(t, configs...)
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

	tm, ok := createTimelineManager(t, configs...)
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

	tm, ok := createTimelineManager(t, configs...)
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

	tm, ok := createTimelineManager(t, configs...)
	if !ok {
		return
	}

	defer closeAll(tm, configs)

	testHTTPMessage(t, "TestBothTransports", tm, timelinemanager.Normal, timelinemanager.RawHTTP, configs[0], true)
	testHTTPMessage(t, "TestBothTransports", tm, timelinemanager.Normal, timelinemanager.RawHTTP, configs[0], false)
	testOpenTSDBMessage(t, "TestBothTransports", tm, timelinemanager.Archive, timelinemanager.RawOpenTSDB, configs[0])
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
		{
			stype: timelinemanager.Archive,
			ttype: timelinemanager.OpenTSDB,
		},
	}

	tm, ok := createTimelineManager(t, configs...)
	if !ok {
		return
	}

	defer closeAll(tm, configs)

	testHTTPMessage(t, "TestBothTransportsWithErrors", tm, timelinemanager.Normal, timelinemanager.RawHTTP, configs[0], true)
	testUnknownStorage(t, "TestBothTransportsWithErrors", tm, timelinemanager.Archive, timelinemanager.RawOpenTSDB)
	testHTTPMessage(t, "TestBothTransportsWithErrors", tm, timelinemanager.Normal, timelinemanager.RawHTTP, configs[0], false)
	testOpenTSDBMessage(t, "TestBothTransportsWithErrors", tm, timelinemanager.Archive, timelinemanager.RawOpenTSDB, configs[0])
	testUnknownTransport(t, "TestBothTransportsWithErrors", tm, timelinemanager.Normal, timelinemanager.RawOpenTSDB, true, true)
}
