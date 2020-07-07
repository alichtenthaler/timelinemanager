package tests

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uol/funks"
	gotesthttp "github.com/uol/gotest/http"
	gotestcpudp "github.com/uol/gotest/tcpudp"
	gotest "github.com/uol/gotest/utils"
	"github.com/uol/hashing"
	"github.com/uol/timeline"
	"github.com/uol/timelinemanager"
)

const (
	testHost       string        = "localhost"
	channelSize    int           = 10
	bufferSize     int           = 256
	cycleDuration  time.Duration = 200
	requestTimeout time.Duration = 5

	tnNumber   transportName = "number"
	tnText     transportName = "text"
	tnOpenTSDB transportName = "opentsdb"
	tnUDP      transportName = "udp"
)

type transportName string

type storageConfig struct {
	stype      timelinemanager.StorageType
	ttype      timelinemanager.TransportType
	tname      transportName
	backendMap map[transportName]interface{}
	port       int
}

func (sc *storageConfig) getHTTPServer(name transportName) *gotesthttp.Server {

	return sc.backendMap[name].(*gotesthttp.Server)
}

func (sc *storageConfig) getTCPServer(name transportName) *gotestcpudp.TCPServer {

	return sc.backendMap[name].(*gotestcpudp.TCPServer)
}

func (sc *storageConfig) getUDPServer(name transportName) *gotestcpudp.UDPServer {

	return sc.backendMap[name].(*gotestcpudp.UDPServer)
}

func (sc *storageConfig) Close() {

	for _, v := range sc.backendMap {

		if v, ok := v.(*gotesthttp.Server); ok {
			v.Close()
			continue
		}

		if v, ok := v.(*gotestcpudp.TCPServer); ok {
			v.Stop()
			continue
		}

		if v, ok := v.(*gotestcpudp.UDPServer); ok {
			v.Stop()
			continue
		}
	}
}

func createTestConf(customJSONMapings []timelinemanager.CustomJSONMapping, configs ...*storageConfig) *timelinemanager.Configuration {

	backends := []timelinemanager.BackendItem{}
	httpTransportConfsMap := map[string]timelinemanager.HTTPTransportConfigExt{}
	tcpTransportConfsMap := map[string]timelinemanager.OpenTSDBTransportConfigExt{}
	udpTransportConfsMap := map[string]timelinemanager.UDPTransportConfigExt{}

	for _, conf := range configs {

		conf.backendMap = map[transportName]interface{}{}

		if conf.ttype == timelinemanager.HTTPTransport {

			headers := http.Header{}
			headers.Add("Content-type", "text/plain; charset=utf-8")

			responses := []gotesthttp.ResponseData{}

			if conf.tname == tnNumber {

				responses = append(responses, gotesthttp.ResponseData{
					RequestData: gotesthttp.RequestData{
						URI:     "/put",
						Body:    "",
						Method:  "PUT",
						Headers: headers,
					},
					Status: http.StatusNoContent,
				})

				httpTransportConfsMap[string(conf.tname)] = timelinemanager.HTTPTransportConfigExt{
					TransportExt: timelinemanager.TransportExt{
						Serializer:   timelinemanager.JSONSerializer,
						JSONMappings: customJSONMapings,
					},
					HTTPTransportConfig: timeline.HTTPTransportConfig{
						ExpectedResponseStatus: http.StatusOK,
						Method:                 "PUT",
						ServiceEndpoint:        "/put",
						CustomSerializerConfig: timeline.CustomSerializerConfig{
							TimestampProperty: "timestamp",
							ValueProperty:     "value",
						},
					},
				}

			} else if conf.tname == tnText {
				responses = append(responses, gotesthttp.ResponseData{
					RequestData: gotesthttp.RequestData{
						URI:     "/text",
						Body:    "",
						Method:  "POST",
						Headers: headers,
					},
					Status: http.StatusOK,
				})

				httpTransportConfsMap[string(conf.tname)] = timelinemanager.HTTPTransportConfigExt{
					TransportExt: timelinemanager.TransportExt{
						Serializer:   timelinemanager.JSONSerializer,
						JSONMappings: customJSONMapings,
					},
					HTTPTransportConfig: timeline.HTTPTransportConfig{
						ExpectedResponseStatus: http.StatusNoContent,
						Method:                 "POST",
						ServiceEndpoint:        "/text",
						CustomSerializerConfig: timeline.CustomSerializerConfig{
							TimestampProperty: "timestamp",
							ValueProperty:     "text",
						},
					},
				}
			}

			conf.port = gotest.GeneratePort()

			conf.backendMap[conf.tname] = gotesthttp.NewServer(testHost, conf.port, channelSize, responses)

		} else if conf.ttype == timelinemanager.OpenTSDBTransport {

			tcpTransportConfsMap[string(conf.tname)] = timelinemanager.OpenTSDBTransportConfigExt{
				OpenTSDBTransportConfig: timeline.OpenTSDBTransportConfig{
					TCPUDPTransportConfig: timeline.TCPUDPTransportConfig{
						MaxReconnectionRetries: 3,
						ReconnectionTimeout:    funks.Duration{Duration: 100 * time.Millisecond},
					},
					MaxReadTimeout: funks.Duration{Duration: requestTimeout * time.Second},
					ReadBufferSize: bufferSize,
				},
			}

			conf.backendMap[conf.tname], conf.port = gotestcpudp.NewTCPServer(
				&gotestcpudp.TCPConfiguration{
					ServerConfiguration: gotestcpudp.ServerConfiguration{
						Host:               testHost,
						MessageChannelSize: channelSize,
						ReadBufferSize:     bufferSize,
					},
					ReadTimeout: requestTimeout * time.Second,
				},
				true,
			)
		} else if conf.ttype == timelinemanager.UDPTransport {

			udpTransportConfsMap[string(conf.tname)] = timelinemanager.UDPTransportConfigExt{
				UDPTransportConfig: timeline.UDPTransportConfig{
					TCPUDPTransportConfig: timeline.TCPUDPTransportConfig{
						MaxReconnectionRetries: 3,
						ReconnectionTimeout:    funks.Duration{Duration: 100 * time.Millisecond},
					},
				},
			}

			conf.backendMap[conf.tname], conf.port = gotestcpudp.NewUDPServer(
				&gotestcpudp.ServerConfiguration{
					Host:               testHost,
					MessageChannelSize: channelSize,
					ReadBufferSize:     bufferSize,
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
			CycleDuration: funks.Duration{Duration: cycleDuration * time.Millisecond},
			Storage:       conf.stype,
			Transport:     string(conf.tname),
		})
	}

	c := &timelinemanager.Configuration{
		Backends:         backends,
		HashingAlgorithm: hashing.SHAKE128,
		HashSize:         12,
		DataTTL:          funks.Duration{Duration: time.Minute},
		DefaultTransportConfig: timeline.DefaultTransportConfig{
			SerializerBufferSize: bufferSize,
			BatchSendInterval:    funks.Duration{Duration: cycleDuration * time.Millisecond},
			RequestTimeout:       funks.Duration{Duration: requestTimeout * time.Second},
			TransportBufferSize:  bufferSize,
			TimeBetweenBatches:   funks.Duration{Duration: 10 * time.Millisecond},
		},
		HTTPTransports:     httpTransportConfsMap,
		OpenTSDBTransports: tcpTransportConfsMap,
		UDPTransports:      udpTransportConfsMap,
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

	<-time.After(requestTimeout * time.Second)

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

	message := <-conf.getTCPServer(tnOpenTSDB).MessageChannel()

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
			stype: timelinemanager.NormalStorage,
			ttype: timelinemanager.OpenTSDBTransport,
			tname: tnOpenTSDB,
		},
	}

	tm, ok := createTimelineManager(t, nil, configs...)
	if !ok {
		return
	}

	defer closeAll(tm, configs)

	testOpenTSDBMessage(t, "TestOpenTSDB", tm, timelinemanager.NormalStorage, timelinemanager.RawOpenTSDB, configs[0])
}

func testSendJSONMessage(
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

func testRequest(t *testing.T, req *gotesthttp.RequestData, number bool) bool {

	if number {

		if !assert.Equalf(t, "/put", req.URI, "expected same uri: %s", req.Body) {
			return false
		}

		return assert.Equalf(t, "PUT", req.Method, "expected same method: %s", req.Body)
	}

	if !assert.Equalf(t, "/text", req.URI, "expected same uri: %s", req.Body) {
		return false
	}

	return assert.Equalf(t, "POST", req.Method, "expected same method: %s", req.Body)
}

func testHTTPMessage(t *testing.T, function string, tm *timelinemanager.Instance, stype timelinemanager.StorageType, op timeline.FlatOperation, conf *storageConfig, number bool) bool {

	metric, value, ok := testSendJSONMessage(t, function, tm, stype, op, number)
	if !ok {
		return false
	}

	var httpServer *gotesthttp.Server
	if number {
		httpServer = conf.getHTTPServer(tnNumber)
	} else {
		httpServer = conf.getHTTPServer(tnText)
	}

	message := gotesthttp.WaitForServerRequest(httpServer, 100*time.Millisecond, 2*time.Second)
	if !assert.NotNil(t, message, "expected a valid request message") {
		return false
	}

	if !testRequest(t, message, number) {
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
	}

	return assert.True(t,
		regexp.MustCompile(
			fmt.Sprintf(`\[\{"metric":"%s","tags":\{"tag[1-2]{1}K_[0-9]+":"tag[1-2]{1}V_[0-9]+","tag[1-2]{1}K_[0-9]+":"tag[1-2]{1}V_[0-9]+"\},"timestamp":[0-9]{10},"text":"%s"\}\]`,
				metric, value.(string))).
			MatchString(message.Body),
		"expected same message",
	)
}

// TestHTTP - creates a new manager http only
func TestHTTP(t *testing.T) {

	configs := []*storageConfig{
		{
			stype: timelinemanager.NormalStorage,
			ttype: timelinemanager.HTTPTransport,
			tname: tnNumber,
		},
		{
			stype: timelinemanager.ArchiveStorage,
			ttype: timelinemanager.HTTPTransport,
			tname: tnText,
		},
	}

	tm, ok := createTimelineManager(t, nil, configs...)
	if !ok {
		return
	}

	defer closeAll(tm, configs)

	for i := 0; i < gotest.RandomInt(2, 5); i++ {
		if !testHTTPMessage(t, "TestHTTP", tm, timelinemanager.NormalStorage, timelinemanager.RawJSON, configs[0], true) {
			return
		}

		if !testHTTPMessage(t, "TestHTTP", tm, timelinemanager.ArchiveStorage, timelinemanager.RawJSON, configs[1], false) {
			return
		}
	}
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
			stype: timelinemanager.NormalStorage,
			ttype: timelinemanager.HTTPTransport,
			tname: tnText,
		},
	}

	tm, ok := createTimelineManager(t, nil, configs...)
	if !ok {
		return
	}

	defer closeAll(tm, configs)

	testUnknownStorage(t, "TestStorageNotFound", tm, timelinemanager.ArchiveStorage, timelinemanager.RawJSON)
}

// TestTransportNotSupported - creates a new manager and tests for a unknown transport
func TestTransportNotSupported(t *testing.T) {

	configs := []*storageConfig{
		{
			stype: timelinemanager.NormalStorage,
			ttype: timelinemanager.OpenTSDBTransport,
			tname: tnOpenTSDB,
		},
	}

	tm, ok := createTimelineManager(t, nil, configs...)
	if !ok {
		return
	}

	defer closeAll(tm, configs)

	testUnknownTransport(t, "TestTransportNotSupported", tm, timelinemanager.NormalStorage, timelinemanager.RawJSON, true, false)
}

// TestBothTransportsWithErrors - creates a new manager and tests http and opentsdb integration (some errors)
func TestBothTransportsWithErrors(t *testing.T) {

	configs := []*storageConfig{
		{
			stype: timelinemanager.ArchiveStorage,
			ttype: timelinemanager.HTTPTransport,
			tname: tnNumber,
		},
		{
			stype: timelinemanager.NormalStorage,
			ttype: timelinemanager.OpenTSDBTransport,
			tname: tnOpenTSDB,
		},
	}

	tm, ok := createTimelineManager(t, nil, configs...)
	if !ok {
		return
	}

	defer closeAll(tm, configs)

	funcName := "TestBothTransportsWithErrors"

	testUnknownStorage(t, funcName, tm, customStorage, timelinemanager.RawJSON)
	testHTTPMessage(t, funcName, tm, timelinemanager.ArchiveStorage, timelinemanager.RawJSON, configs[0], true)
	testUnknownTransport(t, funcName, tm, timelinemanager.ArchiveStorage, timelinemanager.RawOpenTSDB, true, true)
	testHTTPMessage(t, funcName, tm, timelinemanager.ArchiveStorage, timelinemanager.RawJSON, configs[0], true)
	testOpenTSDBMessage(t, funcName, tm, timelinemanager.NormalStorage, timelinemanager.RawOpenTSDB, configs[1])
	testUnknownTransport(t, funcName, tm, timelinemanager.NormalStorage, timelinemanager.RawJSON, true, true)
}

// TestSameBackendConfiguration - creates a new manager duplicating some backend
func TestSameBackendConfiguration(t *testing.T) {

	configs := []*storageConfig{
		{
			stype: timelinemanager.ArchiveStorage,
			ttype: timelinemanager.HTTPTransport,
			tname: tnText,
		},
		{
			stype: timelinemanager.NormalStorage,
			ttype: timelinemanager.OpenTSDBTransport,
			tname: tnOpenTSDB,
		},
		{
			stype: timelinemanager.ArchiveStorage,
			ttype: timelinemanager.OpenTSDBTransport,
			tname: tnOpenTSDB,
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

// TestCustomJSONMapping - creates a new manager with custom json mapping
func TestCustomJSONMapping(t *testing.T) {

	configs := []*storageConfig{
		{
			stype: timelinemanager.NormalStorage,
			ttype: timelinemanager.HTTPTransport,
			tname: tnNumber,
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
		timelinemanager.NormalStorage,
		"custom",
		"tags.tag1", "customVal1",
		"tags.tag3", "3",
	)

	if !assert.NoError(t, err, "expected no error sending custom json") {
		return
	}

	message := <-configs[0].getHTTPServer(tnNumber).RequestChannel()
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

func testUDPMessage(t *testing.T, function string, tm *timelinemanager.Instance, stype timelinemanager.StorageType, op timeline.FlatOperation, conf *storageConfig) bool {

	metric, value, ok := testSendJSONMessage(t, function, tm, stype, op, true)
	if !ok {
		return false
	}

	message := <-conf.getUDPServer(tnUDP).MessageChannel()
	if !assert.NotNil(t, message, "expected a valid request message") {
		return false
	}

	return assert.True(t,
		regexp.MustCompile(
			fmt.Sprintf(`\{"metric":"%s","tags":\{"tag[1-2]{1}K_[0-9]+":"tag[1-2]{1}V_[0-9]+","tag[1-2]{1}K_[0-9]+":"tag[1-2]{1}V_[0-9]+"\},"timestamp":[0-9]{10},"value":%d?(\.[0]+)\}`,
				metric, value.(int))).
			MatchString(message.Message),
		"expected same message",
	)
}

// TestUDP - creates a new manager udp only
func TestUDP(t *testing.T) {

	configs := []*storageConfig{
		{
			stype: timelinemanager.NormalStorage,
			ttype: timelinemanager.UDPTransport,
			tname: tnUDP,
		},
	}

	tm, ok := createTimelineManager(t, nil, configs...)
	if !ok {
		return
	}

	defer closeAll(tm, configs)

	for i := 0; i < gotest.RandomInt(5, 10); i++ {
		testUDPMessage(t, "TestUDP", tm, timelinemanager.NormalStorage, timelinemanager.RawJSON, configs[0])
	}
}

// TestAllTransports - creates a new manager and tests http, opentsdb and udp integrations (no errors)
func TestAllTransports(t *testing.T) {

	configs := []*storageConfig{
		{
			stype: timelinemanager.NormalStorage,
			ttype: timelinemanager.HTTPTransport,
			tname: tnNumber,
		},
		{
			stype: customStorage,
			ttype: timelinemanager.HTTPTransport,
			tname: tnText,
		},
		{
			stype: timelinemanager.ArchiveStorage,
			ttype: timelinemanager.OpenTSDBTransport,
			tname: tnOpenTSDB,
		},
		{
			stype: customUDPStorage,
			ttype: timelinemanager.UDPTransport,
			tname: tnUDP,
		},
	}

	tm, ok := createTimelineManager(t, nil, configs...)
	if !ok {
		return
	}

	defer closeAll(tm, configs)

	testHTTPMessage(t, "TestAllTransports", tm, timelinemanager.NormalStorage, timelinemanager.RawJSON, configs[0], true)
	testOpenTSDBMessage(t, "TestAllTransports", tm, timelinemanager.ArchiveStorage, timelinemanager.RawOpenTSDB, configs[2])
	testHTTPMessage(t, "TestAllTransports", tm, customStorage, timelinemanager.RawJSON, configs[1], false)
	testUDPMessage(t, "TestAllTransports", tm, customUDPStorage, timelinemanager.RawJSON, configs[3])
}
