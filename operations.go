package timelinemanager

import (
	"fmt"
	"time"

	"github.com/uol/logh"
	"github.com/uol/timeline"
)

const (
	// RawJSON - defines a no flattened operation
	RawJSON timeline.FlatOperation = 100
	// RawOpenTSDB - defines a no flattened operation
	RawOpenTSDB timeline.FlatOperation = 101

	cMetric    string = "metric"
	cValue     string = "value"
	cText      string = "text"
	cTimestamp string = "timestamp"
	cTags      string = "tags"
	cEmpty     string = ""
)

func (tm *Instance) logSendOperationError(caller string, err error) {

	if err != nil {
		if logh.ErrorEnabled {
			ev := tm.logger.Error().Err(err)
			if len(caller) > 0 {
				ev = ev.Str(cFunction, caller)
			}
			ev = ev.Str(cFunction, caller)
			ev.Msg("operation error")
		}
	}
}

// SendT - send a point or do a flatten operation with specific timestamp
func (tm *Instance) SendT(caller string, stype StorageType, op timeline.FlatOperation, value float64, metric string, timestamp int64, tags ...interface{}) error {

	if !tm.ready {
		return nil
	}

	backend, ok := tm.backendMap[stype]
	if !ok {
		return tm.storageTypeNotFound(caller, stype)
	}

	tags = append(tags, backend.commonTags...)

	var err error
	if backend.ttype == OpenTSDBTransport {

		if op == RawOpenTSDB {
			err = backend.manager.SendOpenTSDB(value, timestamp, metric, tags...)
		} else if op >= timeline.Avg && op <= timeline.Min {
			err = backend.manager.FlattenOpenTSDB(op, value, timestamp, metric, tags...)
		} else {
			err = ErrTransportNotSupported
		}

	} else if backend.ttype == HTTPTransport || backend.ttype == UDPTransport {

		if op == RawJSON {
			err = backend.manager.SendJSON(
				cHTTPNumberFormat,
				[]interface{}{
					cMetric, metric,
					cValue, value,
					cTimestamp, timestamp,
					cTags, tm.toTagMap(tags),
				}...,
			)
		} else if op >= timeline.Avg && op <= timeline.Min {
			err = backend.manager.FlattenJSON(
				op,
				cHTTPNumberFormat,
				[]interface{}{
					cMetric, metric,
					cValue, value,
					cTimestamp, timestamp,
					cTags, tm.toTagMap(tags),
				}...,
			)
		} else {
			err = ErrTransportNotSupported
		}

	} else {
		err = ErrTransportNotSupported
	}

	tm.logSendOperationError(caller, err)

	return err
}

// Send - send a point or do a flatten operation
func (tm *Instance) Send(caller string, stype StorageType, op timeline.FlatOperation, value float64, metric string, tags ...interface{}) error {

	return tm.SendT(caller, stype, op, value, metric, time.Now().Unix(), tags...)

}

// SendTextT - send a text point with specific timestamp
func (tm *Instance) SendTextT(caller string, stype StorageType, value, metric string, timestamp int64, tags ...interface{}) error {

	if !tm.ready {
		return nil
	}

	backend, ok := tm.backendMap[stype]
	if !ok {
		return tm.storageTypeNotFound(caller, stype)
	}

	tags = append(tags, backend.commonTags...)

	var err error
	if backend.ttype == HTTPTransport || backend.ttype == UDPTransport {

		err = backend.manager.SendJSON(
			cHTTPTextFormat,
			[]interface{}{
				cMetric, metric,
				cText, value,
				cTimestamp, timestamp,
				cTags, tm.toTagMap(tags),
			}...,
		)

	} else {
		err = ErrTransportNotSupported
	}

	tm.logSendOperationError(caller, err)

	return err
}

// SendText - send a text point
func (tm *Instance) SendText(caller string, stype StorageType, value, metric string, tags ...interface{}) error {

	return tm.SendTextT(caller, stype, value, metric, time.Now().Unix(), tags...)

}

// AccumulateHashedData - accumulates a hashed data
func (tm *Instance) AccumulateHashedData(stype StorageType, hash string) (bool, error) {

	backend, ok := tm.backendMap[stype]
	if !ok {
		return false, tm.storageTypeNotFound(cEmpty, stype)
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
func (tm *Instance) StoreHashedData(stype StorageType, hash string, ttl time.Duration, metric string, tags ...interface{}) error {

	backend, ok := tm.backendMap[stype]
	if !ok {
		return tm.storageTypeNotFound(cEmpty, stype)
	}

	tags = append(tags, backend.commonTags...)

	if backend.ttype == OpenTSDBTransport {

		return backend.manager.StoreHashedDataToAccumulateOpenTSDB(
			hash,
			ttl,
			1,
			time.Now().Unix(),
			metric,
			tags...,
		)

	} else if backend.ttype == HTTPTransport || backend.ttype == UDPTransport {

		return backend.manager.StoreHashedDataToAccumulateJSON(
			hash,
			ttl,
			cHTTPNumberFormat,
			[]interface{}{
				cMetric, metric,
				cValue, (float64)(1),
				cTimestamp, time.Now().Unix(),
				cTags, tm.toTagMap(tags),
			}...,
		)

	} else {
		return fmt.Errorf("no transport configured: %s", backend.ttype)
	}
}

func (tm *Instance) toTagMap(tags []interface{}) map[string]interface{} {

	tagMap := map[string]interface{}{}
	for i := 0; i < len(tags); i += 2 {
		tagMap[(tags[i]).(string)] = tags[i+1]
	}

	return tagMap
}

// SendCustomJSON - send a custom json point
func (tm *Instance) SendCustomJSON(caller string, stype StorageType, mappingName string, variables ...interface{}) error {

	if !tm.ready {
		return nil
	}

	backend, ok := tm.backendMap[stype]
	if !ok {
		return tm.storageTypeNotFound(caller, stype)
	}

	var err error
	if backend.ttype == HTTPTransport || backend.ttype == UDPTransport {
		err = backend.manager.SendJSON(mappingName, variables...)
	} else {
		err = ErrTransportNotSupported
	}

	tm.logSendOperationError(caller, err)

	return err
}
