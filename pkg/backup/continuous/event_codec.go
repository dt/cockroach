// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package continuous

import (
	"encoding/binary"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// Event codec for continuous backup SST files.
//
// SST files store rangefeed events in time-sorted order. Each event is stored
// as a key-value pair where:
//   - Key: encodes the event timestamp for sorting
//   - Value: encodes the event data (key, value, prev_value)
//
// The codec handles both encoding (during capture) and decoding (during replay).

// decodedEvent represents a decoded event from the SST.
type decodedEvent struct {
	eventType byte // 0=put, 1=delete
	key       roachpb.Key
	value     []byte
	prevValue []byte
}

// encodeEventKey encodes the SST key for an event.
// Format: <wall_time:8><logical:4><sequence:4> = 16 bytes total.
// Uses big-endian encoding so keys sort by timestamp.
func encodeEventKey(ts hlc.Timestamp, seq uint32) roachpb.Key {
	key := make([]byte, 16)
	binary.BigEndian.PutUint64(key[0:8], uint64(ts.WallTime))
	binary.BigEndian.PutUint32(key[8:12], uint32(ts.Logical))
	binary.BigEndian.PutUint32(key[12:16], seq)
	return key
}

// decodeEventTimestamp extracts the timestamp from an SST event key.
// Key format: <wall_time:8><logical:4><seq:4>
func decodeEventTimestamp(key []byte) hlc.Timestamp {
	if len(key) < 12 {
		return hlc.Timestamp{}
	}
	wallTime := int64(binary.BigEndian.Uint64(key[0:8]))
	logical := int32(binary.BigEndian.Uint32(key[8:12]))
	return hlc.Timestamp{WallTime: wallTime, Logical: logical}
}

// encodeEventValue encodes the SST value for an event.
// Format: <event_type:1><key_len:4><key><value_len:4><value><prev_len:4><prev>
func encodeEventValue(event bufferedEvent) []byte {
	keyLen := len(event.key)
	valueLen := len(event.value.RawBytes)
	prevLen := len(event.prevValue.RawBytes)

	size := 1 + 4 + keyLen + 4 + valueLen + 4 + prevLen
	buf := make([]byte, size)

	offset := 0

	// Event type (0 = put, 1 = delete based on empty value)
	if valueLen == 0 {
		buf[offset] = 1 // Delete
	} else {
		buf[offset] = 0 // Put
	}
	offset++

	// Key length and key
	binary.BigEndian.PutUint32(buf[offset:], uint32(keyLen))
	offset += 4
	copy(buf[offset:], event.key)
	offset += keyLen

	// Value length and value
	binary.BigEndian.PutUint32(buf[offset:], uint32(valueLen))
	offset += 4
	copy(buf[offset:], event.value.RawBytes)
	offset += valueLen

	// Prev value length and prev value
	binary.BigEndian.PutUint32(buf[offset:], uint32(prevLen))
	offset += 4
	copy(buf[offset:], event.prevValue.RawBytes)

	return buf
}

// decodeEvent decodes an event value from the SST.
// Value format: <event_type:1><key_len:4><key><value_len:4><value><prev_len:4><prev>
func decodeEvent(data []byte) (decodedEvent, error) {
	if len(data) < 1 {
		return decodedEvent{}, errors.New("event data too short")
	}

	var event decodedEvent
	offset := 0

	// Event type
	event.eventType = data[offset]
	offset++

	// Key
	if len(data) < offset+4 {
		return decodedEvent{}, errors.New("event data too short for key length")
	}
	keyLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if len(data) < offset+keyLen {
		return decodedEvent{}, errors.New("event data too short for key")
	}
	event.key = make([]byte, keyLen)
	copy(event.key, data[offset:offset+keyLen])
	offset += keyLen

	// Value
	if len(data) < offset+4 {
		return decodedEvent{}, errors.New("event data too short for value length")
	}
	valueLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if len(data) < offset+valueLen {
		return decodedEvent{}, errors.New("event data too short for value")
	}
	event.value = make([]byte, valueLen)
	copy(event.value, data[offset:offset+valueLen])
	offset += valueLen

	// Previous value
	if len(data) < offset+4 {
		return decodedEvent{}, errors.New("event data too short for prev length")
	}
	prevLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if len(data) < offset+prevLen {
		return decodedEvent{}, errors.New("event data too short for prev value")
	}
	event.prevValue = make([]byte, prevLen)
	copy(event.prevValue, data[offset:offset+prevLen])

	return event, nil
}
