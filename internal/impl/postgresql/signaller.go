// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package pgstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream"
	"github.com/redpanda-data/connect/v4/internal/replication"
)

var _ replication.Signaller = (*controlSignaller)(nil)

type controlSignaller struct {
	schema       string
	tableName    string
	onSignalChan chan *string
	log          *service.Logger

	signalPending atomic.Pointer[replication.ControlSignal]
}

// NewControlSignaller creates an instance of replication.Signaller that detects signal INSERTs on the given schema.tableName.
func NewControlSignaller(schema, tableName string, log *service.Logger) *controlSignaller {
	return &controlSignaller{
		schema:       schema,
		tableName:    tableName,
		onSignalChan: make(chan *string, 1),
		log:          log,
	}
}

// Listen checks for signal related events and stores any valid pending signal.
// Signal rows are published as normal messages; callers should not suppress them.
func (o *controlSignaller) Listen(_ context.Context, signal any) error {
	msg, ok := signal.(pglogicalstream.StreamMessage)
	if !ok {
		return nil
	}
	if msg.Schema != o.schema || msg.Table != o.tableName {
		return nil
	}
	if msg.Operation != pglogicalstream.InsertOpType {
		return nil
	}

	row, ok := msg.Data.(map[string]any)
	if !ok {
		return fmt.Errorf("expected map for %s message data, got %T", o.tableName, msg.Data)
	}

	dataStr, ok := row["data"].(string)
	if !ok {
		return fmt.Errorf("expected string for %s.data column, got %T", o.tableName, row["data"])
	}

	var sig replication.ControlSignal
	if err := json.Unmarshal([]byte(dataStr), &sig); err != nil {
		return fmt.Errorf("unmarshaling signal %s.data: %w", o.tableName, err)
	}

	sig.ID = fmt.Sprintf("%v", row["id"])

	evType, ok := row["type"].(string)
	if !ok {
		return errors.New("parsing 'type' data")
	}
	sig.Type = evType

	log := o.log.With("id", sig.ID, "type", sig.Type)

	// Validate snapshot signals before triggering a stream interruption.
	// Invalid or no-op signals are not stored as pending, so streaming continues uninterrupted.
	if sig.IsSnapshot() {
		if len(sig.DataCollections) == 0 {
			log.Warnf("Signal %q received but data-collections is empty — ignoring, streaming continues uninterrupted", sig.Type)
			return nil
		}
		if len(sig.TableNames(o.schema)) == 0 {
			log.Warnf("Signal %q received but data-collections %v matched no tables for schema %q — ignoring, streaming continues uninterrupted", sig.Type, sig.DataCollections, o.schema)
			return nil
		}
	}

	log.Infof("Signal %q received: operation=%s lsn=%v", sig.Type, msg.Operation, msg.LSN)

	o.signalPending.Store(&sig)
	select {
	case o.onSignalChan <- msg.LSN:
	default:
	}
	return nil
}

func (o *controlSignaller) OnSignal() <-chan *string {
	return o.onSignalChan
}

func (o *controlSignaller) IsPending() (bool, *replication.ControlSignal) {
	sig := o.signalPending.Load()
	return sig != nil, sig
}

func (o *controlSignaller) Reset() {
	o.signalPending.Store(nil)
}
