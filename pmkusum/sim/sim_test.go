// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package sim

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/cB-Sigma/indiagov/mqttconn"
)

type recConn struct {
	mu  sync.Mutex
	pub []string
	got chan struct{}
}

func (c *recConn) Publish(_ context.Context, topic string, payload []byte, _ bool) error {
	c.mu.Lock()
	c.pub = append(c.pub, topic+" "+string(payload))
	c.mu.Unlock()
	select {
	case c.got <- struct{}{}:
	default:
	}
	return nil
}

func (c *recConn) Close() {}

// A command the broker delivers before the dial has returned (a queued
// command of a persistent session, or one sent the moment the subscription
// is made) is still answered, on the connection the dial returns.
func TestCommandBeforeDialReturnsIsAnswered(t *testing.T) {
	conn := &recConn{got: make(chan struct{}, 1)}
	old := DialMQTT
	t.Cleanup(func() { DialMQTT = old })
	DialMQTT = func(_ context.Context, _, _ string, filters []string, handle func(mqttconn.Message), opts mqttconn.Options) (mqttconn.Conn, error) {
		cmd, _ := json.Marshal(map[string]any{"type": "ondemand", "cmd": "read", "msgid": "7", "UPDATEINTERVAL": ""})
		handle(mqttconn.Message{Topic: "IIOT-1/S/860000000000001/ondemand", Payload: cmd})
		time.Sleep(20 * time.Millisecond) // the answer goroutine runs before the dial returns
		go opts.OnUp(conn)
		return conn, nil
	}
	r, err := Dial(context.Background(), "mqtt://unused", "860000000000001", "S")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	select {
	case <-conn.got:
	case <-time.After(5 * time.Second):
		t.Fatal("the command was not answered")
	}
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if len(conn.pub) != 1 {
		t.Fatalf("published %v", conn.pub)
	}
}

// The vendor-format RMS answers a command delivered during the dial too.
func TestVendorCommandBeforeDialReturnsIsAnswered(t *testing.T) {
	conn := &recConn{got: make(chan struct{}, 1)}
	old := DialMQTT
	t.Cleanup(func() { DialMQTT = old })
	DialMQTT = func(_ context.Context, _, _ string, _ []string, handle func(mqttconn.Message), opts mqttconn.Options) (mqttconn.Conn, error) {
		handle(mqttconn.Message{Topic: "v/R1/cmd", Payload: []byte(`{"id":"1","op":"REBOOT"}`)})
		go opts.OnUp(conn)
		return conn, nil
	}
	v, err := DialVendor(context.Background(), "mqtt://unused", "v", "R1")
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	select {
	case <-conn.got:
	case <-time.After(5 * time.Second):
		t.Fatal("the command was not answered")
	}
}
