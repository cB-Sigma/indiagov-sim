// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package sim

import (
	"context"
	"sync"
	"time"

	"github.com/cB-Sigma/indiagov/mqttconn"
)

// DialMQTT opens the simulators' MQTT sessions: Eclipse Paho's MQTT 5 client
// by default. Tests that run their own MQTT client set their own.
var DialMQTT mqttconn.Dialer = mqttconn.Paho

// dialSubscribed is DialMQTT that returns once the subscriptions are in
// place (the first OnUp), so a command published right after the dial
// is not lost; it waits at most 5 s or until ctx ends.
func dialSubscribed(ctx context.Context, url, clientID string, filters []string, handle func(mqttconn.Message), opts mqttconn.Options) (mqttconn.Conn, error) {
	ready := make(chan struct{})
	var once sync.Once
	prev := opts.OnUp
	opts.OnUp = func(c mqttconn.Conn) {
		once.Do(func() { close(ready) })
		if prev != nil {
			prev(c)
		}
	}
	c, err := DialMQTT(ctx, url, clientID, filters, handle, opts)
	if err != nil {
		return nil, err
	}
	if len(filters) > 0 {
		select {
		case <-ready:
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
	}
	return c, nil
}
