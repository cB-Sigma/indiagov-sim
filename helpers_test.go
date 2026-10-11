// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package indiagov_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cB-Sigma/indiagov-sim/internal/brokertest"
	"github.com/cB-Sigma/indiagov/mqttconn"
)

var ist = time.FixedZone("IST", 19800)

// platform is the server side of a test: an MQTT session subscribed to
// filters, collecting what arrives.
type platform struct {
	t   *testing.T
	c   mqttconn.Conn
	got chan mqttconn.Message
}

func newPlatform(t *testing.T, url string, filters ...string) *platform {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p := &platform{t: t, got: make(chan mqttconn.Message, 256)}
	up := make(chan struct{}, 1)
	c, err := mqttconn.Paho(ctx, url, "platform-"+strings.ReplaceAll(t.Name(), "/", "-"), filters, func(m mqttconn.Message) { p.got <- m },
		mqttconn.Options{NoLocal: true, OnUp: func(mqttconn.Conn) {
			select {
			case up <- struct{}{}:
			default:
			}
		}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-up: // subscribed
	case <-ctx.Done():
		t.Fatal("the platform did not subscribe")
	}
	p.c = c
	t.Cleanup(c.Close)
	return p
}

func (p *platform) publish(topic string, b []byte) {
	p.t.Helper()
	if err := p.c.Publish(context.Background(), topic, b, false); err != nil {
		p.t.Fatal(err)
	}
}

// next is the next message on a topic ending in suffix, within d.
func (p *platform) next(suffix string, d time.Duration) (mqttconn.Message, bool) {
	deadline := time.After(d)
	for {
		select {
		case m := <-p.got:
			if strings.HasSuffix(m.Topic, suffix) {
				return m, true
			}
		case <-deadline:
			return mqttconn.Message{}, false
		}
	}
}

func broker(t *testing.T) string { return brokertest.Start(t) }

func ctx10(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}
