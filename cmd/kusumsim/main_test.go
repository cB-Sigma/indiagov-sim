// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cB-Sigma/indiagov-sim/internal/brokertest"
	"github.com/cB-Sigma/indiagov/mqttconn"
	"github.com/cB-Sigma/indiagov/pmkusum"
)

// The runner's data message arrives on the profile's topic and decodes as
// a pump controller message.
func TestRunnerPublishesPumpData(t *testing.T) {
	url := brokertest.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	got := make(chan mqttconn.Message, 1)
	c, err := mqttconn.Paho(ctx, url, "server", []string{"IIOT-1/DEMO/+/data"}, func(m mqttconn.Message) {
		select {
		case got <- m:
		default:
		}
	}, mqttconn.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	time.Sleep(200 * time.Millisecond) // the subscription is made after the connection
	if err := run(ctx, []string{"-broker", url, "-imei", "860000000000001", "-count", "1"}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-got:
		if m.Topic != "IIOT-1/DEMO/860000000000001/data" {
			t.Errorf("topic %s", m.Topic)
		}
		msg, err := pmkusum.Decode(m.Payload, time.FixedZone("IST", 19800))
		if err != nil || msg.Group != pmkusum.GroupPump || !msg.HasTime {
			t.Fatalf("decoded %+v, %v", msg, err)
		}
	case <-ctx.Done():
		t.Fatal("no data message")
	}
}

// Running, the runner answers commands; with -hold DO1 a pump write
// comes back with the pump's old value.
func TestRunnerAnswersCommands(t *testing.T) {
	url := brokertest.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	got := make(chan mqttconn.Message, 16)
	c, err := mqttconn.Paho(ctx, url, "server", []string{"IIOT-1/DEMO/+/+"}, func(m mqttconn.Message) { got <- m }, mqttconn.Options{NoLocal: true})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	time.Sleep(200 * time.Millisecond)
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- run(runCtx, []string{"-broker", url, "-every", "1h", "-hold", "DO1"}, &bytes.Buffer{})
	}()
	for m := range got {
		if strings.HasSuffix(m.Topic, "/data") {
			break
		}
	}
	cmd := pmkusum.Pump("", "77", time.Now(), true)
	b, _ := cmd.Encode(time.UTC)
	if err := c.Publish(ctx, "IIOT-1/DEMO/860000000000001/ondemand", b, false); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case m := <-got:
			a, err := pmkusum.Decode(m.Payload, time.UTC)
			if err != nil || a.MsgID != "77" {
				continue
			}
			if res := pmkusum.Evaluate(cmd, a); len(res.NotApplied) != 1 {
				t.Errorf("held pump: %+v", res)
			}
			stop()
			if err := <-done; err != nil {
				t.Error(err)
			}
			return
		case <-ctx.Done():
			t.Fatal("no answer")
		}
	}
}
