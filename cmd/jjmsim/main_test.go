// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/cB-Sigma/indiagov-sim/internal/brokertest"
	"github.com/cB-Sigma/indiagov-sim/jjm/sim"
	"github.com/cB-Sigma/indiagov/jjm"
	"github.com/cB-Sigma/indiagov/mqttconn"
)

// With -serve the runner stands in for the central API or the portal and
// takes a push from the library's clients; an unknown mode is refused.
func TestRunnerServes(t *testing.T) {
	for _, mode := range []string{"central", "portal"} {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		pr, pw := io.Pipe()
		done := make(chan error, 1)
		go func() { done <- run(ctx, []string{"-serve", mode, "-key", "demo-key"}, pw) }()
		line, err := bufio.NewReader(pr).ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		url := strings.TrimSpace(line[strings.LastIndex(line, " ")+1:])
		go func() { _, _ = io.Copy(io.Discard, pr) }()
		if mode == "central" {
			var p jjm.Payload
			_ = json.Unmarshal([]byte(sim.SampleFlowmeter), &p)
			if _, err := (jjm.Central{BaseURL: url, Key: "demo-key"}).Post(ctx, p); err != nil {
				t.Errorf("central: %v", err)
			}
		} else {
			body := map[string]any{"VillageId": "2730100123", "GatewayId": "GW-DEMO-01", "WaterSupply": 42.5, "LPCD": 55, "Date": "2026-10-10"}
			if _, err := (jjm.EJClient{BaseURL: url, Key: "demo-key"}).Push(ctx, "FlowMeter", body); err != nil {
				t.Errorf("portal: %v", err)
			}
		}
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
		pw.Close()
	}
	if err := run(context.Background(), []string{"-serve", "ftp"}, io.Discard); err == nil {
		t.Error("an unknown mode was taken")
	}
}

// Every message of a round arrives and passes the decoder.
func TestRunnerPublishesSamples(t *testing.T) {
	url := brokertest.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	got := make(chan []byte, len(sim.Samples))
	c, err := mqttconn.Paho(ctx, url, "server", []string{"jjm/#"}, func(m mqttconn.Message) { got <- m.Payload }, mqttconn.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	time.Sleep(200 * time.Millisecond)
	if err := run(ctx, []string{"-broker", url, "-rounds", "1"}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	dec := jjm.Decoder{Loc: time.FixedZone("IST", 19800)}
	for i := range sim.Samples {
		select {
		case b := <-got:
			if _, err := dec.Decode(b); err != nil {
				t.Errorf("message %d: %v\n%s", i, err, b)
			}
		case <-ctx.Done():
			t.Fatalf("%d of %d messages", i, len(sim.Samples))
		}
	}
}
