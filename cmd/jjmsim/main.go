// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

// Command jjmsim plays Jal Jeevan Mission field devices: it publishes the
// device-to-cloud message (our own, in the report's layout) of each device type of the JJM
// Technical/Expert Committee report (flow meter, ground water level,
// service reservoir level, chlorine, turbidity, pH, pressure, pump
// controller) on an MQTT topic, with the current time, so a JJM server
// can be tried without devices. For example:
//
//	jjmsim -broker mqtt://127.0.0.1:1883 -topic jjm/MH_D02_B03_V04 -every 15m
//
// With -serve it instead stands in for the other end of a push: a
// state-to-centre API (-serve central, POST <url>/<Name>DataInsert) or the
// eJalShakti IoT API (-serve portal, POST <url>/IoT_API/API/<API> with the
// key -key for every API). It prints the URL and serves until stopped;
// -fail, -down and -malformed (central only) are fault switches.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cB-Sigma/indiagov-sim/jjm/sim"
	"github.com/cB-Sigma/indiagov/jjm"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "jjmsim:", err)
		os.Exit(1)
	}
}

// stamped is a sample with dt set to now (IST, as the report writes it).
func stamped(sample string, now time.Time) (string, error) {
	var v map[string]any
	if err := json.Unmarshal([]byte(sample), &v); err != nil {
		return "", err
	}
	v["dt"] = now.In(time.FixedZone("IST", 19800)).Format("2006-01-02T15:04:05")
	b, err := json.Marshal(v)
	return string(b), err
}

// run publishes until ctx ends or -rounds rounds went out, or serves
// until ctx ends.
func run(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("jjmsim", flag.ContinueOnError)
	fs.SetOutput(out)
	broker := fs.String("broker", "mqtt://127.0.0.1:1883", "MQTT broker URL")
	topic := fs.String("topic", "jjm/MH_D02_B03_V04", "topic to publish on")
	every := fs.Duration("every", 15*time.Minute, "interval between rounds of messages")
	rounds := fs.Int("rounds", 0, "stop after this many rounds (0: never)")
	serve := fs.String("serve", "", "serve an API instead: central or portal")
	key := fs.String("key", "", "the API key the served API requires")
	fail := fs.Int("fail", 0, "fail this many requests first (central: Status 0; portal: HTTP 500)")
	down := fs.Bool("down", false, "answer every request 503")
	malformed := fs.Bool("malformed", false, "central: answer with bytes that are not JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	switch *serve {
	case "":
	case "central":
		c := sim.NewCentral()
		defer c.Close()
		c.SetKey(*key)
		c.FailNext(*fail)
		c.SetDown(*down)
		c.SetMalformed(*malformed)
		fmt.Fprintf(out, "jjmsim central API on %s\n", c.URL)
		<-ctx.Done()
		return nil
	case "portal":
		keys := map[string]string{}
		for _, a := range jjm.PortalAPIs {
			keys[a.Name] = *key
		}
		p := sim.NewPortal(keys)
		defer p.Close()
		p.FailNext(*fail)
		p.SetDown(*down)
		fmt.Fprintf(out, "jjmsim eJalShakti portal on %s/IoT_API/API\n", p.URL)
		<-ctx.Done()
		return nil
	default:
		return fmt.Errorf("-serve %q must be central or portal", *serve)
	}
	g, err := sim.DialGateway(ctx, *broker, "jjmsim")
	if err != nil {
		return err
	}
	defer g.Close()
	fmt.Fprintf(out, "jjmsim on %s, topic %s\n", *broker, *topic)
	t := time.NewTicker(*every)
	defer t.Stop()
	for round := 1; ; round++ {
		for _, s := range sim.Samples {
			msg, err := stamped(s, time.Now())
			if err != nil {
				return err
			}
			if err := g.Publish(ctx, *topic, msg); err != nil {
				return err
			}
		}
		fmt.Fprintf(out, "round %d: %d messages sent\n", round, len(sim.Samples))
		if *rounds > 0 && round >= *rounds {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}
