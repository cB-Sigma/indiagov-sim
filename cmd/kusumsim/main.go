// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

// Command kusumsim plays a PM-KUSUM solar pump controller's Remote
// Monitoring System (RMS) of the "RMS Communication and Security
// Architecture" message profile (2020 and 2022) on an MQTT broker: it
// publishes the pump controller's periodic data message and the RMS
// heartbeat, takes the platform's OTP and info messages, and answers
// on-demand and configuration reads and writes, so an RMS server can be
// tried without a controller. For example:
//
//	kusumsim -broker mqtt://127.0.0.1:1883 -imei 860000000000001 -solution DEMO -every 1m
//
// Topics are IIOT-1/<solution>/<imei>/<type> (with -pubsub, a fifth level
// sub or pub as some field firmware does). The values are our own
// sample message (annexure 1 layout) with the current time. Fault switches: -silent,
// -drop, -delay, -duplicate, -malformed, -wrong-otp, -hold.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/cB-Sigma/indiagov-sim/pmkusum/sim"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "kusumsim:", err)
		os.Exit(1)
	}
}

// run publishes until ctx ends or -count messages went out.
func run(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("kusumsim", flag.ContinueOnError)
	fs.SetOutput(out)
	broker := fs.String("broker", "mqtt://127.0.0.1:1883", "MQTT broker URL (mqtt://user:pass@host:port; mqtts:// for TLS)")
	imei := fs.String("imei", "860000000000001", "the RMS's IMEI")
	solution := fs.String("solution", "DEMO", "the solution level of the topics")
	every := fs.Duration("every", 15*time.Minute, "periodic push interval")
	count := fs.Int("count", 0, "stop after this many data messages (0: never)")
	heartbeat := fs.Bool("heartbeat", true, "send the RMS heartbeat with each data message")
	session := fs.Duration("session", 0, "keep a persistent broker session this long (commands queue while offline)")
	pubsub := fs.Bool("pubsub", false, "use the field topic form with a sub or pub fifth level")
	silent := fs.Bool("silent", false, "never answer commands")
	drop := fs.Int("drop", 0, "lose this many commands before answering")
	delay := fs.Duration("delay", 0, "delay every answer")
	duplicate := fs.Bool("duplicate", false, "send every answer twice")
	malformed := fs.Bool("malformed", false, "answer with bytes that are not JSON")
	wrongOTP := fs.Bool("wrong-otp", false, "carry an OTP pair the platform never issued")
	hold := fs.String("hold", "", "comma-separated keywords writes do not change (DO1: the pump in local mode)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	r, err := sim.DialOptions(ctx, *broker, *imei, *solution, *session, *pubsub)
	if err != nil {
		return err
	}
	defer r.Close()
	r.Silence(*silent)
	r.DropNext(*drop)
	r.SetAnswerDelay(*delay)
	r.SetDuplicateAnswers(*duplicate)
	r.SetMalformedAnswers(*malformed)
	r.SetWrongOTP(*wrongOTP)
	for _, k := range strings.Split(*hold, ",") {
		if k = strings.TrimSpace(k); k != "" {
			r.Hold(k, true)
		}
	}
	fmt.Fprintf(out, "kusumsim RMS %s on %s\n", *imei, *broker)
	t := time.NewTicker(*every)
	defer t.Stop()
	for sent := 0; ; {
		now := time.Now()
		set := map[string]any{"TIMESTAMP": sim.Now(now), "DATE": now.Format("060102")}
		if err := r.Publish(ctx, "data", sim.SamplePump, set); err != nil {
			return err
		}
		if *heartbeat {
			if err := r.Publish(ctx, "heartbeat", sim.SampleHeartbeat, set); err != nil {
				return err
			}
		}
		sent++
		fmt.Fprintf(out, "data %d sent\n", sent)
		if *count > 0 && sent >= *count {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}
