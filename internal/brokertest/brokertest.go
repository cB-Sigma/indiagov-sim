// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

// Package brokertest starts an embedded MQTT broker (mochi-mqtt) for the
// tests.
package brokertest

import (
	"log/slog"
	"testing"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
)

// Start starts a broker on a free loopback port that anyone may use,
// stopped at t's clean-up, and returns its mqtt:// URL.
func Start(t testing.TB) string {
	t.Helper()
	return start(t, new(auth.AllowHook), nil)
}

// StartAuth starts a broker that takes only user with pass and returns
// its mqtt:// URL (without credentials).
func StartAuth(t testing.TB, user, pass string) string {
	t.Helper()
	return start(t, new(auth.Hook), &auth.Options{Ledger: &auth.Ledger{
		Auth: auth.AuthRules{{Username: auth.RString(user), Password: auth.RString(pass), Allow: true}},
		ACL:  auth.ACLRules{{Filters: auth.Filters{"#": auth.ReadWrite}}},
	}})
}

func start(t testing.TB, hook mqtt.Hook, cfg any) string {
	t.Helper()
	s := mqtt.New(&mqtt.Options{InlineClient: false, Logger: slog.New(slog.DiscardHandler)})
	if err := s.AddHook(hook, cfg); err != nil {
		t.Fatal(err)
	}
	ln := listeners.NewTCP(listeners.Config{ID: "test", Address: "127.0.0.1:0"})
	if err := s.AddListener(ln); err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.Serve() }()
	t.Cleanup(func() { _ = s.Close() })
	return "mqtt://" + ln.Address()
}
