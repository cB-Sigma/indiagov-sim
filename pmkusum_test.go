// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package indiagov_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cB-Sigma/indiagov-sim/internal/brokertest"
	"github.com/cB-Sigma/indiagov-sim/pmkusum/sim"
	"github.com/cB-Sigma/indiagov/pmkusum"
)

const imei = "860000000000001"

// A sample message of every annexure layout (1 to 6, the lower-case
// heartbeat and the 2022 USPC
// push) published by the simulated RMS arrives on its topic and decodes
// into its equipment group, with the instance, time and values the
// sample carries.
func TestKusumSamplesOverMQTT(t *testing.T) {
	url := broker(t)
	p := newPlatform(t, url, "IIOT-1/+/+/+")
	r, err := sim.Dial(ctx10(t), url, imei, "Standalonesolarpump")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	cases := []struct {
		name, sample, msgType string
		group                 pmkusum.Group
		value                 string
		want                  any
	}{
		{"pump", sim.SamplePump, pmkusum.TypeData, pmkusum.GroupPump, "outputPower", 4.2},
		{"uspc", sim.SampleUSPC, pmkusum.TypeData, pmkusum.GroupPump, "todayEnergy", 2.48},
		{"meter", sim.SampleMeter, pmkusum.TypeData, pmkusum.GroupMeter, "currentR", 6.12},
		{"inverter", sim.SampleInverter, pmkusum.TypeData, pmkusum.GroupInverter, "dc1Voltage", 610.0},
		{"sjb", sim.SampleSJB, pmkusum.TypeData, pmkusum.GroupSJB, "channel10Current", 5.9},
		{"heartbeat", sim.SampleHeartbeat, pmkusum.TypeHeartbeat, pmkusum.GroupRMS, "rssi", 18.0},
		{"heartbeat lower case", sim.SampleHeartbeatLower, pmkusum.TypeHeartbeat, pmkusum.GroupRMS, "rssi", 17.0},
		{"daq", sim.SampleDAQ, pmkusum.TypeData, pmkusum.GroupDAQ, "analogInput1", 12.4},
	}
	for _, c := range cases {
		if err := r.Publish(context.Background(), c.msgType, c.sample, nil); err != nil {
			t.Fatal(err)
		}
		m, ok := p.next("/"+c.msgType, 5*time.Second)
		if !ok {
			t.Fatalf("%s: nothing arrived", c.name)
		}
		rt, ok := pmkusum.ParseTopic(m.Topic)
		if !ok || rt.IMEI != imei || rt.Type != c.msgType {
			t.Errorf("%s: topic %s read as %+v", c.name, m.Topic, rt)
		}
		msg, err := pmkusum.Decode(m.Payload, ist)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if msg.Group != c.group || msg.IMEI != imei || !msg.HasTime || msg.StorageDate != "2026-10-10" {
			t.Errorf("%s: group %s, imei %s, time %v, date %s", c.name, msg.Group, msg.IMEI, msg.HasTime, msg.StorageDate)
		}
		got := msg.Values[c.value]
		if c.name == "uspc" {
			chans := msg.Values["channels"].(map[string]any)
			got = chans["1"].(map[string]any)[c.value]
			if len(chans) != 5 || chans["2"].(map[string]any)["fault"] != 1.0 {
				t.Errorf("uspc channels %v", chans)
			}
		}
		if got != c.want {
			t.Errorf("%s: %s = %v, want %v", c.name, c.value, got, c.want)
		}
		if len(msg.Other) != 0 {
			t.Errorf("%s: keywords no table knows: %v", c.name, msg.Other)
		}
	}
}

// The platform's OTP reaches the RMS, which carries it in its messages;
// the platform takes them. With SetWrongOTP the RMS's messages carry a
// pair the platform never issued, and the platform refuses them.
func TestKusumOTP(t *testing.T) {
	url := broker(t)
	p := newPlatform(t, url, "IIOT-1/+/+/data")
	r, err := sim.Dial(ctx10(t), url, imei, "Standalonesolarpump")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	src := pmkusum.OTPSource{Secret: []byte("demo-otp-secret-0001"), Digits: 6}
	b, _ := src.Message(imei, time.Now()).Encode(ist)
	p.publish(pmkusum.Topic("", "Standalonesolarpump", imei, pmkusum.TypeOTP), b)
	deadline := time.Now().Add(5 * time.Second)
	for _, c := r.OTP(); c == ""; _, c = r.OTP() {
		if time.Now().After(deadline) {
			t.Fatal("the RMS did not get the OTP")
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, wrong := range []bool{false, true} {
		r.SetWrongOTP(wrong)
		if err := r.PublishMessage(context.Background(), pmkusum.TypeData, pmkusum.Header{VD: 1, Time: time.Now(), MaxIndex: 96, Index: 1, StInterval: 15}, ist,
			map[string]any{"POPKW1": "1.5"}); err != nil {
			t.Fatal(err)
		}
		m, ok := p.next("/data", 5*time.Second)
		if !ok {
			t.Fatal("no data message")
		}
		msg, _ := pmkusum.Decode(m.Payload, ist)
		if src.Valid(imei, msg.COTP, time.Now()) == wrong {
			t.Errorf("wrong OTP %v: COTP %s judged %v", wrong, msg.COTP, !wrong)
		}
	}
}

// command sends c and waits for the answer with its msgid.
func command(t *testing.T, p *platform, solution string, c pmkusum.Command, wait time.Duration) (*pmkusum.Message, bool) {
	t.Helper()
	b, err := c.Encode(ist)
	if err != nil {
		t.Fatal(err)
	}
	p.publish(pmkusum.Topic("", solution, imei, c.Type), b)
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		m, ok := p.next("/"+c.Type, time.Until(deadline))
		if !ok {
			break
		}
		a, err := pmkusum.Decode(m.Payload, ist)
		if err == nil && a.MsgID == c.MsgID {
			return a, true
		}
	}
	return nil, false
}

// Every annexure 7 action, sent to the RMS, comes back applied; the
// RMS's state holds the written values.
func TestKusumActions(t *testing.T) {
	url := broker(t)
	p := newPlatform(t, url, "IIOT-1/+/+/ondemand", "IIOT-1/+/+/config")
	r, err := sim.Dial(ctx10(t), url, imei, "Standalonesolarpump")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for _, k := range pmkusumConfigKeys() {
		if r.Get(k) == nil {
			r.Set(k, 0) // the simulated RMS knows every annexure 7 keyword
		}
	}
	now := time.Now()
	must := func(c pmkusum.Command, err error) pmkusum.Command {
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	cmds := []pmkusum.Command{
		pmkusum.Pump("", "1", now, true),
		pmkusum.Pump(pmkusum.TypeConfig, "2", now, false),
		pmkusum.Restart("3", now),
		must(pmkusum.Interval("4", now, 10)),
		must(pmkusum.HeartbeatInterval("5", now, 2)),
		pmkusum.SyncClock("6", now, now, ist),
		pmkusum.GSMSync("7", now),
		must(pmkusum.APN("8", now, 1, "demo.apn", "demo", "demo-pass")),
		pmkusum.Broker("9", now, "rms.example.invalid", 8883, "d:"+imei, "demo", "demo-pass"),
		pmkusum.Firmware("10", now, "ftp.example.invalid", 21, "demo", "demo-pass"),
		must(pmkusum.AnalogCalibration("11", now, 2, 4, 20)),
		pmkusum.Read(pmkusum.TypeOnDemand, "12", now, "UPDATEINTERVAL", "HEARTINTERVAL", "DO1"),
	}
	for _, c := range cmds {
		a, ok := command(t, p, "Standalonesolarpump", c, 5*time.Second)
		if !ok {
			t.Fatalf("no answer to %s %v", c.MsgID, c.Params)
		}
		if res := pmkusum.Evaluate(c, a); !res.OK() {
			t.Errorf("command %s %v: %v (%v)", c.MsgID, c.Params, res.Err(), res.Values)
		}
	}
	if r.Get("UPDATEINTERVAL") != json.Number("10") || r.Get("HEARTINTERVAL") != json.Number("2") || r.Get("DO1") != json.Number("0") {
		t.Errorf("state %v %v %v", r.Get("UPDATEINTERVAL"), r.Get("HEARTINTERVAL"), r.Get("DO1"))
	}
	if n := len(r.Commands()); n != len(cmds) {
		t.Errorf("%d commands recorded", n)
	}
}

func pmkusumConfigKeys() []string {
	var out []string
	for k := range pmkusum.ConfigKeys {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// Refusals and faults: an unknown keyword (annexure 7 command B), a held
// output, a dropped command and its retry, a silent RMS, a slow one,
// duplicate answers and an answer that is not JSON.
func TestKusumCommandFaults(t *testing.T) {
	url := broker(t)
	p := newPlatform(t, url, "IIOT-1/+/+/ondemand", "IIOT-1/+/+/config")
	r, err := sim.Dial(ctx10(t), url, imei, "Standalonesolarpump")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	now := time.Now()

	var b map[string]any
	_ = json.Unmarshal([]byte(sim.SampleInvalidKeyCommand), &b)
	params := map[string]any{}
	for k, v := range b {
		if k != "timestamp" && k != "type" && k != "cmd" && k != "msgid" {
			params[k] = v
		}
	}
	c := pmkusum.Write(pmkusum.TypeConfig, "130", now, params)
	a, ok := command(t, p, "Standalonesolarpump", c, 5*time.Second)
	if res := pmkusum.Evaluate(c, a); !ok || !slices.Equal(res.Invalid, []string{"APNX1"}) {
		t.Errorf("command B: %+v", res)
	}
	var want map[string]any
	_ = json.Unmarshal([]byte(sim.SampleInvalidKeyResponse), &want)
	if a.Raw["APNX1"] != json.Number("0") || a.Raw["HEARTINTERVAL"] != json.Number("5") || want["APNX1"] != 0.0 {
		t.Errorf("command B answer %v, sample %v", a.Raw, want)
	}

	r.Hold("DO1", true)
	c = pmkusum.Pump("", "201", now, true)
	a, ok = command(t, p, "Standalonesolarpump", c, 5*time.Second)
	if res := pmkusum.Evaluate(c, a); !ok || !slices.Equal(res.NotApplied, []string{"DO1"}) {
		t.Errorf("held pump: %+v", res)
	}
	r.Hold("DO1", false)

	r.DropNext(1)
	c = pmkusum.Pump("", "202", now, true)
	if _, ok := command(t, p, "Standalonesolarpump", c, 700*time.Millisecond); ok {
		t.Error("a dropped command was answered")
	}
	if a, ok := command(t, p, "Standalonesolarpump", c, 5*time.Second); !ok || !pmkusum.Evaluate(c, a).OK() {
		t.Error("the retry was not answered")
	}

	r.Silence(true)
	if _, ok := command(t, p, "Standalonesolarpump", pmkusum.GSMSync("203", now), 700*time.Millisecond); ok {
		t.Error("a silent RMS answered")
	}
	r.Silence(false)

	r.SetAnswerDelay(800 * time.Millisecond)
	start := time.Now()
	if _, ok := command(t, p, "Standalonesolarpump", pmkusum.GSMSync("204", now), 5*time.Second); !ok || time.Since(start) < 700*time.Millisecond {
		t.Errorf("slow answer: %v after %v", ok, time.Since(start))
	}
	r.SetAnswerDelay(0)

	r.SetDuplicateAnswers(true)
	if _, ok := command(t, p, "Standalonesolarpump", pmkusum.GSMSync("205", now), 5*time.Second); !ok {
		t.Fatal("no answer")
	}
	if m, ok := p.next("/config", 2*time.Second); !ok || !strings.Contains(string(m.Payload), `"205"`) {
		t.Error("the answer did not come twice")
	}
	r.SetDuplicateAnswers(false)

	r.SetMalformedAnswers(true)
	b2, _ := pmkusum.GSMSync("206", now).Encode(ist)
	p.publish(pmkusum.Topic("", "Standalonesolarpump", imei, pmkusum.TypeConfig), b2)
	m, ok := p.next("/config", 5*time.Second)
	if !ok {
		t.Fatal("no answer")
	}
	if _, err := pmkusum.Decode(m.Payload, ist); err == nil {
		t.Errorf("the malformed answer %s decoded", m.Payload)
	}
}

// A stored record asked for again (pmkusum.Retrieve) is answered with
// LOAD 1 and sent on the data topic; one not stored is answered LOAD 0.
func TestKusumRetrieve(t *testing.T) {
	url := broker(t)
	p := newPlatform(t, url, "IIOT-1/+/+/ondemand", "IIOT-1/+/+/data")
	r, err := sim.Dial(ctx10(t), url, imei, "Standalonesolarpump")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rec, _ := pmkusum.Encode(pmkusum.Header{VD: 1, Time: time.Date(2026, 10, 10, 6, 15, 0, 0, ist), MaxIndex: 96, Index: 25, StInterval: 15, IMEI: imei, Asset: "ASN_11", AssetValue: "DEMO"},
		ist, map[string]any{"POPKW1": "2.5"})
	if err := r.Store(1, "2026-10-10", 25, rec); err != nil {
		t.Fatal(err)
	}
	c, _ := pmkusum.Retrieve("300", time.Now(), 1, "2026-10-10", 25)
	a, ok := command(t, p, "Standalonesolarpump", c, 5*time.Second)
	if !ok || !pmkusum.Evaluate(c, a).OK() {
		t.Fatalf("retrieve answer %+v", a)
	}
	m, ok := p.next("/data", 5*time.Second)
	if !ok {
		t.Fatal("the record was not sent again")
	}
	if msg, _ := pmkusum.Decode(m.Payload, ist); msg == nil || *msg.Index != 25 || msg.StorageDate != "2026-10-10" || msg.Values["outputPower"] != 2.5 {
		t.Errorf("record %s", m.Payload)
	}
	c, _ = pmkusum.Retrieve("301", time.Now(), 1, "2026-10-10", 26)
	a, ok = command(t, p, "Standalonesolarpump", c, 5*time.Second)
	if res := pmkusum.Evaluate(c, a); !ok || !slices.Equal(res.NotApplied, []string{"LOAD"}) {
		t.Errorf("missing record: %+v", res)
	}
}

// An RMS with a persistent session gets a command published while it was
// offline once it is back; one without misses it.
func TestKusumOfflineQueue(t *testing.T) {
	url := broker(t)
	p := newPlatform(t, url, "IIOT-1/+/+/config")
	r, err := sim.DialSession(ctx10(t), url, imei, "Standalonesolarpump", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	time.Sleep(200 * time.Millisecond)
	r.Offline()
	time.Sleep(200 * time.Millisecond)
	c := pmkusum.Restart("400", time.Now())
	b, _ := c.Encode(ist)
	p.publish(pmkusum.Topic("", "Standalonesolarpump", imei, pmkusum.TypeConfig), b)
	if err := r.Online(ctx10(t)); err != nil {
		t.Fatal(err)
	}
	m, ok := p.next("/config", 5*time.Second)
	if !ok || !strings.Contains(string(m.Payload), `"400"`) {
		t.Fatal("the queued command was not answered after reconnecting")
	}
}

// The field topic form with /pub and /sub levels: the RMS receives its
// commands on .../sub and answers on .../pub; ParseTopic reads both.
func TestKusumPubSubTopics(t *testing.T) {
	url := broker(t)
	p := newPlatform(t, url, "IIOT-1/+/+/+/pub")
	r, err := sim.DialOptions(ctx10(t), url, imei, "standalonesolarpump", 0, true)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	c := pmkusum.GSMSync("500", time.Now())
	b, _ := c.Encode(ist)
	p.publish("IIOT-1/standalonesolarpump/"+imei+"/config/sub", b)
	m, ok := p.next("/pub", 5*time.Second)
	if !ok {
		t.Fatal("no answer")
	}
	rt, ok := pmkusum.ParseTopic(m.Topic)
	if !ok || rt.Type != pmkusum.TypeConfig || rt.Direction != "pub" || rt.IMEI != imei {
		t.Errorf("answer topic %s read as %+v", m.Topic, rt)
	}
	if err := r.Publish(context.Background(), pmkusum.TypeHeartbeat, sim.SampleHeartbeat, nil); err != nil {
		t.Fatal(err)
	}
	if m, ok := p.next("/heartbeat/pub", 5*time.Second); !ok {
		t.Error("no heartbeat on .../heartbeat/pub")
	} else if msg, err := pmkusum.Decode(m.Payload, ist); err != nil || msg.Group != pmkusum.GroupRMS {
		t.Errorf("heartbeat %v", err)
	}
}

// The info message reaches the RMS; a malformed message from the RMS is
// refused by the decoder; a broker that refuses the RMS's credentials
// keeps it from coming up.
func TestKusumInfoMalformedAndAuth(t *testing.T) {
	url := broker(t)
	p := newPlatform(t, url, "IIOT-1/+/+/data")
	r, err := sim.Dial(ctx10(t), url, imei, "Standalonesolarpump")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	p.publish(pmkusum.Topic("", "Standalonesolarpump", imei, pmkusum.TypeInfo), []byte(`{"HEARTINTERVAL":5}`))
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(string(r.Info()), "HEARTINTERVAL") {
		if time.Now().After(deadline) {
			t.Fatal("no info message")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := r.PublishRaw(context.Background(), pmkusum.TypeData, []byte(`{"VD":1,"IMEI":`)); err != nil {
		t.Fatal(err)
	}
	m, ok := p.next("/data", 5*time.Second)
	if !ok {
		t.Fatal("nothing arrived")
	}
	if _, err := pmkusum.Decode(m.Payload, ist); err == nil {
		t.Error("a malformed message decoded")
	}

	secured := brokertest.StartAuth(t, "rms", "demo-pass")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := sim.Dial(ctx, strings.Replace(secured, "mqtt://", "mqtt://rms:wrong@", 1), imei, "Standalonesolarpump"); err == nil {
		t.Error("an RMS with a wrong password came up")
	} else if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "MQTT broker") {
		t.Errorf("error %v", err)
	}
	ok2, err := sim.Dial(ctx10(t), strings.Replace(secured, "mqtt://", "mqtt://rms:demo-pass@", 1), imei, "Standalonesolarpump")
	if err != nil {
		t.Fatalf("the right password: %v", err)
	}
	ok2.Close()
}

// The vendor-style RMS (a format no agency publishes): telemetry, a
// backlog, commands answered OK, refused in local mode, an unknown
// operation, a bad value, and silence.
func TestVendorRMS(t *testing.T) {
	url := broker(t)
	p := newPlatform(t, url, "vend/+/telemetry", "vend/+/backlog", "vend/+/ack")
	v, err := sim.DialVendor(ctx10(t), url, "vend", "R1")
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	now := time.Now()
	if err := v.Publish(context.Background(), now, "ON"); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.next("/telemetry", 5*time.Second); !ok {
		t.Fatal("no telemetry")
	}
	if err := v.PublishBacklog(context.Background(), v.Telemetry(now.Add(-time.Hour), "OFF"), v.Telemetry(now, "DRY_RUN")); err != nil {
		t.Fatal(err)
	}
	if m, ok := p.next("/backlog", 5*time.Second); !ok || strings.Count(string(m.Payload), `"ts"`) != 2 {
		t.Fatal("no backlog of two")
	}
	ack := func(cmd string) map[string]any {
		t.Helper()
		p.publish("vend/R1/cmd", []byte(cmd))
		m, ok := p.next("/ack", 3*time.Second)
		if !ok {
			return nil
		}
		var a map[string]any
		_ = json.Unmarshal(m.Payload, &a)
		return a
	}
	if a := ack(`{"id":"1","op":"PUMP","value":"ON"}`); a["result"] != "OK" || !v.Running() {
		t.Errorf("pump on: %v", a)
	}
	v.Local(true)
	if a := ack(`{"id":"2","op":"PUMP","value":"OFF"}`); a["result"] != "FAIL" || a["reason"] != "LOCAL_MODE" || !v.Running() {
		t.Errorf("local mode: %v", a)
	}
	v.Local(false)
	if a := ack(`{"id":"3","op":"RATE","value":90}`); a["result"] != "FAIL" || v.Rate() != 15 {
		t.Errorf("bad rate: %v", a)
	}
	if a := ack(`{"id":"4","op":"RATE","value":5}`); a["result"] != "OK" || v.Rate() != 5 {
		t.Errorf("rate: %v", a)
	}
	if a := ack(`{"id":"5","op":"FLY"}`); a["reason"] != "UNKNOWN_OP" {
		t.Errorf("unknown op: %v", a)
	}
	v.Silence(true)
	if a := ack(`{"id":"6","op":"REBOOT"}`); a != nil {
		t.Errorf("a silent RMS answered %v", a)
	}
	if len(v.Commands()) != 6 {
		t.Errorf("%d commands", len(v.Commands()))
	}
}
