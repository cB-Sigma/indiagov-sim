// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package indiagov_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/cB-Sigma/indiagov-sim/jjm/sim"
	"github.com/cB-Sigma/indiagov/jjm"
)

// A message of every device type in the report's layout, sent by the
// simulated gateway over MQTT, decodes with its values (ground water
// depth sent in feet, recorded in metres); a flow meter's code 01 means
// tamper by default and leakage with the section 5(f) table.
func TestJJMSamplesOverMQTT(t *testing.T) {
	url := broker(t)
	p := newPlatform(t, url, "jjm/#")
	g, err := sim.DialGateway(ctx10(t), url, "gw-demo")
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	want := map[string]struct {
		value string
		v     float64
		flags string
	}{
		"fm":  {"quantity", 48250.5, "good"},
		"gl":  {"groundWaterDepth", 11.64336, "good"}, // 38.20 ft
		"sl":  {"reservoirLevelMetres", 3.15, "good"},
		"ca":  {"chlorine", 0.42, "good"},
		"tbs": {"turbidity", 1.8, "good"},
		"ph":  {"ph", 7.35, "good"},
		"ps":  {"pressure", 2.6, "good"},
		"pc":  {"current", 18.4, "good"},
	}
	for _, s := range sim.Samples {
		if err := g.Publish(context.Background(), "jjm/MH_D02_B03_V04", s); err != nil {
			t.Fatal(err)
		}
		m, ok := p.next("MH_D02_B03_V04", 5*time.Second)
		if !ok {
			t.Fatal("nothing arrived")
		}
		r, err := jjm.Decoder{Loc: ist}.Decode(m.Payload)
		if err != nil {
			t.Fatal(err)
		}
		w := want[r.DTY]
		if r.Values[w.value] != w.v || r.Quality() != w.flags || !r.HasTime || r.ErrorCode != "00" {
			t.Errorf("%s: %s = %v, quality %s", r.DTY, w.value, r.Values[w.value], r.Quality())
		}
		if len(r.Unknown) != 0 {
			t.Errorf("%s: unknown codes %v", r.DTY, r.Unknown)
		}
	}
	_ = g.Publish(context.Background(), "jjm/MH_D02_B03_V04", sim.SampleFlowmeterSection5)
	m5, ok := p.next("MH_D02_B03_V04", 5*time.Second)
	if !ok {
		t.Fatal("nothing arrived")
	}
	for table, want := range map[string]string{"": "tamper", "annex": "tamper", "section5": "leakage"} {
		if r, err := (jjm.Decoder{Loc: ist, FlowErrorCodes: table}).Decode(m5.Payload); err != nil || r.Error != want {
			t.Errorf("table %q: %v, %v", table, r.Error, err)
		}
	}
	var resp jjm.CentralResponse
	if err := json.Unmarshal([]byte(sim.SampleAPIResponse), &resp); err != nil || !resp.OK() || resp.DeviceID != "fm01" {
		t.Errorf("the output sample: %+v, %v", resp, err)
	}
	// A message built with the library and a malformed one.
	pl, _ := jjm.NewPayload("MH_D02_B03_V04", "fm02", "fm", time.Now(), ist, map[string]any{"t": 5.5, "f": 1.0, "b": 90, "e": jjm.ErrorTamper})
	if err := g.PublishPayload(context.Background(), "jjm/MH_D02_B03_V04", pl); err != nil {
		t.Fatal(err)
	}
	m, _ := p.next("MH_D02_B03_V04", 5*time.Second)
	if r, err := (jjm.Decoder{Loc: ist}).Decode(m.Payload); err != nil || r.Error != "tamper" {
		t.Errorf("built message: %+v, %v", r, err)
	}
	_ = g.Publish(context.Background(), "jjm/MH_D02_B03_V04", `{"gid":"MH_D02_B03_V04","did":`)
	m, _ = p.next("MH_D02_B03_V04", 5*time.Second)
	if _, err := (jjm.Decoder{Loc: ist}).Decode(m.Payload); err == nil {
		t.Error("a malformed message decoded")
	}
}

// The library's state-to-centre client against the simulated central
// API: success, Status 0 and a retry, a refused key, the API down, an
// answer that is not JSON, an unknown method and a type that does not
// match its method.
func TestJJMCentral(t *testing.T) {
	cen := sim.NewCentral()
	defer cen.Close()
	c := jjm.Central{BaseURL: cen.URL + "/api", Loc: ist}
	ctx := context.Background()
	var p jjm.Payload
	_ = json.Unmarshal([]byte(sim.SampleFlowmeter), &p)

	r, err := c.Post(ctx, p)
	if err != nil || !r.OK() || r.DeviceID != "fm01" || r.UploadedOn == "" {
		t.Fatalf("post: %+v, %v", r, err)
	}
	if got := cen.Received(); len(got) != 1 || got[0].Path != "/api/FlowmeterDataInsert" || got[0].Body["did"] != "fm01" {
		t.Errorf("received %+v", got)
	}

	cen.FailNext(1)
	if _, err := c.Post(ctx, p); !jjm.ErrRetry(err) {
		t.Errorf("Status 0: %v", err)
	}
	if _, err := c.Post(ctx, p); err != nil {
		t.Errorf("the retry: %v", err)
	}

	cen.SetKey("demo-central-key")
	_, err = c.Post(ctx, p)
	var pe *jjm.PushError
	if !errors.As(err, &pe) || pe.HTTPStatus != http.StatusUnauthorized {
		t.Errorf("no key: %v", err)
	}
	c.Key = "demo-central-key"
	if _, err := c.Post(ctx, p); err != nil {
		t.Errorf("X-API-Key: %v", err)
	}
	c.Auth = "bearer"
	if _, err := c.Post(ctx, p); err != nil {
		t.Errorf("bearer: %v", err)
	}

	cen.SetDown(true)
	if _, err := c.Post(ctx, p); !errors.As(err, &pe) || pe.HTTPStatus != http.StatusServiceUnavailable || !pe.Retry {
		t.Errorf("down: %v", err)
	}
	cen.SetDown(false)
	cen.SetMalformed(true)
	if _, err := c.Post(ctx, p); !jjm.ErrRetry(err) {
		t.Errorf("malformed answer: %v", err)
	}
	cen.SetMalformed(false)

	code, out, _ := sim.Post(cen.URL+"/api/NoSuchMethod", "demo-central-key", sim.SampleFlowmeter)
	if code != http.StatusNotFound || out["Status"] != "0" {
		t.Errorf("unknown method: %d %v", code, out)
	}
	code, out, _ = sim.Post(cen.URL+"/api/PressureDataInsert", "demo-central-key", sim.SampleFlowmeter)
	if code != http.StatusBadRequest || out["Status"] != "0" {
		t.Errorf("type mismatch: %d %v", code, out)
	}
	code, _, _ = sim.Post(cen.URL+"/api/DataInsert", "demo-central-key", `{"gid":`)
	if code != http.StatusBadRequest {
		t.Errorf("malformed body: %d", code)
	}
	code, out, _ = sim.Post(cen.URL+"/api/DataInsert", "demo-central-key", sim.SamplePressure)
	if code != http.StatusOK || out["Status"] != "1" {
		t.Errorf("DataInsert takes any type: %d %v", code, out)
	}
	if cen.Refused() < 6 {
		t.Errorf("refused %d", cen.Refused())
	}
	cen.Close()
	if _, err := c.Post(ctx, p); !jjm.ErrRetry(err) {
		t.Errorf("closed: %v", err)
	}
}

// The library's eJalShakti client against the simulated portal: a body of
// every API written by the library is taken and read back; the XML form
// is taken; a wrong key is refused (Status true with the not-matched
// message) until the key is renewed; failures, an outage, an unknown API
// and a body the portal cannot read.
func TestJJMPortal(t *testing.T) {
	keys := map[string]string{}
	for _, a := range jjm.PortalAPIs {
		keys[a.Name] = "demo-key-" + a.Name
	}
	portal := sim.NewPortal(keys)
	defer portal.Close()
	c := jjm.EJClient{BaseURL: portal.URL + "/IoT_API/API", Keys: keys, AllowRetired: true}
	portal.AcceptRetired(true) // every API of either manual first
	ctx := context.Background()
	at := time.Date(2026, 10, 11, 6, 0, 0, 0, ist)
	for _, a := range jjm.PortalAPIs {
		var body map[string]any
		if a.Daily {
			body, _ = jjm.EJDailyBody(a, "2730100123", "GW-DEMO-01", "2026-10-10", 42, 56.5)
		} else {
			body, _ = jjm.EJSeriesBody(a, "2730100123", "GW-DEMO-01", ist, jjm.EJReading{Time: at, Value: 0.4}, jjm.EJReading{Time: at.Add(15 * time.Minute), Value: 0.5})
		}
		if _, err := c.Push(ctx, a.Name, body); err != nil {
			t.Errorf("%s: %v", a.Name, err)
		}
	}
	pushes := portal.Pushes()
	if len(pushes) != len(jjm.PortalAPIs) {
		t.Fatalf("%d pushes", len(pushes))
	}
	for i, ps := range pushes {
		want := 2
		if jjm.PortalAPIs[i].Daily {
			want = 1
		}
		if ps.API != jjm.PortalAPIs[i].Name || len(ps.Readings) != want {
			t.Errorf("push %d: %s with %d readings", i, ps.API, len(ps.Readings))
		}
	}
	for _, s := range []struct{ api, body, ct string }{
		{"Chlorine", sim.EjChlorine, ""}, {"PHLevel", sim.EjPH, ""}, {"FlowMeter", sim.EjFlowMeter, ""},
		{"GroundWater", sim.EjGroundWater, ""}, {"Pressure", sim.EjPressure, ""}, {"Chlorine", sim.EjChlorineXML, "application/xml"},
		{"PHLevel", sim.EjPHXML, "application/xml"},
	} {
		code, out, _ := sim.PostEj(portal.URL+"/IoT_API/API/"+s.api, keys[s.api], s.body, s.ct)
		if code != http.StatusOK || out["Message"] != jjm.EjalshaktiSuccess {
			t.Errorf("%s sample: %d %v", s.api, code, out)
		}
	}

	c.Keys = map[string]string{"Chlorine": "stale-key"}
	body, _ := jjm.EJSeriesBody(jjm.PortalAPIs[2], "2730100123", "GW-DEMO-01", ist, jjm.EJReading{Time: at, Value: 0.3})
	_, err := c.Push(ctx, "Chlorine", body)
	var kr jjm.ErrKeyRefused
	if !errors.As(err, &kr) || portal.KeyRefusals() != 1 {
		t.Errorf("stale key: %v", err)
	}
	portal.SetKey("Chlorine", "stale-key")
	if _, err := c.Push(ctx, "Chlorine", body); err != nil {
		t.Errorf("renewed key: %v", err)
	}

	portal.FailNext(1)
	if _, err := c.Push(ctx, "Chlorine", body); !jjm.ErrRetry(err) {
		t.Errorf("500: %v", err)
	}
	portal.SetDown(true)
	var pe *jjm.PushError
	if _, err := c.Push(ctx, "Chlorine", body); !errors.As(err, &pe) || pe.HTTPStatus != http.StatusServiceUnavailable {
		t.Errorf("down: %v", err)
	}
	portal.SetDown(false)
	code, _, _ := sim.PostEj(portal.URL+"/IoT_API/API/Weather", "k", `{}`, "")
	if code != http.StatusNotFound {
		t.Errorf("unknown API: %d", code)
	}
	_, err = c.Push(ctx, "Chlorine", map[string]any{"VillageId": "2730100123", "GatewayId": "GW-DEMO-01"})
	if !errors.As(err, &pe) || pe.HTTPStatus != http.StatusBadRequest || pe.Retry {
		t.Errorf("a body without a value: %v", err)
	}
	// As the current portal: the retired and unlisted APIs are gone.
	portal.AcceptRetired(false)
	for _, s := range []struct{ api, body string }{{"GroundWater", sim.EjGroundWater}, {"Pressure", sim.EjPressure}} {
		if code, _, _ := sim.PostEj(portal.URL+"/IoT_API/API/"+s.api, keys[s.api], s.body, ""); code != http.StatusNotFound {
			t.Errorf("%s on the current portal: %d", s.api, code)
		}
	}
	// The library's client sends XML too.
	c.XML, c.Keys = true, keys
	if _, err := c.Push(ctx, "PHLevel", map[string]any{"VillageId": "2730100123", "GatewayId": "GW-DEMO-01",
		"PhLevelData": []any{map[string]any{"TimeStamp": "2026-10-11T06:00:00.0000000+05:30", "PH_Level": 7.1}}}); err != nil {
		t.Errorf("XML push: %v", err)
	}
	if last := portal.Pushes(); last[len(last)-1].API != "PHLevel" || last[len(last)-1].Readings[0].P["ph"] != "7.1" {
		t.Errorf("XML push read as %+v", last[len(last)-1])
	}
}

// The vendor-style scheme RTU (a format no one publishes): readings, a
// pump command, local mode refusing it, a valve, a bad argument, an
// unknown command, silence, and a command queued while it was offline.
func TestJJMRTU(t *testing.T) {
	url := broker(t)
	p := newPlatform(t, url, "rtu/+/data", "rtu/+/ack")
	r, err := sim.DialRTUSession(ctx10(t), url, "rtu", "R7", "SCH-DEMO", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := r.Report(context.Background(), time.Now(), "RUN", 12.5); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.next("/data", 5*time.Second); !ok {
		t.Fatal("no reading")
	}
	ack := func(cmd string) map[string]any {
		t.Helper()
		p.publish("rtu/R7/cmd", []byte(cmd))
		m, ok := p.next("/ack", 3*time.Second)
		if !ok {
			return nil
		}
		var a map[string]any
		_ = json.Unmarshal(m.Payload, &a)
		return a
	}
	if a := ack(`{"id":"1","cmd":"MOTOR","arg":"ON"}`); a["ok"] != 1.0 || !r.Motor() {
		t.Errorf("motor on: %v", a)
	}
	r.Local(true)
	if a := ack(`{"id":"2","cmd":"MOTOR","arg":"OFF"}`); a["ok"] != 0.0 || a["err"] != "LOCAL" || !r.Motor() {
		t.Errorf("local mode: %v", a)
	}
	r.Local(false)
	if a := ack(`{"id":"3","cmd":"VALVE","valve":"V1","arg":"OPEN"}`); a["ok"] != 1.0 || !r.Valve("V1") {
		t.Errorf("valve: %v", a)
	}
	if a := ack(`{"id":"4","cmd":"RATE","arg":0}`); a["err"] != "BAD_ARG" {
		t.Errorf("bad rate: %v", a)
	}
	if a := ack(`{"id":"5","cmd":"DANCE"}`); a["err"] != "UNKNOWN" {
		t.Errorf("unknown: %v", a)
	}
	r.Silence(true)
	if a := ack(`{"id":"6","cmd":"MOTOR","arg":"OFF"}`); a != nil {
		t.Errorf("a silent RTU answered %v", a)
	}
	r.Silence(false)
	r.Offline()
	time.Sleep(200 * time.Millisecond)
	p.publish("rtu/R7/cmd", []byte(`{"id":"7","cmd":"MOTOR","arg":"OFF"}`))
	if err := r.Report(context.Background(), time.Now(), "STOP", 1); err == nil {
		t.Error("an offline RTU reported")
	}
	if err := r.Online(ctx10(t)); err != nil {
		t.Fatal(err)
	}
	if m, ok := p.next("/ack", 5*time.Second); !ok || r.Motor() {
		t.Errorf("queued command: %s, motor %v", m.Payload, r.Motor())
	}
}
