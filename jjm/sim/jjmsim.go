// Package sim holds the JJM Technical/Expert Committee report's sample
// messages and a simulated sensor gateway that sends them over MQTT or to
// the HTTP API, a simulated state-to-centre API that records what it
// receives, a simulated eJalShakti portal and a vendor-style scheme RTU,
// each with fault switches.
package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/cB-Sigma/indiagov/jjm"
	"github.com/cB-Sigma/indiagov/mqttconn"
)

// Device messages in the layout of the report's section 5(f) and
// annexure 9(c)(i), one per device type, with the empty p4 to p6 slots the
// layout shows. The gid, IDs, times and values are our own (not the
// report's printed samples); every value lies within the report's
// reference ranges, and ground water depth is in feet as the annexure
// sends it.
const (
	SampleFlowmeter   = `{"gid": "MH_D02_B03_V04", "did": "fm01", "dty": "fm", "dt": "2026-09-20T06:00:00", "p": {"t": "48250.5", "f": "212.4", "b": "87", "e": "00", "p5": "", "p6": ""}}`
	SampleGroundwater = `{"gid": "MH_D02_B03_V04", "did": "gl01", "dty": "gl", "dt": "2026-09-20T06:00:00", "p": {"g": "38.20", "b": "91", "e": "00", "p4": "", "p5": "", "p6": ""}}`
	SampleReservoir   = `{"gid": "MH_D02_B03_V04", "did": "sl01", "dty": "sl", "dt": "2026-09-20T06:00:00", "p": {"lp": "72", "lm": "3.15", "b": "88", "e": "00", "p5": "", "p6": ""}}`
	SampleChlorine    = `{"gid": "MH_D02_B03_V04", "did": "ca01", "dty": "ca", "dt": "2026-09-20T06:00:00", "p": {"c": "0.42", "b": "95", "e": "00", "p4": "", "p5": "", "p6": ""}}`
	SampleTurbidity   = `{"gid": "MH_D02_B03_V04", "did": "tbs01", "dty": "tbs", "dt": "2026-09-20T06:00:00", "p": {"tb": "1.8", "b": "93", "e": "00", "p4": "", "p5": "", "p6": ""}}`
	SamplePH          = `{"gid": "MH_D02_B03_V04", "did": "ph01", "dty": "ph", "dt": "2026-09-20T06:00:00", "p": {"ph": "7.35", "b": "94", "e": "00", "p4": "", "p5": "", "p6": ""}}`
	SamplePressure    = `{"gid": "MH_D02_B03_V04", "did": "ps01", "dty": "ps", "dt": "2026-09-20T06:00:00", "p": {"pr": "2.6", "b": "90", "e": "00", "p4": "", "p5": "", "p6": ""}}`
	SamplePump        = `{"gid": "MH_D02_B03_V04", "did": "pc01", "dty": "pc", "dt": "2026-09-20T06:00:00", "p": {"a": "18.4", "v": "412.5", "s": "1", "vb": "48.2", "e": "00", "p6": ""}}`
	// SampleFlowmeterSection5 is a flow meter reporting code 01, which
	// means leakage in section 5(f)'s table and tamper in the annexure's
	// (jjm.Decoder.FlowErrorCodes picks the table; the annexure's is the
	// default).
	SampleFlowmeterSection5 = `{"gid": "MH_D02_B03_V04", "did": "fm02", "dty": "fm", "dt": "2026-09-20T06:00:00", "p": {"t": "1200.0", "f": "0", "b": "80", "e": "01"}}`
	// SampleAPIResponse is an answer in the layout of annexure 9(c)(iii).
	SampleAPIResponse = `{"Status":"1","Message":"Data uploaded successfully.","DeviceID":"fm01","UploadedOn":"2026-10-10T06:00:05"}`
)

// Samples lists every device sample.
var Samples = []string{SampleFlowmeter, SampleGroundwater, SampleReservoir, SampleChlorine, SampleTurbidity, SamplePH, SamplePressure, SamplePump}

// Gateway sends messages over MQTT.
type Gateway struct{ c mqttconn.Conn }

// DialGateway connects to the broker at url.
func DialGateway(ctx context.Context, url, clientID string) (*Gateway, error) {
	c, err := DialMQTT(ctx, url, clientID, nil, func(mqttconn.Message) {}, mqttconn.Options{})
	if err != nil {
		return nil, err
	}
	return &Gateway{c: c}, nil
}

// Publish sends a message on topic (any bytes: a malformed message too).
func (g *Gateway) Publish(ctx context.Context, topic, msg string) error {
	return g.c.Publish(ctx, topic, []byte(msg), false)
}

// PublishPayload sends a message built with jjm.NewPayload.
func (g *Gateway) PublishPayload(ctx context.Context, topic string, p jjm.Payload) error {
	b, err := p.JSON()
	if err != nil {
		return err
	}
	return g.c.Publish(ctx, topic, b, false)
}

// Close disconnects.
func (g *Gateway) Close() { g.c.Close() }

// Post sends a message to an HTTP API method URL with an API key; it
// returns the HTTP status and the decoded answer.
func Post(url, apiKey, msg string) (int, map[string]any, error) {
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader([]byte(msg)))
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("X-API-Key", apiKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out, nil
}

// Central is a simulated state-to-centre API (annexure 9(c)(iii)): POST
// {URL}/{Name}DataInsert with a device message, answered with Status,
// Message, DeviceID and UploadedOn. Faults: FailNext (Status "0"),
// SetKey (a key in X-API-Key or a bearer token is required: 401
// otherwise), SetDown (503), SetMalformed (an answer that is not JSON),
// SetDelay. A method that is not {Name}DataInsert is answered 404; a body
// that is not a device message of the method's type, 400 with Status "0".
type Central struct {
	*httptest.Server
	mu       sync.Mutex
	received []Received
	// fail makes the next n requests answer Status "0".
	fail      int
	key       string
	down      bool
	malformed bool
	delay     time.Duration
	refused   int
}

// Received is one request the central API took.
type Received struct {
	Path   string
	Header http.Header
	Body   map[string]any
}

// NewCentral starts a central API answering with the report's output.
func NewCentral() *Central {
	c := &Central{}
	c.Server = httptest.NewServer(http.HandlerFunc(c.serve))
	return c
}

func (c *Central) serve(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	c.mu.Lock()
	key, down, malformed, delay := c.key, c.down, c.malformed, c.delay
	c.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	reply := func(code int, status, msg, did string) {
		c.mu.Lock()
		if code != http.StatusOK || status != "1" {
			c.refused++
		}
		c.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		if malformed {
			_, _ = io.WriteString(w, `{"Status":`)
			return
		}
		out := map[string]any{"Status": status, "Message": msg, "DeviceID": did}
		if status == "1" {
			out["UploadedOn"] = time.Now().In(time.FixedZone("IST", 19800)).Format("2006-01-02T15:04:05")
		}
		_ = json.NewEncoder(w).Encode(out)
	}
	if down {
		reply(http.StatusServiceUnavailable, "0", "Service unavailable.", "")
		return
	}
	if key != "" && r.Header.Get("X-API-Key") != key && r.Header.Get("Authorization") != "Bearer "+key {
		reply(http.StatusUnauthorized, "0", "Invalid API key.", "")
		return
	}
	method := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
	want, ok := jjm.MethodDTY(method)
	if r.Method != http.MethodPost || !ok {
		reply(http.StatusNotFound, "0", "Unknown method; use {Name}DataInsert.", "")
		return
	}
	var body map[string]any
	var p jjm.Payload
	if json.Unmarshal(b, &body) != nil || json.Unmarshal(b, &p) != nil || p.GID == "" || p.DID == "" {
		reply(http.StatusBadRequest, "0", "Invalid data.", "")
		return
	}
	if want != "" && !strings.EqualFold(p.DTY, want) {
		reply(http.StatusBadRequest, "0", "The device type does not match the method.", p.DID)
		return
	}
	c.mu.Lock()
	fail := c.fail > 0
	if fail {
		c.fail--
	} else {
		c.received = append(c.received, Received{Path: r.URL.Path, Header: r.Header.Clone(), Body: body})
	}
	c.mu.Unlock()
	if fail {
		reply(http.StatusOK, "0", "Try later.", p.DID)
		return
	}
	reply(http.StatusOK, "1", jjm.CentralSuccess, p.DID)
}

// FailNext makes the next n requests fail.
func (c *Central) FailNext(n int) {
	c.mu.Lock()
	c.fail = n
	c.mu.Unlock()
}

// SetKey requires key in X-API-Key or as a bearer token ("": none).
func (c *Central) SetKey(key string) { c.mu.Lock(); c.key = key; c.mu.Unlock() }

// SetDown answers every request 503 (true) or serves again (false).
func (c *Central) SetDown(on bool) { c.mu.Lock(); c.down = on; c.mu.Unlock() }

// SetMalformed answers with a body that is not JSON.
func (c *Central) SetMalformed(on bool) { c.mu.Lock(); c.malformed = on; c.mu.Unlock() }

// SetDelay delays every answer by d.
func (c *Central) SetDelay(d time.Duration) { c.mu.Lock(); c.delay = d; c.mu.Unlock() }

// Refused counts the requests answered with a failure.
func (c *Central) Refused() int { c.mu.Lock(); defer c.mu.Unlock(); return c.refused }

// Received returns what the API took.
func (c *Central) Received() []Received {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Received(nil), c.received...)
}
