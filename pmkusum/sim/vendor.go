package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/cB-Sigma/indiagov/mqttconn"
)

// VendorRMS is an RMS of a format no agency publishes, for the mapping
// profile: it is not any vendor's real format (none is public, F-2707) but
// has the shape vendor RMS units commonly have: telemetry with nested
// objects, an epoch-millisecond time, text states, a batch of stored
// records, and commands answered on a separate topic with the command's ID
// and a result word.
//
//	{prefix}/{id}/telemetry  {"deviceId", "ts", "pump": {"state": "ON"|"OFF"|"DRY_RUN", "lpm", "kwhToday", "kwhTotal",
//	                          "litresToday", "dcV", "dcA"}, "gps": {"lat", "lon"}}
//	{prefix}/{id}/backlog    {"deviceId", "records": [ … telemetry objects … ]}
//	{prefix}/{id}/cmd        {"id", "op": "PUMP"|"RATE"|"REBOOT", "value"}   (to the RMS)
//	{prefix}/{id}/ack        {"id", "result": "OK"|"FAIL", "reason"}        (from the RMS)
type VendorRMS struct {
	ID     string
	Prefix string

	// c is set once the dial returns; up is closed then, so an answer to
	// a command delivered during the dial waits for it.
	c  mqttconn.Conn
	up chan struct{}

	mu       sync.Mutex
	running  bool
	rate     int
	local    bool // local mode: remote pump commands refused
	silent   bool
	commands []map[string]any
}

// DialVendor connects a vendor-format RMS.
func DialVendor(ctx context.Context, url, prefix, id string) (*VendorRMS, error) {
	v := &VendorRMS{ID: id, Prefix: prefix, rate: 15, up: make(chan struct{})}
	c, err := dialSubscribed(ctx, url, "v-"+id, []string{prefix + "/" + id + "/cmd"}, v.handle, mqttconn.Options{NoLocal: true})
	if err != nil {
		return nil, err
	}
	v.c = c
	close(v.up)
	return v, nil
}

// Close disconnects.
func (v *VendorRMS) Close() { v.c.Close() }

// Local puts the controller in local mode (true): remote pump commands
// are answered FAIL.
func (v *VendorRMS) Local(on bool) { v.mu.Lock(); v.local = on; v.mu.Unlock() }

// Silence makes the RMS ignore commands.
func (v *VendorRMS) Silence(on bool) { v.mu.Lock(); v.silent = on; v.mu.Unlock() }

// Running reports the pump state.
func (v *VendorRMS) Running() bool { v.mu.Lock(); defer v.mu.Unlock(); return v.running }

// Rate is the reporting interval in minutes.
func (v *VendorRMS) Rate() int { v.mu.Lock(); defer v.mu.Unlock(); return v.rate }

// Commands returns the commands received.
func (v *VendorRMS) Commands() []map[string]any {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]map[string]any(nil), v.commands...)
}

// Telemetry is one telemetry object at t with the pump's state word.
func (v *VendorRMS) Telemetry(t time.Time, state string) map[string]any {
	return map[string]any{"deviceId": v.ID, "ts": t.UnixMilli(),
		"pump": map[string]any{"state": state, "lpm": "41.5", "kwhToday": 6.2, "kwhTotal": 812.4, "litresToday": 15400, "dcV": 540.0, "dcA": 7.5},
		"gps":  map[string]any{"lat": 26.9124, "lon": 75.7873}}
}

// Publish sends telemetry.
func (v *VendorRMS) Publish(ctx context.Context, t time.Time, state string) error {
	b, _ := json.Marshal(v.Telemetry(t, state))
	return v.c.Publish(ctx, v.Prefix+"/"+v.ID+"/telemetry", b, false)
}

// PublishBacklog sends stored records in one message.
func (v *VendorRMS) PublishBacklog(ctx context.Context, recs ...map[string]any) error {
	l := make([]any, len(recs))
	for i, r := range recs {
		l[i] = r
	}
	b, _ := json.Marshal(map[string]any{"deviceId": v.ID, "records": l})
	return v.c.Publish(ctx, v.Prefix+"/"+v.ID+"/backlog", b, false)
}

func (v *VendorRMS) handle(m mqttconn.Message) {
	var cmd map[string]any
	if json.Unmarshal(m.Payload, &cmd) != nil {
		return
	}
	v.mu.Lock()
	v.commands = append(v.commands, cmd)
	silent := v.silent
	ans := map[string]any{"id": cmd["id"], "result": "OK"}
	switch cmd["op"] {
	case "PUMP":
		if v.local {
			ans["result"], ans["reason"] = "FAIL", "LOCAL_MODE"
		} else {
			v.running = cmd["value"] == "ON"
		}
	case "RATE":
		n, ok := cmd["value"].(float64)
		if !ok || n < 1 || n > 60 {
			ans["result"], ans["reason"] = "FAIL", fmt.Sprint("BAD_VALUE ", cmd["value"])
		} else {
			v.rate = int(n)
		}
	case "REBOOT":
	default:
		ans["result"], ans["reason"] = "FAIL", "UNKNOWN_OP"
	}
	v.mu.Unlock()
	if silent {
		return
	}
	b, _ := json.Marshal(ans)
	select {
	case <-v.up: // v.c is set: answer at once, in order
		_ = v.c.Publish(context.Background(), v.Prefix+"/"+v.ID+"/ack", b, false)
	default: // during the dial
		go func() {
			<-v.up
			_ = v.c.Publish(context.Background(), v.Prefix+"/"+v.ID+"/ack", b, false)
		}()
	}
}
