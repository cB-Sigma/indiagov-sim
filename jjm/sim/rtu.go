package sim

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cB-Sigma/indiagov/mqttconn"
)

// RTU is a scheme controller in a format no one publishes (F-488): it has
// the shape vendor RTUs commonly have, not any vendor's real format. It
// reports the pump motor and a valve with text states and an epoch time,
// and takes commands with an ID, answering on its own ack topic; in local
// mode (the panel's selector switch) it refuses remote pump commands.
//
//	{prefix}/{id}/data  {"scheme", "ts", "motor": "RUN"|"STOP"|"TRIP", "amp", "volt", "valve": "OPEN"|"CLOSED", "flow_lpm", "total_kl"}
//	{prefix}/{id}/cmd   {"id", "cmd": "MOTOR"|"VALVE"|"RATE", "arg"}            (to the RTU)
//	{prefix}/{id}/ack   {"id", "ok": 1|0, "err"}                                 (from the RTU)
type RTU struct {
	ID, Scheme, Prefix string

	// c is the RTU's connection: set when it comes up, which with a
	// persistent session can be after queued commands are already being
	// handled (handle waits for it).
	c atomic.Pointer[connRef]
	// url and session are kept for Online (DialRTUSession).
	url     string
	session time.Duration

	mu       sync.Mutex
	motor    bool
	valve    map[string]bool
	rate     float64
	local    bool
	silent   bool
	commands []map[string]any
}

// DialRTU connects an RTU.
func DialRTU(ctx context.Context, url, prefix, id, scheme string) (*RTU, error) {
	return DialRTUSession(ctx, url, prefix, id, scheme, 0)
}

// DialRTUSession is DialRTU for an RTU that keeps a persistent broker
// session for session (0: none), as GSM RTUs that drop off the network
// do: QoS 1 commands published while it is offline reach it on Online
// unless they expired.
func DialRTUSession(ctx context.Context, url, prefix, id, scheme string, session time.Duration) (*RTU, error) {
	r := &RTU{ID: id, Scheme: scheme, Prefix: prefix, valve: map[string]bool{}, rate: 15, url: url, session: session}
	if err := r.Online(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

// Online connects the RTU (again, after Offline), resuming its session.
func (r *RTU) Online(ctx context.Context) error {
	c, err := dialSubscribed(ctx, r.url, "rtu-"+r.ID, []string{r.Prefix + "/" + r.ID + "/cmd"},
		r.handle, mqttconn.Options{NoLocal: true, SessionExpiry: r.session,
			OnUp: func(c mqttconn.Conn) { r.c.Store(&connRef{c}) }})
	if err != nil {
		return err
	}
	r.c.Store(&connRef{c})
	return nil
}

// Offline disconnects the RTU; its session stays with the broker when it
// has one (DialRTUSession).
func (r *RTU) Offline() {
	// Commands queued meanwhile wait for the next connection (handle).
	if c := r.c.Swap(nil); c != nil {
		c.Close()
	}
}

// Close disconnects.
func (r *RTU) Close() {
	if c := r.c.Swap(nil); c != nil {
		c.Close()
	}
}

// Local sets local mode.
func (r *RTU) Local(on bool) { r.mu.Lock(); r.local = on; r.mu.Unlock() }

// Silence makes the RTU ignore commands.
func (r *RTU) Silence(on bool) { r.mu.Lock(); r.silent = on; r.mu.Unlock() }

// Motor reports whether the pump runs.
func (r *RTU) Motor() bool { r.mu.Lock(); defer r.mu.Unlock(); return r.motor }

// Valve reports whether a valve is open.
func (r *RTU) Valve(id string) bool { r.mu.Lock(); defer r.mu.Unlock(); return r.valve[id] }

// Rate is the reporting interval in minutes.
func (r *RTU) Rate() float64 { r.mu.Lock(); defer r.mu.Unlock(); return r.rate }

// Commands returns the commands received.
func (r *RTU) Commands() []map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]map[string]any(nil), r.commands...)
}

// Report publishes one reading at t.
func (r *RTU) Report(ctx context.Context, t time.Time, motor string, totalKL float64) error {
	b, _ := json.Marshal(map[string]any{"scheme": r.Scheme, "ts": t.Unix(), "motor": motor, "amp": 12.4, "volt": 412.0,
		"valve": "OPEN", "flow_lpm": 180.0, "total_kl": totalKL})
	c := r.c.Load()
	if c == nil {
		return errors.New("jjmsim: the RTU is offline")
	}
	return c.Publish(ctx, r.Prefix+"/"+r.ID+"/data", b, false)
}

func (r *RTU) handle(m mqttconn.Message) {
	var cmd map[string]any
	if json.Unmarshal(m.Payload, &cmd) != nil {
		return
	}
	r.mu.Lock()
	r.commands = append(r.commands, cmd)
	ans := map[string]any{"id": cmd["id"], "ok": 1}
	arg, _ := cmd["arg"].(string)
	switch cmd["cmd"] {
	case "MOTOR":
		if r.local {
			ans["ok"], ans["err"] = 0, "LOCAL"
		} else {
			r.motor = arg == "ON"
		}
	case "VALVE":
		v, _ := cmd["valve"].(string)
		r.valve[v] = arg == "OPEN"
	case "RATE":
		n, ok := cmd["arg"].(float64)
		if !ok || n < 1 {
			ans["ok"], ans["err"] = 0, "BAD_ARG"
		} else {
			r.rate = n
		}
	default:
		ans["ok"], ans["err"] = 0, "UNKNOWN"
	}
	silent := r.silent
	r.mu.Unlock()
	if silent {
		return
	}
	b, _ := json.Marshal(ans)
	c := r.c.Load()
	for i := 0; c == nil && i < 500; i++ {
		// A queued command delivered before Online has the connection.
		time.Sleep(10 * time.Millisecond)
		c = r.c.Load()
	}
	if c != nil {
		_ = c.Publish(context.Background(), r.Prefix+"/"+r.ID+"/ack", b, false)
	}
}
