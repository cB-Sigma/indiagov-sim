package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cB-Sigma/indiagov/mqttconn"
	"github.com/cB-Sigma/indiagov/pmkusum"
)

// RMS is a simulated Remote Monitoring System.
type RMS struct {
	IMEI     string
	Solution string
	Prefix   string
	// PubSub makes the RMS use the topic form some field firmware uses:
	// a fifth level "sub" on the topics it subscribes to and "pub" on
	// those it publishes to. Set it before Online (DialOptions).
	PubSub bool

	// c is the connection, guarded by mu; up is closed once c is set by
	// the current Online, so an answer to a command that arrives while
	// the dial is still returning waits for it.
	c  mqttconn.Conn
	up chan struct{}
	// url and session are kept for Online (DialSession).
	url     string
	session time.Duration

	mu       sync.Mutex
	potp     string
	cotp     string
	info     []byte
	commands []map[string]any
	// state holds the keyword values the RMS reports on a read and
	// changes on a write (annexure 7 keywords and any other).
	state map[string]any
	// noAnswer makes the RMS ignore commands (timeouts).
	noAnswer bool
	// held are keywords a write does not change (the pump in local mode,
	// or held off by a dry-run cut-off): the answer carries the value
	// they keep.
	held map[string]bool
	// Faults (SetWrongOTP, SetAnswerDelay, SetDuplicateAnswers,
	// SetMalformedAnswers, DropNext).
	wrongOTP  bool
	delay     time.Duration
	duplicate bool
	malformed bool
	drop      int
	// stored are records kept for retrieval, by VD/DATE/INDEX.
	stored map[string][]byte
}

// Hold makes writes of keyword k leave it unchanged (true), as a
// controller in local mode or after a protection trip does, or lets them
// through again (false).
func (r *RMS) Hold(k string, on bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.held == nil {
		r.held = map[string]bool{}
	}
	r.held[k] = on
}

// Set gives a keyword a value the RMS reports (and knows).
func (r *RMS) Set(k string, v any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state[k] = v
}

// Get returns a keyword's value.
func (r *RMS) Get(k string) any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state[k]
}

// Silence makes the RMS ignore (true) or answer (false) commands.
func (r *RMS) Silence(on bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.noAnswer = on
}

// SetWrongOTP makes the RMS's messages carry an OTP pair the platform
// never issued (true), as an RMS with a stale or forged OTP would.
func (r *RMS) SetWrongOTP(on bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.wrongOTP = on
}

// SetAnswerDelay delays every answer by d (a slow GSM link).
func (r *RMS) SetAnswerDelay(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.delay = d
}

// SetDuplicateAnswers makes the RMS send every answer twice (a resend
// after a lost acknowledgement).
func (r *RMS) SetDuplicateAnswers(on bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.duplicate = on
}

// SetMalformedAnswers makes the RMS answer with bytes that are not JSON.
func (r *RMS) SetMalformedAnswers(on bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.malformed = on
}

// DropNext makes the RMS lose the next n commands (received, recorded,
// never answered); later ones are answered, so a platform's retry works.
func (r *RMS) DropNext(n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.drop = n
}

// Store keeps a record for retrieval: an on-demand write of VD, DATE
// (YYMMDD), INDEX and LOAD 1 (pmkusum.Retrieve) for it is answered with
// LOAD 1 and the record is sent again on the data topic; for a record not
// stored, LOAD comes back 0.
func (r *RMS) Store(vd int, date string, index int, payload []byte) error {
	ds, err := pmkusum.YYMMDD(date)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stored == nil {
		r.stored = map[string][]byte{}
	}
	r.stored[storeKey(strconv.Itoa(vd), ds, strconv.Itoa(index))] = payload
	return nil
}

func storeKey(vd, date, index string) string { return vd + "/" + date + "/" + index }

// Dial connects an RMS to the broker at url as client id "d:<imei>".
func Dial(ctx context.Context, url, imei, solution string) (*RMS, error) {
	return DialSession(ctx, url, imei, solution, 0)
}

// DialSession is Dial for an RMS that keeps a persistent broker session
// for session (0: none), as GSM controllers that drop off the network do:
// QoS 1 commands published while it is offline reach it on Online unless
// they expired.
func DialSession(ctx context.Context, url, imei, solution string, session time.Duration) (*RMS, error) {
	return DialOptions(ctx, url, imei, solution, session, false)
}

// DialOptions is DialSession with the field firmware topic form (pubSub,
// see RMS.PubSub).
func DialOptions(ctx context.Context, url, imei, solution string, session time.Duration, pubSub bool) (*RMS, error) {
	r := &RMS{IMEI: imei, Solution: solution, Prefix: "IIOT-1", PubSub: pubSub, url: url, session: session, state: map[string]any{
		"APN1": "www", "UPDATEINTERVAL": 15, "HEARTINTERVAL": 5, "DO1": 0, "RESTART": 0, "UPDATERTC": 0,
	}}
	if err := r.Online(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

// Online connects the RMS (again, after Offline), resuming its session.
func (r *RMS) Online(ctx context.Context) error {
	var filters []string
	for _, t := range pmkusum.RMSSubscribes {
		filters = append(filters, r.subTopic(t))
	}
	up := make(chan struct{})
	r.mu.Lock()
	r.up = up
	r.mu.Unlock()
	c, err := dialSubscribed(ctx, r.url, "d:"+r.IMEI, filters, r.handle, mqttconn.Options{NoLocal: true, SessionExpiry: r.session})
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.c = c
	r.mu.Unlock()
	close(up)
	return nil
}

// conn is the connection, once the current Online has set it (nil if it
// does not within 10 s).
func (r *RMS) conn() mqttconn.Conn {
	r.mu.Lock()
	up := r.up
	r.mu.Unlock()
	if up != nil {
		select {
		case <-up:
		case <-time.After(10 * time.Second):
			return nil
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.c
}

// Offline disconnects the RMS; its session stays with the broker when it
// has one (DialSession).
func (r *RMS) Offline() {
	if c := r.conn(); c != nil {
		c.Close()
	}
}

// Close disconnects.
func (r *RMS) Close() {
	if c := r.conn(); c != nil {
		c.Close()
	}
}

func (r *RMS) base(t string) string { return pmkusum.Topic(r.Prefix, r.Solution, r.IMEI, t) }

// topic is where the RMS publishes a message type.
func (r *RMS) topic(t string) string {
	if r.PubSub {
		return r.base(t) + "/pub"
	}
	return r.base(t)
}

// subTopic is where the RMS receives a message type.
func (r *RMS) subTopic(t string) string {
	if r.PubSub {
		return r.base(t) + "/sub"
	}
	return r.base(t)
}

func (r *RMS) handle(m mqttconn.Message) {
	var v map[string]any
	d := json.NewDecoder(bytes.NewReader(m.Payload))
	d.UseNumber()
	if d.Decode(&v) != nil {
		return
	}
	rt, _ := pmkusum.ParseTopic(m.Topic)
	switch rt.Type {
	case pmkusum.TypeOTP:
		if o, err := pmkusum.DecodeOTP(m.Payload, time.UTC); err == nil {
			r.mu.Lock()
			r.potp, r.cotp = o.POTP, o.COTP
			r.mu.Unlock()
		}
	case pmkusum.TypeInfo:
		r.mu.Lock()
		r.info = m.Payload
		r.mu.Unlock()
	default:
		r.mu.Lock()
		r.commands = append(r.commands, v)
		skip := r.noAnswer
		if !skip && r.drop > 0 {
			r.drop--
			skip = true
		}
		r.mu.Unlock()
		if !skip {
			// The answer goes back on the topic type the command came on.
			t := rt.Type
			if t == "" || rt.Direction != "" {
				t = pmkusum.TypeOnDemand
				if strings.Contains(strings.ToLower(m.Topic), "/config") {
					t = pmkusum.TypeConfig
				}
			}
			topic := m.Topic
			if r.PubSub {
				topic = r.topic(t)
			}
			go r.answer(topic, v)
		}
	}
}

// answer replies to an annexure 7 command on topic.
func (r *RMS) answer(topic string, cmd map[string]any) {
	out := map[string]any{}
	verb, _ := cmd["cmd"].(string)
	write := strings.EqualFold(verb, "write")
	r.mu.Lock()
	for k, v := range cmd {
		switch k {
		case "timestamp", "type", "cmd", "msgid", "POTP", "COTP":
			out[k] = v // handshake keywords go back unchanged
			continue
		}
		cur, known := r.state[k]
		switch {
		case !known:
			out[k] = 0
		case write && r.held[k]:
			out[k] = cur
		case write:
			r.state[k] = v
			out[k] = v
		default:
			out[k] = cur
		}
	}
	// A retrieval (pmkusum.Retrieve): LOAD 1 with VD, DATE and INDEX.
	var replay []byte
	if write && fmt.Sprint(cmd["LOAD"]) == "1" {
		for _, k := range []string{"VD", "DATE", "INDEX", "LOAD"} {
			out[k] = cmd[k]
		}
		if rec, ok := r.stored[storeKey(fmt.Sprint(cmd["VD"]), fmt.Sprint(cmd["DATE"]), fmt.Sprint(cmd["INDEX"]))]; ok {
			replay = rec
		} else {
			out["LOAD"] = 0
		}
	}
	delay, dup, bad := r.delay, r.duplicate, r.malformed
	r.mu.Unlock()
	b, _ := json.Marshal(out)
	if bad {
		b = []byte(`{"msgid":` + fmt.Sprint(cmd["msgid"]) + `,`)
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	c := r.conn()
	if c == nil {
		return
	}
	_ = c.Publish(context.Background(), topic, b, false)
	if dup {
		_ = c.Publish(context.Background(), topic, b, false)
	}
	if replay != nil {
		_ = c.Publish(context.Background(), r.topic(pmkusum.TypeData), replay, false)
	}
}

// OTP returns the last OTP pair received.
func (r *RMS) OTP() (potp, cotp string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.potp, r.cotp
}

// Info returns the last info message received.
func (r *RMS) Info() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.info
}

// Commands returns the commands received.
func (r *RMS) Commands() []map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]map[string]any(nil), r.commands...)
}

// otpPair is the pair messages carry: the last received, or a wrong one
// (SetWrongOTP); ok is false when none was received.
func (r *RMS) otpPair() (p, c string, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.wrongOTP {
		return "00000000", "99999999", true
	}
	return r.potp, r.cotp, r.cotp != ""
}

// Publish sends a message on one of the RMS's topics (heartbeat, data)
// after replacing IMEI with the RMS's own and POTP/COTP with the last OTP
// received (when one was), and applying set.
func (r *RMS) Publish(ctx context.Context, msgType, sample string, set map[string]any) error {
	var v map[string]any
	d := json.NewDecoder(strings.NewReader(sample))
	d.UseNumber()
	if err := d.Decode(&v); err != nil {
		return fmt.Errorf("sample: %w", err)
	}
	// Keywords keep the sample's letter case (some firmware sends lower
	// case).
	key := func(k string) string {
		for x := range v {
			if strings.EqualFold(x, k) {
				return x
			}
		}
		return k
	}
	if k := key("IMEI"); v[k] != nil {
		v[k] = r.IMEI
	}
	if p, c, ok := r.otpPair(); ok {
		v[key("POTP")], v[key("COTP")] = p, c
	}
	for k, x := range set {
		if x == nil {
			delete(v, k)
			continue
		}
		v[k] = x
	}
	b, _ := json.Marshal(v)
	return r.PublishRaw(ctx, msgType, b)
}

// PublishMessage writes a message with pmkusum.Encode (the header's IMEI,
// POTP and COTP filled in from the RMS when empty) and sends it.
func (r *RMS) PublishMessage(ctx context.Context, msgType string, h pmkusum.Header, loc *time.Location, keywords map[string]any) error {
	if h.IMEI == "" {
		h.IMEI = r.IMEI
	}
	if h.COTP == "" {
		h.POTP, h.COTP, _ = r.otpPair()
	}
	b, err := pmkusum.Encode(h, loc, keywords)
	if err != nil {
		return err
	}
	return r.PublishRaw(ctx, msgType, b)
}

// PublishRaw sends bytes on one of the RMS's topics (a malformed message,
// for instance).
func (r *RMS) PublishRaw(ctx context.Context, msgType string, b []byte) error {
	c := r.conn()
	if c == nil {
		return fmt.Errorf("pmkusum sim: RMS %s is not connected", r.IMEI)
	}
	return c.Publish(ctx, r.topic(msgType), b, false)
}

// Now formats a time as an RMS TIMESTAMP in IST.
func Now(t time.Time) string {
	return t.In(time.FixedZone("IST", 5*3600+1800)).Format("2006-01-02 15:04:05")
}
