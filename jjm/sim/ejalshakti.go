package sim

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/cB-Sigma/indiagov/jjm"
)

// The ejalshakti IoT push API (DDWS/NIC, help pages at
// https://ejalshakti.gov.in/IoT_API/Help and the February 2026 operational
// manual). The bodies below have the members and shapes of the help pages'
// samples, with IDs and values of our own.
const (
	// EjChlorine is a Chlorine body with one top-level reading and a list
	// of two (TimeStamp with fractional seconds and +05:30, as the help
	// page prints it).
	EjChlorine = `{"VillageId":"2730100123","GatewayId":"GW-KHED-01","TimeStamp":"2026-10-05T10:00:00.1234567+05:30","ResidualCL":0.4,
"ChlorineData":[{"TimeStamp":"2026-10-05T10:15:00.0000000+05:30","ResidualCL":0.5},{"TimeStamp":"2026-10-05T10:30:00.0000000+05:30","ResidualCL":0.1}]}`
	// EjPH is a PHLevel body whose list member is PhLevelData and value
	// member PH_Level.
	EjPH = `{"VillageId":"2730100123","GatewayId":"GW-KHED-01","PhLevelData":[{"TimeStamp":"2026-10-05T10:00:00+05:30","PH_Level":7.2}]}`
	// EjFlowMeter is the daily FlowMeter body (Date as YYYY-MM-DD).
	EjFlowMeter = `{"VillageId":"2730100123","GatewayId":"GW-KHED-01","WaterSupply":42,"LPCD":56.5,"Date":"2026-10-04"}`
	// EjGroundWater is the daily body of the GroundWater API of the 2021
	// manual, which the current help pages no longer list.
	EjGroundWater = `{"VillageId":"2730100123","GatewayId":"GW-KHED-02","Depth":12,"Date":"2026-10-04"}`
	// EjPressure is a body of the Pressure API of the 2021 manual (no
	// longer listed).
	EjPressure = `{"VillageId":"2730100123","GatewayId":"GW-KHED-02","PressureSensorData":[{"TimeStamp":"2026-10-05T10:00:00+05:30","Pressure":2.4}]}`
	// EjPHXML is a PHLevel body as DataContract XML, whose collection
	// entries are PhLevelinputData elements.
	EjPHXML = `<PhLevelinput xmlns:i="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://schemas.datacontract.org/2004/07/IoT_API.Models">
<GatewayId>GW-KHED-01</GatewayId><PhLevelData><PhLevelinputData><PH_Level>7.2</PH_Level><TimeStamp>2026-10-05T10:00:00+05:30</TimeStamp></PhLevelinputData>
<PhLevelinputData><PH_Level>7.4</PH_Level><TimeStamp>2026-10-05T10:15:00+05:30</TimeStamp></PhLevelinputData></PhLevelData><VillageId>2730100123</VillageId></PhLevelinput>`
	// EjChlorineXML is the Chlorine body as DataContract XML (elements in
	// alphabetical order, the list wrapped in an element of its own name).
	EjChlorineXML = `<Chlorineinput xmlns:i="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://schemas.datacontract.org/2004/07/IoT_API.Models">
<ChlorineData><ChlorineData><ResidualCL>0.3</ResidualCL><TimeStamp>2026-10-05T11:00:00+05:30</TimeStamp></ChlorineData></ChlorineData>
<GatewayId>GW-KHED-01</GatewayId><VillageId>2730100123</VillageId></Chlorineinput>`
)

// PostEj sends a body to an ejalshakti-style API with the key in
// Authorization as it is (the portal's way); contentType defaults to JSON.
func PostEj(url, key, body, contentType string) (int, map[string]any, error) {
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader([]byte(body)))
	if contentType == "" {
		contentType = "application/json"
	}
	req.Header.Set("Content-Type", contentType)
	if key != "" {
		req.Header.Set("Authorization", key)
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

// Portal is a simulated ejalshakti IoT API: POST /IoT_API/API/{API} with
// the API's key in Authorization. It answers as the manual's screenshots
// show: HTTP 200 with {"Status": true, "Message": "sucess"}, and for a key
// that does not match also HTTP 200 and Status true, with Message
// "KeyToken not Matched". Bodies are read as the portal reads them (JSON
// or DataContract XML, jjm.EJPayloads); one it cannot take is answered
// 400. An API that does not exist, or one the help pages no longer list
// (unless AcceptRetired), is answered 404. Faults: FailNext (500),
// SetDown (503), SetDelay.
type Portal struct {
	*httptest.Server
	mu    sync.Mutex
	keys  map[string]string // API → key
	got   []PortalPush
	fail  int
	down  bool
	delay time.Duration
	// refusedKeys counts key refusals.
	refusedKeys int
	// retired makes the portal take the APIs the help pages no longer
	// list (AcceptRetired).
	retired bool
}

// AcceptRetired makes the portal take the retired GroundWater and
// Pressure APIs and the unlisted water quality APIs (true), as the 2021
// portal did; by default they are answered 404, as the current help pages
// list only FlowMeter, Chlorine, PHLevel, TurbidityLevel and LevelSensor.
func (p *Portal) AcceptRetired(on bool) { p.mu.Lock(); p.retired = on; p.mu.Unlock() }

// PortalPush is one accepted push.
type PortalPush struct {
	API string
	// Body is the JSON body (nil for an XML one).
	Body map[string]any
	// Readings are the body read as device messages.
	Readings []jjm.Payload
}

// NewPortal starts a portal with a key per API.
func NewPortal(keys map[string]string) *Portal {
	p := &Portal{keys: map[string]string{}}
	for k, v := range keys {
		p.keys[k] = v
	}
	p.Server = httptest.NewServer(http.HandlerFunc(p.serve))
	return p
}

func (p *Portal) serve(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/IoT_API/API/")
	b, _ := io.ReadAll(r.Body)
	w.Header().Set("Content-Type", "application/json")
	reply := func(code int, v map[string]any) {
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	}
	p.mu.Lock()
	down, delay := p.down, p.delay
	fail := !down && p.fail > 0
	if fail {
		p.fail--
	}
	p.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	switch {
	case down:
		reply(http.StatusServiceUnavailable, map[string]any{"Message": "Service Unavailable"})
		return
	case fail:
		reply(http.StatusInternalServerError, map[string]any{"Message": "An error has occurred."})
		return
	}
	api, ok := jjm.EjalshaktiAPI(name)
	p.mu.Lock()
	retiredOK := p.retired
	p.mu.Unlock()
	if !ok || r.Method != http.MethodPost || (!api.Current() && !retiredOK) {
		reply(http.StatusNotFound, map[string]any{"Message": "No HTTP resource was found that matches the request URI."})
		return
	}
	p.mu.Lock()
	want := ""
	for k, v := range p.keys {
		if strings.EqualFold(k, api.Name) {
			want = v
		}
	}
	p.mu.Unlock()
	if want == "" || r.Header.Get("Authorization") != want {
		p.mu.Lock()
		p.refusedKeys++
		p.mu.Unlock()
		reply(http.StatusOK, map[string]any{"Status": true, "Message": jjm.EJKeyNotMatched})
		return
	}
	readings, err := jjm.EJPayloads(api, b, r.Header.Get("Content-Type"))
	if err != nil {
		var e jjm.EJError
		code := http.StatusBadRequest
		if errors.As(err, &e) {
			code = e.Code
		}
		reply(code, map[string]any{"Message": "The request is invalid.", "Detail": err.Error()})
		return
	}
	var body map[string]any
	_ = json.Unmarshal(b, &body)
	p.mu.Lock()
	p.got = append(p.got, PortalPush{API: api.Name, Body: body, Readings: readings})
	p.mu.Unlock()
	reply(http.StatusOK, map[string]any{"Status": true, "Message": jjm.EjalshaktiSuccess})
}

// SetKey changes an API's key (a key renewed on the portal).
func (p *Portal) SetKey(api, key string) {
	p.mu.Lock()
	p.keys[api] = key
	p.mu.Unlock()
}

// FailNext answers the next n requests 500.
func (p *Portal) FailNext(n int) { p.mu.Lock(); p.fail = n; p.mu.Unlock() }

// SetDown answers every request 503 (true) or serves again (false).
func (p *Portal) SetDown(on bool) { p.mu.Lock(); p.down = on; p.mu.Unlock() }

// SetDelay delays every answer by d.
func (p *Portal) SetDelay(d time.Duration) { p.mu.Lock(); p.delay = d; p.mu.Unlock() }

// KeyRefusals counts the requests refused for their key.
func (p *Portal) KeyRefusals() int { p.mu.Lock(); defer p.mu.Unlock(); return p.refusedKeys }

// Pushes returns what was accepted.
func (p *Portal) Pushes() []PortalPush {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]PortalPush(nil), p.got...)
}
