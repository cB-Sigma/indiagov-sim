// Package sim simulates a PM-KUSUM Remote Monitoring System (RMS)
// speaking the message profile of "RMS Communication and Security
// Architecture" (the 2020 SEDM and 2022 National Portal documents) over
// MQTT: it publishes messages in the layouts of the documents' annexures
// (with the OTP it last received), and answers on-demand and config
// commands the way annexure 7 describes (read: each keyword's value
// replaced; write: applied and echoed; unknown keyword: 0; handshake
// keywords returned as received).
package sim

// Sample messages in the layouts of annexures 1 to 8: the keywords each
// annexure lists, with IMEIs, asset serials, values, hosts and credentials
// of our own. They are not the documents' printed samples. The IMEI has 16
// digits, as the documents' samples do (F-472 in PROVENANCE.md).
const (
	// SamplePump is the annexure 1 layout (pump controller periodic
	// push, VD 1, ASN_11); the pump is stopped.
	SamplePump = `{"VD":1,"TIMESTAMP":"2026-10-10 06:15:00","MAXINDEX":96,"INDEX":25,"LOAD":0,"STINTERVAL":15,"MSGID":"","DATE":261010,
"IMEI":"8600000000000016","ASN_11":"PC0000001","POTP":"11112222","COTP":"33334444",
"PMAXFREQ1":"50.00","PFREQLSP1":"30.00","PFREQHSP1":"50.00","PCNTRMODE1":"2","PRUNST1":"0","PREFFREQ1":"48.50",
"POPFREQ1":"48.20","POPI1":"9.60","POPV1":"380.00","POPKW1":"4.20","PDC1V1":"560.00","PDC1I1":"8.00",
"PDCVOC1":"640.00","PDKWH1":"12.40","PTOTKWH1":"1834.60","POPFLW1":"310.00","POPDWD1":"52000","POPTOTWD1":"7480000",
"PMAXDCV1":"800.00","PMAXDCI1":"12.00","PMAXKW1":"5.50","PMAXFLW1":"420.00","PDHR1":"3.50","PTOTHR1":"1265.20"}`

	// SampleUSPC is the annexure 8 layout of the 2022 National Portal
	// document: pump controller keywords and the five channels of a
	// universal solar pump controller (numbers as text, run and fault
	// status as numbers); channel 2 reports a fault.
	SampleUSPC = `{"VD":1,"TIMESTAMP":"2026-10-10 06:15:00","MAXINDEX":96,"INDEX":25,"LOAD":0,"STINTERVAL":15,"MSGID":"","DATE":261010,
"IMEI":"8600000000000016","ASN_11":"PC0000001","POTP":"11112222","COTP":"33334444",
"PCNTRMODE1":"2","PRUNST1":"1","POPFREQ1":"48.20","POPI1":"9.60","POPV1":"380.00","POPKW1":"4.20","PDC1V1":"560.00","PDC1I1":"8.00",
"PDKWH1":"12.40","PTOTKWH1":"1834.60","POPFLW1":"310","POPDWD1":"52000","POPTOTWD1":"7480000","PDHR1":"3.50","PTOTHR1":"1265.20",
"UDKWH1":"2.48","UTOTKWH1":"366.90","UDHR1":"0.70","UTOTHR1":"253.10","UOPI1":"9.60","UOPV1":"380.00","UOPKW1":"4.20","URUNST1":1,"UFLTST1":0,
"UDKWH2":"1.10","UTOTKWH2":"201.40","UDHR2":"0.30","UTOTHR2":"140.20","UOPI2":"0.00","UOPV2":"0.00","UOPKW2":"0.00","URUNST2":0,"UFLTST2":1,
"UDKWH3":"0.00","UTOTKWH3":"88.20","UDHR3":"0.00","UTOTHR3":"61.50","UOPI3":"0.00","UOPV3":"0.00","UOPKW3":"0.00","URUNST3":0,"UFLTST3":0,
"UDKWH4":"3.05","UTOTKWH4":"512.70","UDHR4":"0.90","UTOTHR4":"355.80","UOPI4":"4.10","UOPV4":"230.00","UOPKW4":"0.94","URUNST4":1,"UFLTST4":0,
"UDKWH5":"0.40","UTOTKWH5":"64.30","UDHR5":"0.20","UTOTHR5":"44.60","UOPI5":"1.20","UOPV5":"230.00","UOPKW5":"0.28","URUNST5":1,"UFLTST5":0}`

	// SampleMeter is the annexure 2 layout (energy meter, VD 2, ASN_21).
	SampleMeter = `{"VD":2,"TIMESTAMP":"2026-10-10 06:15:00","MAXINDEX":96,"INDEX":25,"LOAD":0,"STINTERVAL":15,"MSGID":"","DATE":261010,
"IMEI":"8600000000000016","ASN_21":"EM0000001","MTDET1":40020001,"POTP":"11112222","COTP":"33334444",
"MTBLDATE1":1,"DATE1":261010,"TIME1":61500,"IR1":6.12,"IY1":6.08,"IB1":6.21,"VRN1":231.4,"VYN1":229.8,"VBN1":232.1,
"VRY1":399.6,"VYB1":398.2,"VBR1":401.3,"PFR1":0.91,"PFY1":0.93,"PFB1":0.92,"FRQ1":49.98,
"POWR1":1.29,"POWY1":1.30,"POWB1":1.33,"POW1":3.92,"RPOWR1":0.58,"RPOWY1":0.52,"RPOWB1":0.55,"RPOW1":1.65,
"APOWR1":1.42,"APOWY1":1.40,"APOWB1":1.44,"APOW1":4.26,"KWHNET1":5120.4,"KWHIMP1":5310.7,"KWHEXP1":190.3,
"KVAHNET1":5560.2,"KVAHIMP1":5752.8,"KVAHEXP1":192.6,"MDKWIMP1":4.6,"MDKWEXP1":0.4,
"POFF1":312,"TC1":0,"PF1":0.92,"LBKWHNET1":4980,"LBKWHIMP1":5165,"LBKWHEXP1":185,"PMDKVAIMP1":5.10,"PMDKVAEXP1":0.50,
"LBMDKWIMP1":4.4,"LBMDKWEXP1":0.3,"LBMDKVAIMP1":4.9,"LBMDKVAEXP1":0.4,"MDRSTC1":7}`

	// SampleInverter is the annexure 3 layout (inverter, VD 5, ASN_31);
	// fault word 3 is set.
	SampleInverter = `{"VD":5,"TIMESTAMP":"2026-10-10 06:15:00","MAXINDEX":96,"INDEX":25,"LOAD":0,"STINTERVAL":15,"MSGID":"","DATE":261010,
"IMEI":"8600000000000016","ASN_31":"IN0000001","POTP":"11112222","COTP":"33334444",
"IST1":1,"IFREQ1":50.01,"IPF1":0.99,"IDC1V1":610,"IDC1I1":7.9,"IDC1KW1":4.82,"IDC2V1":605,"IDC2I1":7.7,"IDC2KW1":4.66,
"IDC3V1":0,"IDC3I1":0,"IDC3KW1":0,"IDC4V1":0,"IDC4I1":0,"IDC4KW1":0,
"IRPHV1":232.0,"IRPHI1":13.6,"IRPHKW1":3.13,"IYPHV1":230.5,"IYPHI1":13.5,"IYPHKW1":3.10,"IBPHV1":231.2,"IBPHI1":13.4,"IBPHKW1":3.08,
"IKW1":9.31,"ITKWH1":41.2,"ITON1":6.4,"ILKWH1":28450.6,"ILON1":9120.5,"ITEMP1":47.5,
"IFT11":0,"IFT21":0,"IFT31":4,"IFT41":0,"IFT51":0,"IKVA1":9.40,"IKVAR1":1.10}`

	// SampleSJB is the annexure 4 layout (string combiner box, VD 9,
	// ASN_41).
	SampleSJB = `{"VD":9,"TIMESTAMP":"2026-10-10 06:15:00","MAXINDEX":96,"INDEX":25,"LOAD":0,"STINTERVAL":15,"MSGID":"","DATE":261010,
"IMEI":"8600000000000016","ASN_41":"SB0000001","POTP":"11112222","COTP":"33334444",
"SI11":"6.10","SI21":"6.05","SI31":"6.02","SI41":"6.08","SI51":"6.11","SI61":"5.97","SI71":"6.00","SI81":"6.04","SI91":"6.06",
"SI101":"5.90","SI111":"6.01","SI121":"6.03","SI131":"6.09","SI141":"5.99","SI151":"6.02","SI161":"6.00","SI171":"6.07",
"SI181":"6.05","SI191":"6.01","SI201":"5.98","SI211":"6.04","SI221":"6.02","SI231":"6.03","SI241":"6.00",
"SDCV1":"602.00","SDCTOTI1":"144.20","SDCTOTKW1":"86.80","SDI11":"1","SDI21":"0","ST11":"41.0","ST21":"42.5","ST31":"40.8"}`

	// SampleHeartbeat is the annexure 5 layout (RMS heartbeat, VD 0).
	SampleHeartbeat = `{"VD":0,"TIMESTAMP":"2026-10-10 06:15:00","MAXINDEX":96,"INDEX":25,"LOAD":0,"STINTERVAL":15,"MSGID":"","DATE":261010,
"IMEI":"8600000000000016","POTP":"11112222","COTP":"33334444","GSM":1,"SIM":1,"NET":1,"GPRS":1,"RSSI":18,"SD":1,"ONLINE":1,
"GPS":1,"GPSLOC":1,"RF":0,"RTCDATE":261010,"RTCTIME":61500,"TEMP":38.5,"LAT":18.5204,"LONG":73.8567,"SIMSLOT":1,"SIMCHNGCNT":2,
"FLASH":1,"BATTST":0,"VBATT":4.1,"PST":1}`

	// SampleHeartbeatLower is the heartbeat with every keyword in lower
	// case (as some field firmware sends it) and DATE in the YYYY-MM-DD
	// form of the header table instead of the samples' YYMMDD.
	SampleHeartbeatLower = `{"vd":0,"timestamp":"2026-10-10 06:30:00","maxindex":96,"index":26,"load":0,"stinterval":15,"msgid":"","date":"2026-10-10",
"imei":"8600000000000016","potp":"11112222","cotp":"33334444","gsm":1,"sim":1,"net":1,"gprs":1,"rssi":17,"sd":1,"online":1,
"gps":1,"gpsloc":1,"rf":0,"rtcdate":261010,"rtctime":63000,"temp":38.9,"lat":18.5204,"long":73.8567,"simslot":1,"simchngcnt":2,
"flash":1,"battst":0,"vbatt":4.1,"pst":1}`

	// SampleDAQ is the annexure 6 layout (DAQ, VD 12, no ASN keyword).
	SampleDAQ = `{"VD":12,"TIMESTAMP":"2026-10-10 06:15:00","MAXINDEX":96,"INDEX":25,"LOAD":0,"STINTERVAL":15,"MSGID":"","DATE":261010,
"IMEI":"8600000000000016","POTP":"11112222","COTP":"33334444",
"AI11":12.4,"AI21":7.9,"AI31":0,"AI41":18.2,"DI11":1,"DI21":0,"DI31":0,"DI41":1,"DO11":0,"DO21":1,"DO31":0,"DO41":1}`

	// SampleConfigWrite is the annexure 7 layout of a config write with
	// every keyword (its answer has the same content).
	SampleConfigWrite = `{"timestamp":"2026-10-10 06:20:00","type":"config","cmd":"write","msgid":"501",
"APN1":"demo.apn","USR1":"demo","PASS1":"demo-pass","APN2":"demo2.apn","USR2":"demo","PASS2":"demo-pass","RESTART":0,
"UPDATEINTERVAL":10,"HEARTINTERVAL":5,"URTCDATE":261010,"URTCTIME":62000,"UPDATERTC":1,"GSMSYNC":0,"DO1":0,
"AI1ZERO":4,"AI1SPAN":20,"AI2ZERO":4,"AI2SPAN":20,"AI3ZERO":0,"AI3SPAN":10,"AI4ZERO":0,"AI4SPAN":10,
"URL":"rms.example.invalid","PORT":8883,"CID":"d:860000000000001$standalonesolarpump$1",
"USERNAME":"860000000000001$standalonesolarpump$1","PASSWORD":"demo-pass",
"FTPURL":"ftp.example.invalid","FTPUSER":"demo","FTPPASS":"demo-pass","FTPPORT":21,"FTPDOWN":0}`

	// SampleInvalidKeyCommand and SampleInvalidKeyResponse follow annexure
	// 7's second exchange: a write with a keyword the RMS does not know,
	// answered with 0 for that keyword.
	SampleInvalidKeyCommand  = `{"timestamp":"2026-10-10 06:25:00","type":"config","cmd":"write","msgid":"502","APNX1":"demo.apn","HEARTINTERVAL":5}`
	SampleInvalidKeyResponse = `{"timestamp":"2026-10-10 06:25:00","type":"config","cmd":"write","msgid":"502","APNX1":0,"HEARTINTERVAL":5}`
)
