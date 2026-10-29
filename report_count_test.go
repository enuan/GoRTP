package rtp

import (
	"net"
	"testing"
)

// The number of reports a stream has seen identifies them: two reports can carry
// the same numbers (a call on hold, an idle direction) and still be two
// measurements.
func TestStreamStatsReportCount(t *testing.T) {
	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")
	tp, err := NewTransportUDP(addr, 5252, "")
	if err != nil {
		t.Fatalf("create transport: %v", err)
	}

	rs := NewSession(tp, tp)
	rs.rtcpServiceActive = true

	strIdx, herr := rs.NewSsrcStreamOut(&Address{IpAddr: addr.IP, DataPort: 5252, CtrlPort: 5253}, 0x01020304, 0x4712)
	if herr != "" {
		t.Fatalf("create output stream: %s", herr)
	}
	strOut := rs.SsrcStreamOutForIndex(strIdx)
	strOut.SetPayloadType(0)

	si := newSsrcStreamIn(&Address{IpAddr: addr.IP, DataPort: 5010, CtrlPort: 5011}, 0x0d0c0b0a)
	rs.streamsMapMutex.Lock()
	rs.streamsIn[1] = si
	rs.streamsMapMutex.Unlock()

	feedRecvPackets(si, 1, 2, 3)

	// our report is built for the streams with data to report
	rs.buildRtcpPkt(strOut, 31)
	stats := rs.GetStats()
	if got := stats.Inbound[0].ReportCount; got != 1 {
		t.Fatalf("report count = %d after the first report, want 1", got)
	}

	feedRecvPackets(si, 4, 5)
	rs.buildRtcpPkt(strOut, 31)
	stats = rs.GetStats()
	if got := stats.Inbound[0].ReportCount; got != 2 {
		t.Fatalf("report count = %d after the second report, want 2", got)
	}

	// the peer reports about our output stream: the same counter, so that a
	// report is identified in both directions
	first := stats.Outbound[0].ReportCount
	pkt, _ := strOut.newCtrlPacket(RtcpRR)
	offset := pkt.addHeaderSsrc(0, strOut.Ssrc())
	strOut.readRecvReport(pkt.toRecvReport(offset))
	if got := rs.GetStats().Outbound[0].ReportCount; got != first+1 {
		t.Fatalf("outbound report count = %d, want %d", got, first+1)
	}
}

// The report is computed and read while holding the stream: an application that
// reads the statistics sees the values and the count of the same report.
func TestStreamStatsReportCountLocking(t *testing.T) {
	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")
	tp, err := NewTransportUDP(addr, 5353, "")
	if err != nil {
		t.Fatalf("create transport: %v", err)
	}

	rs := NewSession(tp, tp)
	rs.rtcpServiceActive = true

	strIdx, herr := rs.NewSsrcStreamOut(&Address{IpAddr: addr.IP, DataPort: 5353, CtrlPort: 5354}, 0x01020304, 0x4712)
	if herr != "" {
		t.Fatalf("create output stream: %s", herr)
	}
	strOut := rs.SsrcStreamOutForIndex(strIdx)
	strOut.SetPayloadType(0)

	si := newSsrcStreamIn(&Address{IpAddr: addr.IP, DataPort: 5012, CtrlPort: 5013}, 0x0d0c0b0a)
	rs.streamsMapMutex.Lock()
	rs.streamsIn[1] = si
	rs.streamsMapMutex.Unlock()

	feedRecvPackets(si, 1, 2, 3)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			si.Lock()
			si.dataAfterLastReport = true
			si.Unlock()
			rs.buildRtcpPkt(strOut, 31)
		}
	}()
	for i := 0; i < 100; i++ {
		rs.GetStats()
	}
	<-done
}
