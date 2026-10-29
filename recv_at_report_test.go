package rtp

import (
	"net"
	"testing"
	"time"
)

// The loss of a stream we receive is computed when we build our report, while
// the received count keeps advancing with every packet: the snapshot exposes the
// count of the report, so that a window ends on two counters of the same moment.
func TestStreamStatsReceivedAtReport(t *testing.T) {
	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")
	tp, err := NewTransportUDP(addr, 5250, "")
	if err != nil {
		t.Fatalf("create transport: %v", err)
	}

	rs := NewSession(tp, tp)
	rs.rtcpServiceActive = true

	strIdx, herr := rs.NewSsrcStreamOut(&Address{IpAddr: addr.IP, DataPort: 5250, CtrlPort: 5251}, 0x01020304, 0x4711)
	if herr != "" {
		t.Fatalf("create output stream: %s", herr)
	}
	strOut := rs.SsrcStreamOutForIndex(strIdx)
	strOut.SetPayloadType(0)

	si := newSsrcStreamIn(&Address{IpAddr: addr.IP, DataPort: 5000, CtrlPort: 5001}, 0x0a0b0c0d)
	rs.streamsMapMutex.Lock()
	rs.streamsIn[1] = si
	rs.streamsMapMutex.Unlock()

	feedRecvPackets(si, 1, 2, 3, 5) // 4 is lost

	// before any report the loss was never computed
	stats := rs.GetStats()
	if len(stats.Inbound) != 1 {
		t.Fatalf("inbound streams = %d, want 1", len(stats.Inbound))
	}
	if got := stats.Inbound[0].PacketsReceivedAtReport; got != 0 {
		t.Fatalf("received at report = %d before a report, want 0", got)
	}

	// build our report: the loss is computed on the 5 packets received so far
	rs.buildRtcpPkt(strOut, 31)
	stats = rs.GetStats()
	if stats.Inbound[0].PacketsLost != 1 {
		t.Fatalf("lost = %d, want 1", stats.Inbound[0].PacketsLost)
	}

	// two more packets arrive: the received count advances, the loss does not
	feedRecvPackets(si, 6, 8)
	stats = rs.GetStats()
	if got := stats.Inbound[0].PacketsReceived; got != 6 {
		t.Fatalf("received = %d, want 6", got)
	}
	if got := stats.Inbound[0].PacketsReceivedAtReport; got != 4 {
		t.Fatalf("received at report = %d, want the 4 of the report", got)
	}

	// the next report recomputes both on the same moment
	rs.buildRtcpPkt(strOut, 31)
	stats = rs.GetStats()
	if got := stats.Inbound[0].PacketsReceivedAtReport; got != 6 {
		t.Fatalf("received at report = %d, want 6", got)
	}
	if stats.Inbound[0].PacketsLost != 2 { // 7 was expected and never arrived
		t.Fatalf("lost = %d, want 2", stats.Inbound[0].PacketsLost)
	}

	_ = time.Now
}
