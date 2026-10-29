// Copyright (C) 2011 Werner Dittmann
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package rtp

import (
	"net"
	"testing"
)

// Building a receiver report refreshes the reception statistics of the input
// stream (packets lost, jitter, highest sequence number, LSR/DLSR) and signals
// the transmission to the application, which samples them on that event.
func TestBuildRtcpPktSignalsTransmittedReport(t *testing.T) {
	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")

	tp, err := NewTransportUDP(addr, 5240, "")
	if err != nil {
		t.Fatalf("create transport: %v", err)
	}

	rs := NewSession(tp, tp)
	rs.rtcpServiceActive = true

	strIdx, herr := rs.NewSsrcStreamOut(&Address{IpAddr: addr.IP, DataPort: 5240, CtrlPort: 5241}, 0x01020304, 0x4711)
	if herr != "" {
		t.Fatalf("create output stream: %s", herr)
	}
	strOut := rs.SsrcStreamOutForIndex(strIdx)
	strOut.SetPayloadType(0)

	// an input stream with data received since the last report: it is the one
	// that gets a receiver report block
	si := newSsrcStreamIn(&Address{IpAddr: addr.IP, DataPort: 5000, CtrlPort: 5001}, 0x0a0b0c0d)
	rs.streamsMapMutex.Lock()
	rs.streamsIn[2] = si
	rs.streamsMapMutex.Unlock()
	si.dataAfterLastReport = true

	ctrlCh := rs.CreateCtrlEventChan()

	rs.buildRtcpPkt(strOut, 31)

	select {
	case evts := <-ctrlCh:
		if len(evts) != 1 {
			t.Fatalf("events = %d, want 1", len(evts))
		}
		if evts[0].EventType != RtcpTxReport {
			t.Fatalf("event type = %d, want %d (RtcpTxReport)", evts[0].EventType, RtcpTxReport)
		}
		if evts[0].Ssrc != si.ssrc {
			t.Fatalf("event ssrc = %#x, want %#x", evts[0].Ssrc, si.ssrc)
		}
		if evts[0].Index != 2 {
			t.Fatalf("event index = %d, want 2", evts[0].Index)
		}
	default:
		t.Fatal("no control event for the transmitted report")
	}

	// no new data since the last report: no report block, no event
	rs.buildRtcpPkt(strOut, 31)
	select {
	case evts := <-ctrlCh:
		t.Fatalf("events = %v, want none without new data", evts)
	default:
	}
}
