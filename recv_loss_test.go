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
	"time"
)

func newTestInputStream() *SsrcStream {
	return newSsrcStreamIn(
		&Address{IpAddr: net.ParseIP("127.0.0.1"), DataPort: 5000, CtrlPort: 5001},
		0x01020304,
	)
}

// feedRecvPackets delivers the given sequence numbers to an input stream, as if
// they were received from the remote source.
func feedRecvPackets(si *SsrcStream, seqs ...uint16) {
	for _, seq := range seqs {
		rp := newDataPacket()
		rp.SetSsrc(si.ssrc)
		rp.SetSequence(seq)
		rp.SetTimestamp(uint32(seq) * 160)
		rp.SetPayloadType(0)
		rp.SetPayload([]byte{0x01, 0x02})

		si.recordReceptionData(rp, &Session{}, time.Now().UnixNano(), func(int) *PayloadFormat {
			return PayloadFormatMap[0]
		})
		rp.FreePacket()
	}
}

// TestRecvReportPacketsLostRoundTrip checks that the 24 bit signed field of a
// receiver report survives a write/read cycle.
func TestRecvReportPacketsLostRoundTrip(t *testing.T) {
	for _, want := range []int32{0, 1, 42, -1, -1000, maxCumulativeLost, minCumulativeLost} {
		rr := make(recvReport, reportBlockLen)
		rr.setPacketsLost(want)

		if got := rr.packetsLost(); got != want {
			t.Errorf("packetsLost() = %d, want %d", got, want)
		}
	}
}

// TestReadRecvReportKeepsSignedPacketsLost checks that a negative cumulative
// loss reported by the remote receiver (duplicated packets) stays negative in
// the stream statistics instead of becoming a huge unsigned value.
func TestReadRecvReportKeepsSignedPacketsLost(t *testing.T) {
	const (
		startSeqNo = uint16(60000)
		ssrc       = uint32(0x01020304)
	)

	own := &Address{IpAddr: net.ParseIP("127.0.0.1"), DataPort: 5000, CtrlPort: 5001}
	str := newSsrcStreamOut(own, ssrc, startSeqNo)
	str.readRecvReport(buildRecvReport(ssrc, -1, uint32(startSeqNo)+999))

	if got := str.PacketsLost; got != -1 {
		t.Fatalf("PacketsLost = %d, want -1", got)
	}
	if got := str.statistics.packetCount; got != 1001 {
		t.Fatalf("packetCount = %d, want 1001", got)
	}
	if got := buildStreamStats(str).PacketsLost; got != -1 {
		t.Fatalf("StreamStats.PacketsLost = %d, want -1", got)
	}
}

// TestResolveRecvReportDuplicatePackets checks the cumulative loss computed for
// a received stream: a duplicated packet makes it negative (RFC 3550 defines
// the field as signed) instead of wrapping around to 4294967295.
func TestResolveRecvReportDuplicatePackets(t *testing.T) {
	si := newTestInputStream()

	feedRecvPackets(si, 1000, 1001, 1002, 1003, 1004)
	// The sequence tracking counts a duplicate of the highest sequence number
	// as received (RFC 3550 A.1), so the expected count stays behind.
	feedRecvPackets(si, 1004)

	si.resolveRecvReport()

	if got := si.statistics.packetCount; got != 6 {
		t.Fatalf("packetCount = %d, want 6", got)
	}
	if got := si.PacketsLost; got != -1 {
		t.Fatalf("PacketsLost = %d, want -1", got)
	}
}

// TestResolveRecvReportSequenceJump checks that a confirmed sequence number jump
// resets the counters of the previous epoch (RFC 3550 A.1, init_seq): without
// the reset the loss is computed against a stale base sequence number.
func TestResolveRecvReportSequenceJump(t *testing.T) {
	si := newTestInputStream()

	seqs := make([]uint16, 0, 500)
	for i := 0; i < 500; i++ {
		seqs = append(seqs, uint16(1000+i))
	}
	feedRecvPackets(si, seqs...)

	// Two sequential packets with a very large step confirm that the remote
	// source restarted its sequence numbering.
	feedRecvPackets(si, 60000, 60001)

	si.resolveRecvReport()

	if got := si.statistics.packetCount; got != 1 {
		t.Fatalf("packetCount = %d, want 1 (counters reset on re-sync)", got)
	}
	if got := si.PacketsLost; got != 0 {
		t.Fatalf("PacketsLost = %d, want 0", got)
	}
	if got := si.statistics.baseSeqNum; got != 60001 {
		t.Fatalf("baseSeqNum = %d, want 60001", got)
	}
}

// TestResolveRecvReportClampsCumulativeLost checks that the cumulative loss is
// clamped to the 24 bit signed range prescribed by RFC 3550 A.3.
func TestResolveRecvReportClampsCumulativeLost(t *testing.T) {
	t.Run("negative", func(t *testing.T) {
		si := newTestInputStream()
		si.statistics.baseSeqNum = 1000
		si.statistics.maxSeqNum = 1000
		si.statistics.packetCount = 0x900000

		si.resolveRecvReport()

		if got := si.PacketsLost; got != minCumulativeLost {
			t.Fatalf("PacketsLost = %d, want %d", got, minCumulativeLost)
		}
	})

	t.Run("positive", func(t *testing.T) {
		si := newTestInputStream()
		si.statistics.baseSeqNum = 1000
		si.statistics.seqNumAccum = 0x7fff0000
		si.statistics.maxSeqNum = 1000
		si.statistics.packetCount = 1

		si.resolveRecvReport()

		if got := si.PacketsLost; got != maxCumulativeLost {
			t.Fatalf("PacketsLost = %d, want %d", got, maxCumulativeLost)
		}
	})
}

// TestReinitSeqResetsJitter checks that a confirmed sequence number jump (the
// remote source restarted) also resets the jitter estimate and the transit
// time baseline: the timestamps of the new epoch are unrelated to the previous
// ones, so keeping the old state produces a huge jitter spike.
func TestReinitSeqResetsJitter(t *testing.T) {
	si := newTestInputStream()

	feed := func(seq uint16, ts uint32) {
		rp := newDataPacket()
		rp.SetSsrc(si.ssrc)
		rp.SetSequence(seq)
		rp.SetTimestamp(ts)
		rp.SetPayloadType(0)
		rp.SetPayload([]byte{0x01, 0x02})

		si.recordReceptionData(rp, &Session{}, time.Now().UnixNano(), func(int) *PayloadFormat {
			return PayloadFormatMap[0]
		})
		rp.FreePacket()
	}

	// Timestamps advance faster than the arrival time, so the transit time
	// changes at every packet and the jitter estimate becomes non zero.
	for i := 0; i < 100; i++ {
		feed(uint16(1000+i), uint32(i)*160*50)
	}
	if si.statistics.jitter == 0 {
		t.Fatal("precondition failed: jitter is zero before the restart")
	}

	// The source restarts with unrelated sequence number and timestamp.
	feed(60000, 0x70000000)
	feed(60001, 0x70000000+160)

	if got := si.statistics.jitter; got != 0 {
		t.Fatalf("jitter = %d, want 0 after sequence re-sync", got)
	}
	if got := si.statistics.packetCount; got != 1 {
		t.Fatalf("packetCount = %d, want 1 after sequence re-sync", got)
	}

	// The next packet must not produce a delta against the old epoch.
	feed(60002, 0x70000000+320)
	if got := si.statistics.jitter >> 4; got > 1000 {
		t.Fatalf("jitter = %d after restart, spike from the old transit time baseline", got)
	}
}

// TestReadRecvReportRecordsPeerReportTime checks that the reception of a
// receiver report is timestamped: it is what tells "the peer reported no loss"
// from "the peer never reported at all" (an output stream without reports has
// zeroed counters that look perfect).
func TestReadRecvReportRecordsPeerReportTime(t *testing.T) {
	const (
		startSeqNo = uint16(1000)
		ssrc       = uint32(0x05060708)
	)

	own := &Address{IpAddr: net.ParseIP("127.0.0.1"), DataPort: 5000, CtrlPort: 5001}
	str := newSsrcStreamOut(own, ssrc, startSeqNo)

	if got := buildStreamStats(str).LastPeerReport; got != 0 {
		t.Fatalf("LastPeerReport = %d before any report, want 0", got)
	}

	str.readRecvReport(buildRecvReport(ssrc, 0, uint32(startSeqNo)+99))

	if got := buildStreamStats(str).LastPeerReport; got == 0 {
		t.Fatal("LastPeerReport = 0 after a receiver report, want the reception time")
	}
}
