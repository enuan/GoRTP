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

// buildRecvReport builds a report block as it is carried by an RTCP receiver
// report. lost is the cumulative number of packets lost (24 bit signed).
func buildRecvReport(ssrc uint32, lost int32, highestSeq uint32) recvReport {
	rr := make(recvReport, reportBlockLen)
	rr.setSsrc(ssrc)
	rr.setPacketsLost(lost)
	rr.setHighestSeq(highestSeq)
	return rr
}

func TestReadRecvReportPacketCount(t *testing.T) {
	const startSeqNo = uint16(60000)
	const ssrc = uint32(0x01020304)

	own := &Address{IpAddr: net.ParseIP("127.0.0.1"), DataPort: 5000, CtrlPort: 5001}

	cases := []struct {
		name       string
		lost       int32
		highestSeq uint32
		want       uint32
	}{
		{
			// No wrap yet: 2000 packets sent since startSeqNo.
			name:       "no wrap",
			lost:       0,
			highestSeq: uint32(startSeqNo) + 1999,
			want:       2000,
		},
		{
			// The sequence number wrapped (startSeqNo + 7999 > 65535): the
			// extended sequence is 60000+7999, 8000 packets sent in total.
			name:       "wrapped sequence",
			lost:       10,
			highestSeq: uint32(startSeqNo) + 7999,
			want:       7990,
		},
		{
			// Several wraps: 3*65536 + 1000 packets since startSeqNo.
			name:       "multiple wraps",
			lost:       0,
			highestSeq: uint32(startSeqNo) + 3*65536 + 999,
			want:       3*65536 + 1000,
		},
		{
			// A negative loss (-1, duplicates): the receiver got one packet
			// more than expected.
			name:       "negative loss",
			lost:       -1,
			highestSeq: uint32(startSeqNo) + 999,
			want:       1001,
		},
		{
			// Report received before the stream sent anything: must not
			// underflow.
			name:       "stale report",
			lost:       0,
			highestSeq: 0,
			want:       0,
		},
		{
			// More packets reported lost than expected: the count is
			// clamped to zero instead of wrapping around.
			name:       "lost greater than expected",
			lost:       5000,
			highestSeq: uint32(startSeqNo) + 999,
			want:       0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			str := newSsrcStreamOut(own, ssrc, startSeqNo)
			str.readRecvReport(buildRecvReport(ssrc, tc.lost, tc.highestSeq))

			if got := str.statistics.packetCount; got != tc.want {
				t.Errorf("packetCount = %d, want %d", got, tc.want)
			}
		})
	}
}
