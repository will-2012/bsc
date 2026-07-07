// Copyright 2020 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package eth

import (
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/forkid"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// Tests that handshake failures are detected and reported correctly.
func TestHandshake68(t *testing.T) { testHandshake(t, ETH68) }
func TestHandshake70(t *testing.T) { testHandshake(t, ETH70) }

func testHandshake(t *testing.T, protocol uint) {
	t.Parallel()

	// Create a test backend only to have some valid genesis chain
	backend := newTestBackend(3)
	defer backend.close()

	var (
		genesis = backend.chain.Genesis()
		head    = backend.chain.CurrentBlock()
		td      = backend.chain.GetTd(head.Hash(), head.Number.Uint64())
		forkID  = forkid.NewID(backend.chain.Config(), backend.chain.Genesis(), backend.chain.CurrentHeader().Number.Uint64(), backend.chain.CurrentHeader().Time)
	)
	// makeStatus builds a status packet matching the negotiated protocol version:
	// eth/68 uses StatusPacket68, while eth/70 uses StatusPacket. Both carry total
	// difficulty (BSC keeps TD in the eth/70 handshake); eth/70 additionally
	// carries the served block range.
	makeStatus := func(version uint32, networkID uint64, genesisHash common.Hash, fID forkid.ID) interface{} {
		if protocol >= ETH69 {
			return StatusPacket{version, networkID, td, genesisHash, fID, 0, head.Number.Uint64(), head.Hash()}
		}
		return StatusPacket68{version, networkID, td, head.Hash(), genesisHash, fID}
	}

	type handshakeTest struct {
		code uint64
		data interface{}
		want error
	}
	tests := []handshakeTest{
		{
			code: TransactionsMsg, data: []interface{}{},
			want: errNoStatusMsg,
		},
		{
			code: StatusMsg, data: makeStatus(10, 1, genesis.Hash(), forkID),
			want: errProtocolVersionMismatch,
		},
		{
			code: StatusMsg, data: makeStatus(uint32(protocol), 999, genesis.Hash(), forkID),
			want: errNetworkIDMismatch,
		},
		{
			code: StatusMsg, data: makeStatus(uint32(protocol), 1, common.Hash{3}, forkID),
			want: errGenesisMismatch,
		},
		{
			code: StatusMsg, data: makeStatus(uint32(protocol), 1, genesis.Hash(), forkid.ID{Hash: [4]byte{0x00, 0x01, 0x02, 0x03}}),
			want: errForkIDRejected,
		},
	}
	// The block-range field only exists on eth/69+ status packets, so the
	// invalid-range rejection can only be exercised for those versions.
	if protocol >= ETH69 {
		tests = append(tests, handshakeTest{
			code: StatusMsg, data: StatusPacket{uint32(protocol), 1, td, genesis.Hash(), forkID, head.Number.Uint64() + 1, head.Number.Uint64(), head.Hash()},
			want: errInvalidBlockRange,
		})
	}
	for i, test := range tests {
		// Create the two peers to shake with each other
		app, net := p2p.MsgPipe()
		defer app.Close()
		defer net.Close()

		peer := NewPeer(protocol, p2p.NewPeer(enode.ID{}, "peer", nil), net, nil, nil)
		defer peer.Close()

		// Send the junk test with one peer, check the handshake failure
		go p2p.Send(app, test.code, test.data)

		err := peer.Handshake(1, backend.chain, BlockRangeUpdatePacket{}, td, nil)
		if err == nil {
			t.Errorf("test %d: protocol returned nil error, want %q", i, test.want)
		} else if !errors.Is(err, test.want) {
			t.Errorf("test %d: wrong error: got %q, want %q", i, err, test.want)
		}
	}
}
