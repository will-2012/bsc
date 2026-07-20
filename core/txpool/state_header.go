// Copyright 2026 The go-ethereum Authors
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

package txpool

import (
	"math/big"

	"github.com/ethereum/go-ethereum/core/types"
)

// EmptyStateHeader returns a synthetic header whose state root is the empty trie.
// Tx pool startup uses this as a fallback when the head state is temporarily
// unavailable during snap sync recovery.
func EmptyStateHeader() *types.Header {
	return &types.Header{
		Number: new(big.Int),
		Root:   types.EmptyRootHash,
	}
}
