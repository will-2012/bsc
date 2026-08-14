package paymentlane

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/holiman/uint256"
)

const (
	paymentContractsLenSlot = 8
	numParams               = 8
)

// StorageReader is the test-only state capability the legacy raw-slot oracle needs.
type StorageReader interface {
	Storage(addr common.Address, slot common.Hash) (common.Hash, error)
}

func paramSlot(i int) common.Hash {
	return common.Hash{31: byte(i)}
}

func paymentContractSlot(i uint64) common.Hash {
	base := new(uint256.Int).SetBytes32(crypto.Keccak256(common.Hash{31: paymentContractsLenSlot}.Bytes()))
	return base.AddUint64(base, i).Bytes32()
}

func word64(w common.Hash) (uint64, bool) {
	for _, b := range w[:24] {
		if b != 0 {
			return 0, false
		}
	}
	return new(uint256.Int).SetBytes32(w[:]).Uint64(), true
}

func orDefault(stored, fallback uint64) uint64 {
	if stored == 0 {
		return fallback
	}
	return stored
}

// LoadParams is the legacy raw-slot oracle kept only for tests.
func LoadParams(r StorageReader) (Params, error) {
	var raw [numParams]uint64
	for i := range raw {
		slot := paramSlot(i)
		w, err := r.Storage(ContractAddress, slot)
		if err != nil {
			return Params{}, fmt.Errorf("%w: params slot %d: %w", ErrStateUnavailable, i, err)
		}
		v, ok := word64(w)
		if !ok {
			return Params{}, fmt.Errorf("%w: params slot %d = %#x", ErrCorruptConfig, i, w)
		}
		raw[i] = v
	}
	return Params{
		MinRatio:      orDefault(raw[0], defaultMinRatio),
		MaxRatio:      orDefault(raw[1], defaultMaxRatio),
		ExpandTrigger: orDefault(raw[2], defaultExpandTrigger),
		ShrinkTrigger: orDefault(raw[3], defaultShrinkTrigger),
		ExpandStep:    orDefault(raw[4], defaultExpandStep),
		ShrinkStep:    orDefault(raw[5], defaultShrinkStep),
		MinGas:        orDefault(raw[6], defaultMinGas),
		MaxGas:        orDefault(raw[7], defaultMaxGas),
	}, nil
}

// LoadPaymentContracts is the legacy raw-slot oracle kept only for tests.
func LoadPaymentContracts(r StorageReader) (map[common.Address]struct{}, error) {
	w, err := r.Storage(ContractAddress, common.Hash{31: paymentContractsLenSlot})
	if err != nil {
		return nil, fmt.Errorf("%w: payment-contract count: %w", ErrStateUnavailable, err)
	}
	n, ok := word64(w)
	if !ok {
		return nil, fmt.Errorf("%w: payment-contract count = %#x", ErrCorruptConfig, w)
	}
	if n == 0 {
		return nil, nil
	}
	set := make(map[common.Address]struct{})
	for i := uint64(0); i < n; i++ {
		w, err := r.Storage(ContractAddress, paymentContractSlot(i))
		if err != nil {
			return nil, fmt.Errorf("%w: payment contract %d: %w", ErrStateUnavailable, i, err)
		}
		for _, b := range w[:12] {
			if b != 0 {
				return nil, fmt.Errorf("%w: payment contract %d = %#x", ErrCorruptConfig, i, w)
			}
		}
		addr := common.BytesToAddress(w[12:])
		if _, dup := set[addr]; dup {
			return nil, fmt.Errorf("%w: payment contract %d = %x is a duplicate", ErrCorruptConfig, i, addr)
		}
		set[addr] = struct{}{}
	}
	return set, nil
}
