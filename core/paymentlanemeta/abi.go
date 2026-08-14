package paymentlanemeta

import (
	"encoding/binary"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/paymentlane"
)

var (
	getPaymentLaneParamsSelector = [4]byte{0xff, 0x62, 0x01, 0x47}
	getPaymentContractsSelector  = [4]byte{0x08, 0xfc, 0xc4, 0x5a}
)

func encodeGetPaymentLaneParams() []byte {
	return getPaymentLaneParamsSelector[:]
}

func encodeGetPaymentContracts(offset, limit uint64) []byte {
	input := make([]byte, 4+32+32)
	copy(input, getPaymentContractsSelector[:])
	binary.BigEndian.PutUint64(input[4+24:4+32], offset)
	binary.BigEndian.PutUint64(input[4+32+24:4+64], limit)
	return input
}

func decodeParams(ret []byte) (paymentlane.Params, error) {
	if len(ret) != 8*32 {
		return paymentlane.Params{}, fmt.Errorf("%w: getPaymentLaneParams returned %d bytes", paymentlane.ErrCorruptConfig, len(ret))
	}
	words := make([]uint64, 8)
	for i := range words {
		v, ok := decodeWord64(ret[i*32 : (i+1)*32])
		if !ok {
			return paymentlane.Params{}, fmt.Errorf("%w: getPaymentLaneParams word %d overflows uint64", paymentlane.ErrCorruptConfig, i)
		}
		words[i] = v
	}
	return paymentlane.Params{
		MinRatio:      words[0],
		MaxRatio:      words[1],
		ExpandTrigger: words[2],
		ShrinkTrigger: words[3],
		ExpandStep:    words[4],
		ShrinkStep:    words[5],
		MinGas:        words[6],
		MaxGas:        words[7],
	}, nil
}

func decodeContractsPage(ret []byte) ([]common.Address, uint64, error) {
	if len(ret) < 96 {
		return nil, 0, fmt.Errorf("%w: getPaymentContracts returned %d bytes", paymentlane.ErrCorruptConfig, len(ret))
	}
	offset, ok := decodeWord64(ret[:32])
	if !ok || offset != 64 {
		return nil, 0, fmt.Errorf("%w: getPaymentContracts offset = %#x", paymentlane.ErrCorruptConfig, ret[:32])
	}
	total, ok := decodeWord64(ret[32:64])
	if !ok {
		return nil, 0, fmt.Errorf("%w: getPaymentContracts totalLength = %#x", paymentlane.ErrCorruptConfig, ret[32:64])
	}
	count, ok := decodeWord64(ret[64:96])
	if !ok {
		return nil, 0, fmt.Errorf("%w: getPaymentContracts page length = %#x", paymentlane.ErrCorruptConfig, ret[64:96])
	}
	wantLen := 96 + int(count)*32
	if len(ret) != wantLen {
		return nil, 0, fmt.Errorf("%w: getPaymentContracts length = %d, want %d", paymentlane.ErrCorruptConfig, len(ret), wantLen)
	}
	page := make([]common.Address, count)
	for i := uint64(0); i < count; i++ {
		word := ret[96+i*32 : 128+i*32]
		for _, b := range word[:12] {
			if b != 0 {
				return nil, 0, fmt.Errorf("%w: getPaymentContracts[%d] = %#x", paymentlane.ErrCorruptConfig, i, word)
			}
		}
		page[i] = common.BytesToAddress(word[12:])
	}
	return page, total, nil
}

func decodeWord64(word []byte) (uint64, bool) {
	if len(word) != 32 {
		return 0, false
	}
	for _, b := range word[:24] {
		if b != 0 {
			return 0, false
		}
	}
	return binary.BigEndian.Uint64(word[24:32]), true
}
