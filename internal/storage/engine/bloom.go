package engine

import (
	"encoding/binary"
)

// BloomFilter is a space-efficient probabilistic data structure for set membership testing.
type BloomFilter struct {
	bits []byte
	k    uint32 // number of hash functions
}

// NewBloomFilter creates a bloom filter sized for expected n keys with target ~1% false positive rate (10 bits/key).
func NewBloomFilter(n int) *BloomFilter {
	if n < 64 {
		n = 64
	}
	mBits := uint32(n * 10)
	mBytes := (mBits + 7) / 8
	return &BloomFilter{
		bits: make([]byte, mBytes),
		k:    7, // optimal for 10 bits/key: ln(2) * 10 ≈ 6.93
	}
}

// NewBloomFilterFromBytes loads a serialized bloom filter.
func NewBloomFilterFromBytes(bits []byte, k uint32) *BloomFilter {
	if k == 0 {
		k = 7
	}
	return &BloomFilter{
		bits: bits,
		k:    k,
	}
}

// Bytes returns the underlying bitset.
func (b *BloomFilter) Bytes() []byte {
	return b.bits
}

// K returns the number of hash functions.
func (b *BloomFilter) K() uint32 {
	return b.k
}

// Add inserts a string key into the filter.
func (b *BloomFilter) Add(key string) {
	if len(b.bits) == 0 {
		return
	}
	h1, h2 := hashString(key)
	mBits := uint64(len(b.bits) * 8)

	for i := uint32(0); i < b.k; i++ {
		bit := (uint64(h1) + uint64(i)*uint64(h2)) % mBits
		b.bits[bit/8] |= (1 << (bit % 8))
	}
}

// MayContain returns false if the key is definitely not in the set, or true if it might be.
func (b *BloomFilter) MayContain(key string) bool {
	if len(b.bits) == 0 {
		return false
	}
	h1, h2 := hashString(key)
	mBits := uint64(len(b.bits) * 8)

	for i := uint32(0); i < b.k; i++ {
		bit := (uint64(h1) + uint64(i)*uint64(h2)) % mBits
		if (b.bits[bit/8] & (1 << (bit % 8))) == 0 {
			return false
		}
	}
	return true
}

// murmur3_32 implementation in pure Go
func murmur3(data []byte, seed uint32) uint32 {
	const (
		c1 = 0xcc9e2d51
		c2 = 0x1b873593
	)

	h := seed
	nblocks := len(data) / 4

	for i := 0; i < nblocks; i++ {
		k := binary.LittleEndian.Uint32(data[i*4 : (i+1)*4])
		k *= c1
		k = (k << 15) | (k >> 17)
		k *= c2

		h ^= k
		h = (h << 13) | (h >> 19)
		h = h*5 + 0xe6546b64
	}

	tail := data[nblocks*4:]
	var k1 uint32
	switch len(tail) {
	case 3:
		k1 ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(tail[0])
		k1 *= c1
		k1 = (k1 << 15) | (k1 >> 17)
		k1 *= c2
		h ^= k1
	}

	h ^= uint32(len(data))
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16

	return h
}

// hashString computes two 32-bit Murmur3 hashes with seeds 0 and 1
func hashString(s string) (uint32, uint32) {
	b := []byte(s)
	h1 := murmur3(b, 0)
	h2 := murmur3(b, 1)
	if h2 == 0 {
		h2 = 1 // avoid degenerate step
	}
	return h1, h2
}
