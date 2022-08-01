package index

import (
	"fmt"
	"io"
	"math/rand"
	"sort"
	"testing"

	roaring "github.com/dgraph-io/sroar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFromBytes(t *testing.T) {
	for i := uint32(1); i < 200; i++ {
		t.Run(fmt.Sprintf("New%d", i), func(t *testing.T) {
			index := &CheckpointIndex{}
			index.SetActive(i)
			b := index.Flush()
			newIndex, err := NewCheckpointIndex(b)
			require.NoError(t, err)
			assert.Equal(t, index.firstCheckpoint, newIndex.firstCheckpoint)
			assert.Equal(t, index.lastCheckpoint, newIndex.lastCheckpoint)
			assert.Equal(t, index.bitmap, newIndex.bitmap)
		})
	}
}

func TestSetActive(t *testing.T) {
	cases := []struct {
		checkpoint           uint32
		rangeFirstCheckpoint uint32
		bitmap               []byte
	}{
		{1, 1, []byte{0b1000_0000}},
		{2, 1, []byte{0b0100_0000}},
		{3, 1, []byte{0b0010_0000}},
		{4, 1, []byte{0b0001_0000}},
		{5, 1, []byte{0b0000_1000}},
		{6, 1, []byte{0b0000_0100}},
		{7, 1, []byte{0b0000_0010}},
		{8, 1, []byte{0b0000_0001}},

		{9, 9, []byte{0b1000_0000}},
		{10, 9, []byte{0b0100_0000}},
		{11, 9, []byte{0b0010_0000}},
		{12, 9, []byte{0b0001_0000}},
		{13, 9, []byte{0b0000_1000}},
		{14, 9, []byte{0b0000_0100}},
		{15, 9, []byte{0b0000_0010}},
		{16, 9, []byte{0b0000_0001}},
	}

	for _, tt := range cases {
		t.Run(fmt.Sprintf("init_%d", tt.checkpoint), func(t *testing.T) {
			index := &CheckpointIndex{}
			index.SetActive(tt.checkpoint)

			assert.Equal(t, tt.bitmap, index.bitmap)
			assert.Equal(t, tt.rangeFirstCheckpoint, index.rangeFirstCheckpoint())
			assert.Equal(t, tt.checkpoint, index.firstCheckpoint)
			assert.Equal(t, tt.checkpoint, index.lastCheckpoint)
		})
	}

	// Update current bitmap right
	index := &CheckpointIndex{}
	index.SetActive(1)
	assert.Equal(t, uint32(1), index.firstCheckpoint)
	assert.Equal(t, uint32(1), index.lastCheckpoint)
	index.SetActive(8)
	assert.Equal(t, []byte{0b1000_0001}, index.bitmap)
	assert.Equal(t, uint32(1), index.firstCheckpoint)
	assert.Equal(t, uint32(8), index.lastCheckpoint)

	// Update current bitmap left
	index = &CheckpointIndex{}
	index.SetActive(8)
	assert.Equal(t, uint32(8), index.firstCheckpoint)
	assert.Equal(t, uint32(8), index.lastCheckpoint)
	index.SetActive(1)
	assert.Equal(t, []byte{0b1000_0001}, index.bitmap)
	assert.Equal(t, uint32(1), index.firstCheckpoint)
	assert.Equal(t, uint32(8), index.lastCheckpoint)

	index = &CheckpointIndex{}
	index.SetActive(10)
	index.SetActive(9)
	index.SetActive(16)
	assert.Equal(t, []byte{0b1100_0001}, index.bitmap)
	assert.Equal(t, uint32(9), index.firstCheckpoint)
	assert.Equal(t, uint32(16), index.lastCheckpoint)

	// Expand bitmap to the left
	index = &CheckpointIndex{}
	index.SetActive(10)
	index.SetActive(1)
	assert.Equal(t, []byte{0b1000_0000, 0b0100_0000}, index.bitmap)
	assert.Equal(t, uint32(1), index.firstCheckpoint)
	assert.Equal(t, uint32(10), index.lastCheckpoint)

	index = &CheckpointIndex{}
	index.SetActive(17)
	index.SetActive(2)
	assert.Equal(t, []byte{0b0100_0000, 0b0000_0000, 0b1000_0000}, index.bitmap)
	assert.Equal(t, uint32(2), index.firstCheckpoint)
	assert.Equal(t, uint32(17), index.lastCheckpoint)

	// Expand bitmap to the right
	index = &CheckpointIndex{}
	index.SetActive(1)
	index.SetActive(10)
	assert.Equal(t, []byte{0b1000_0000, 0b0100_0000}, index.bitmap)
	assert.Equal(t, uint32(1), index.firstCheckpoint)
	assert.Equal(t, uint32(10), index.lastCheckpoint)

	index = &CheckpointIndex{}
	index.SetActive(2)
	index.SetActive(17)
	assert.Equal(t, []byte{0b0100_0000, 0b0000_0000, 0b1000_0000}, index.bitmap)
	assert.Equal(t, uint32(2), index.firstCheckpoint)
	assert.Equal(t, uint32(17), index.lastCheckpoint)

	index = &CheckpointIndex{}
	index.SetActive(17)
	index.SetActive(26)
	assert.Equal(t, []byte{0b1000_0000, 0b0100_0000}, index.bitmap)
	assert.Equal(t, uint32(17), index.firstCheckpoint)
	assert.Equal(t, uint32(26), index.lastCheckpoint)
}

func TestNextActive(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		index := &CheckpointIndex{}

		i, err := index.NextActive(0)
		assert.Equal(t, uint32(0), i)
		assert.EqualError(t, err, io.EOF.Error())
	})

	t.Run("one byte", func(t *testing.T) {
		t.Run("after last", func(t *testing.T) {
			index := &CheckpointIndex{}
			index.SetActive(3)

			// 16 is well-past the end
			i, err := index.NextActive(16)
			assert.Equal(t, uint32(0), i)
			assert.EqualError(t, err, io.EOF.Error())
		})

		t.Run("only one bit in the byte", func(t *testing.T) {
			index := &CheckpointIndex{}
			index.SetActive(1)

			i, err := index.NextActive(1)
			assert.NoError(t, err)
			assert.Equal(t, uint32(1), i)
		})

		t.Run("only one bit in the byte (offset)", func(t *testing.T) {
			index := &CheckpointIndex{}
			index.SetActive(9)

			i, err := index.NextActive(1)
			assert.NoError(t, err)
			assert.Equal(t, uint32(9), i)
		})

		severalSet := &CheckpointIndex{}
		severalSet.SetActive(9)
		severalSet.SetActive(11)

		t.Run("several bits set (first)", func(t *testing.T) {
			i, err := severalSet.NextActive(9)
			assert.NoError(t, err)
			assert.Equal(t, uint32(9), i)
		})

		t.Run("several bits set (second)", func(t *testing.T) {
			i, err := severalSet.NextActive(10)
			assert.NoError(t, err)
			assert.Equal(t, uint32(11), i)
		})

		t.Run("several bits set (second, inclusive)", func(t *testing.T) {
			i, err := severalSet.NextActive(11)
			assert.NoError(t, err)
			assert.Equal(t, uint32(11), i)
		})
	})

	t.Run("many bytes", func(t *testing.T) {
		index := &CheckpointIndex{}
		index.SetActive(9)
		index.SetActive(129)

		// Before the first
		i, err := index.NextActive(8)
		assert.NoError(t, err)
		assert.Equal(t, uint32(9), i)

		// at the first
		i, err = index.NextActive(9)
		assert.NoError(t, err)
		assert.Equal(t, uint32(9), i)

		// In the middle
		i, err = index.NextActive(11)
		assert.NoError(t, err)
		assert.Equal(t, uint32(129), i)

		// At the end
		i, err = index.NextActive(129)
		assert.NoError(t, err)
		assert.Equal(t, uint32(129), i)

		// after the end
		i, err = index.NextActive(130)
		assert.EqualError(t, err, io.EOF.Error())
		assert.Equal(t, uint32(0), i)
	})
}

func TestMaxBitAfter(t *testing.T) {
	for _, tc := range []struct {
		b     byte
		after uint32
		shift uint32
		ok    bool
	}{
		{0b0000_0000, 0, 0, false},
		{0b0000_0000, 1, 0, false},
		{0b1000_0000, 0, 0, true},
		{0b0100_0000, 0, 1, true},
		{0b0100_0000, 1, 1, true},
		{0b0010_1000, 0, 2, true},
		{0b0010_1000, 1, 2, true},
		{0b0010_1000, 2, 2, true},
		{0b0010_1000, 3, 4, true},
		{0b0010_1000, 4, 4, true},
		{0b0000_0001, 7, 7, true},
	} {
		t.Run(fmt.Sprintf("0b%b,%d", tc.b, tc.after), func(t *testing.T) {
			shift, ok := maxBitAfter(tc.b, tc.after)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.shift, shift)
		})
	}
}

func TestMerge(t *testing.T) {
	a := &CheckpointIndex{}
	require.NoError(t, a.SetActive(9))
	require.NoError(t, a.SetActive(129))

	b := &CheckpointIndex{}
	require.NoError(t, b.SetActive(900))
	require.NoError(t, b.SetActive(1000))

	var checkpoints []uint32
	b.iterate(func(c uint32) {
		checkpoints = append(checkpoints, c)
	})

	assert.Equal(t, []uint32{900, 1000}, checkpoints)

	require.NoError(t, a.Merge(b))

	assert.True(t, a.isActive(9))
	assert.True(t, a.isActive(129))
	assert.True(t, a.isActive(900))
	assert.True(t, a.isActive(1000))

	checkpoints = []uint32{}
	a.iterate(func(c uint32) {
		checkpoints = append(checkpoints, c)
	})

	assert.Equal(t, []uint32{9, 129, 900, 1000}, checkpoints)
}

type Uint64Slice []uint64

func (a Uint64Slice) Len() int           { return len(a) }
func (a Uint64Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Uint64Slice) Less(i, j int) bool { return a[i] < a[j] }

func avg(vals []uint64) uint64 {
	var sum uint64
	for v, _ := range vals {
		sum += uint64(v)
	}
	return sum / uint64(len(vals))
}

func BenchmarkBitmapInsertion(b *testing.B) {
	checkpoints := makeCheckpointAccessPattern()
	b.ResetTimer()

	roaringCheckpoints := make(Uint64Slice, len(checkpoints))
	for i := range roaringCheckpoints {
		roaringCheckpoints[i] = uint64(checkpoints[i])
	}

	var roaringSizes []uint64
	b.Run("Roaring", func(bb *testing.B) {
		for trial := 0; trial < bb.N; trial++ {
			sort.Sort(roaringCheckpoints)
			bm := roaring.FromSortedList(roaringCheckpoints)

			// Calculate serialized size
			b.StopTimer()
			roaringSizes = append(roaringSizes, uint64(len(bm.ToBuffer())))
			b.StartTimer()
		}
	})

	fmt.Printf("Roaring avg size: %d\n", avg(roaringSizes))

	var customSizes []uint64
	b.Run("Custom", func(bb *testing.B) {
		for trial := 0; trial < bb.N; trial++ {
			bm := CheckpointIndex{}
			for _, chk := range checkpoints {
				bm.SetActive(chk)
			}

			// Calculate serialized size
			b.StopTimer()
			customSizes = append(customSizes, uint64(len(bm.Flush())))
			b.StartTimer()
		}
	})

	fmt.Printf("Custom avg size: %d\n", avg(customSizes))

}
func BenchmarkBitmapNextActive(b *testing.B) {
	checkpoints := makeCheckpointAccessPattern()

	roar := roaring.NewBitmap()
	paul := CheckpointIndex{}
	for _, chk := range checkpoints {
		roar.Set(uint64(chk))
		paul.SetActive(chk)
	}

	var roarResults []uint64
	var paulResults []uint32

	b.ResetTimer()

	b.Run("Roaring", func(bb *testing.B) {
		for trial := 0; trial < bb.N; trial++ {
			it := roar.NewIterator()
			roarResults = nil
			for {
				n := it.Next()
				if n == 0 {
					break
				}
				roarResults = append(roarResults, n)
			}
		}
	})

	b.Run("Custom", func(bb *testing.B) {
		for trial := 0; trial < bb.N; trial++ {
			n := uint32(0)
			paulResults = nil
			for {
				current, err := paul.NextActive(n)
				if err != nil && err == io.EOF {
					break
				}
				paulResults = append(paulResults, current)
				n = current + 1
			}
		}
	})

	require.Equal(b, len(checkpoints), len(roarResults))
	require.Equal(b, len(checkpoints), len(paulResults))
	for i := range paulResults {
		require.Equal(b, uint64(paulResults[i]), roarResults[i])
	}
}

// Pubnet currently has ~41mln ledgers, so we can use this as an upper bound for
// benchmarking the size of a bitmap.
const MAX_CHECKPOINTS = 41_000_000 / 64

func makeCheckpointAccessPattern() []uint32 {
	// We shouldn't pick *random* ledgers to be active in, because this would
	// completely ruin the purpose of a bitmap. Something more reliable, then
	// would be to pick a random *range* and then activate most ledgers within
	// that range (but also at random).
	//
	// TODO: Actually follow my own advice above. ^

	// We suppose with absolutely no knowledge a prior or justification that 40%
	// "activity" within all checkpoints (and completely random checkpoints, at
	// that) is a reasonable metric.
	const ACTIVITY = int(MAX_CHECKPOINTS * 0.4)

	checkpoints := make([]uint32, ACTIVITY)
	for i, _ := range checkpoints {
		checkpoints[i] = uint32(1 + rand.Int31n(MAX_CHECKPOINTS))
	}

	// We don't want repeats (for performance and also fear of bugs..):
	keys := make(map[uint32]struct{})
	list := []uint32{}
	for _, entry := range checkpoints {
		if _, value := keys[entry]; !value {
			keys[entry] = struct{}{}
			list = append(list, entry)
		}
	}
	return list
}
