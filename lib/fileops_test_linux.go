package pitreos

import (
	"fmt"
	"testing"

	fibmap "github.com/frostschutz/go-fibmap"
	"github.com/stretchr/testify/assert"
)

func TestSparseExtents(t *testing.T) {
	tests := []struct {
		in     []fibmap.Extent
		start  int64
		end    int64
		output bool
	}{
		{
			[]fibmap.Extent{
				fibmap.Extent{
					Logical: 0,
					Length:  60,
				},
				fibmap.Extent{
					Logical: 860,
					Length:  940,
				},
				fibmap.Extent{
					Logical: 2000,
					Length:  5000000, // 5.0002MB
				},
			},
			0,
			250000000,
			true,
		},
		{
			[]fibmap.Extent{
				fibmap.Extent{
					Logical: 0,
					Length:  60,
				},
				fibmap.Extent{
					Logical: 860,
					Length:  940,
				},
				fibmap.Extent{
					Logical: 2000,
					Length:  5000000, // 5.0002MB
				},
			},
			100,
			20,
			false,
		},
		{
			[]fibmap.Extent{
				fibmap.Extent{
					Logical: 20,
					Length:  60,
				},
			},
			0,
			20,
			false,
		},
		{
			[]fibmap.Extent{
				fibmap.Extent{
					Logical: 20,
					Length:  60,
				},
			},
			0,
			20,
			false,
		},
		{
			[]fibmap.Extent{
				fibmap.Extent{
					Logical: 20,
					Length:  60,
				},
			},
			20,
			1,
			true,
		},
		{
			[]fibmap.Extent{
				fibmap.Extent{
					Logical: 0,
					Length:  20,
				},
			},
			20,
			1,
			false,
		},
	}

	for idx, test := range tests {
		fo := &FileOps{
			extentsLoaded: true,
			extents:       test.in,
		}
		res := fo.hasDataInRange(test.start, test.end)
		assert.Equal(t, test.output, res, fmt.Sprintf("test %d", idx))
	}
}
