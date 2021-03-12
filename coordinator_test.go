package distpow

import (
	"testing"
)

func TestCoordBytesCompare(t *testing.T) {
	type test struct {
		secret1  []uint8
		secret2  []uint8
		expected bool
		name     string
	}
	tests := []test{
		{
			secret1:  []uint8{192, 168, 1, 1},
			secret2:  []uint8{1, 168, 1, 2},
			expected: true,
			name:     "TestCoordBytesCompare1",
		},
		{
			secret1:  []uint8{1, 168, 1, 2},
			secret2:  []uint8{192, 168, 1, 1},
			expected: false,
			name:     "TestCoordBytesCompare2",
		},
		{
			secret2:  []uint8{192, 168, 1, 1},
			secret1:  []uint8{192, 168, 1, 1},
			expected: true,
			name:     "TestCoordBytesCompare3",
		},
		{
			secret1:  []uint8{192, 168, 1, 1},
			secret2:  []uint8{1, 168, 1, 1, 1},
			expected: true,
			name:     "TestCoordBytesCompare4",
		},
		{
			secret1:  []uint8{192, 168, 1, 1},
			secret2:  []uint8{192, 168, 1},
			expected: true,
			name:     "TestCoordBytesCompare5",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if dominates(test.secret1, test.secret2) != test.expected {
				t.Fatalf("unexpected")
			}
		})
	}
}
