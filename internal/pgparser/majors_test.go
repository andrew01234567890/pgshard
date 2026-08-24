package pgparser

import "testing"

func TestEffectiveMajor(t *testing.T) {
	cases := []struct {
		name    string
		present []int
		want    int
	}{
		{"mixed rollout parses with the lowest major", []int{19, 18}, 18},
		{"all upgraded flips to the new major", []int{19, 19}, 19},
		{"single major", []int{18}, 18},
		{"unknown majors ignored", []int{0, -1, 19}, 19},
		{"no known major falls back to the bound grammar", nil, Major},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := EffectiveMajor(tc.present); got != tc.want {
				t.Fatalf("got %d want %d", got, tc.want)
			}
		})
	}
}
