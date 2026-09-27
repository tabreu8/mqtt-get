package topic

import (
	"sort"
	"testing"
)

var matchCases = []struct {
	filter, name string
	want         bool
}{
	{"a/b/c", "a/b/c", true},
	{"a/b/c", "a/b", false},
	{"a/b", "a/b/c", false},
	{"a/+/c", "a/x/c", true},
	{"a/+/c", "a/x/y/c", false},
	{"a/+", "a", false},
	{"a/+", "a/", true},
	{"+/+", "/x", true},
	{"#", "a/b/c", true},
	{"#", "/", true},
	{"a/#", "a", true},
	{"a/#", "a/b/c", true},
	{"a/#", "ab", false},
	{"+/b/#", "x/b", true},
	{"+/b/#", "x/c", false},
	{"#", "$SYS/foo", false},
	{"+/foo", "$SYS/foo", false},
	{"$SYS/#", "$SYS/foo", true},
	{"$SYS/+", "$SYS/foo", true},
	{"sport/tennis/+", "sport/tennis/player1", true},
	{"sport/tennis/+", "sport/tennis/player1/ranking", false},
}

func TestMatch(t *testing.T) {
	for _, c := range matchCases {
		if got := Match(c.filter, c.name); got != c.want {
			t.Errorf("Match(%q, %q) = %v, want %v", c.filter, c.name, got, c.want)
		}
	}
}

func TestTrieAgreesWithMatch(t *testing.T) {
	for _, c := range matchCases {
		tr := NewTrie[int]()
		tr.Insert(c.filter, 1)
		got := len(tr.Match(c.name, nil)) == 1
		if got != c.want {
			t.Errorf("Trie(%q).Match(%q) = %v, want %v", c.filter, c.name, got, c.want)
		}
	}
}

func TestTrieMulti(t *testing.T) {
	tr := NewTrie[string]()
	tr.Insert("a/b", "exact")
	tr.Insert("a/+", "plus")
	tr.Insert("a/#", "hash")
	tr.Insert("#", "all")
	tr.Insert("b/#", "other")
	tr.Insert("a/+", "exact") // same value via second filter: deduped
	got := tr.Match("a/b", nil)
	sort.Strings(got)
	want := []string{"all", "exact", "hash", "plus"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

func TestValidate(t *testing.T) {
	good := []string{"a", "a/b", "#", "a/#", "+", "+/+/c", "/", "$SYS/#"}
	bad := []string{"", "a/#/b", "a#", "a/b+", "+a"}
	for _, f := range good {
		if err := ValidateFilter(f); err != nil {
			t.Errorf("ValidateFilter(%q) = %v", f, err)
		}
	}
	for _, f := range bad {
		if err := ValidateFilter(f); err == nil {
			t.Errorf("ValidateFilter(%q) = nil, want error", f)
		}
	}
	if ValidateName("a/+") == nil || ValidateName("") == nil || ValidateName("a/b") != nil {
		t.Error("ValidateName")
	}
}

func BenchmarkMatch(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Match("home/+/sensors/#", "home/kitchen/sensors/temp/1")
	}
}

func BenchmarkTrieMatch(b *testing.B) {
	tr := NewTrie[int]()
	for i := 0; i < 1000; i++ {
		tr.Insert("devices/"+string(rune('a'+i%26))+"/+/state", i)
	}
	tr.Insert("devices/#", -1)
	buf := make([]int, 0, 16)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = tr.Match("devices/k/42/state", buf[:0])
	}
}
