package config

import "strings"

// overlapping returns the first pair of filters that can match the same
// topic name.
func overlapping(subs []Subscription) (string, string, bool) {
	for i := range subs {
		for j := i + 1; j < len(subs); j++ {
			if FiltersOverlap(subs[i].Filter, subs[j].Filter) {
				return subs[i].Filter, subs[j].Filter, true
			}
		}
	}
	return "", "", false
}

// FiltersOverlap reports whether some topic name matches both filters.
func FiltersOverlap(a, b string) bool {
	// Wildcards in the first level never match topics starting with '$'.
	if dollar(a) != dollar(b) {
		fa, fb := first(a), first(b)
		if (dollar(a) && (fb == "+" || fb == "#")) || (dollar(b) && (fa == "+" || fa == "#")) {
			return false
		}
	}
	la, lb := strings.Split(a, "/"), strings.Split(b, "/")
	for i := 0; ; i++ {
		switch {
		case i < len(la) && la[i] == "#", i < len(lb) && lb[i] == "#":
			return true
		case i == len(la) && i == len(lb):
			return true
		case i == len(la) || i == len(lb):
			// One filter ended; "a" and "a/#" overlap on "a" (handled
			// above only if the longer one has '#' next).
			longer := la
			if i == len(la) {
				longer = lb
			}
			return len(longer) == i+1 && longer[i] == "#"
		case la[i] == "+" || lb[i] == "+" || la[i] == lb[i]:
			continue
		default:
			return false
		}
	}
}

func dollar(f string) bool { return strings.HasPrefix(f, "$") }

func first(f string) string {
	l, _, _ := strings.Cut(f, "/")
	return l
}
