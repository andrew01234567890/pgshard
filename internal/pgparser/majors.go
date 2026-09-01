package pgparser

// EffectiveMajor is the grammar major a router must target while shard
// groups of several majors serve.
//
// Nothing calls it yet: the router binds one grammar at build time
// (internal/pgparser/pg18) and has no way to swap it per statement, so this
// is the rule waiting for the mechanism rather than a switch that is wired.
// A reader of docs/upgrade.md should not take the reference there as a
// description of what runs today.
//
// The rule itself: during a rolling major upgrade it is the
// lowest still-present major, so no statement is accepted that an old-major
// group would refuse. It flips to the new major only once every group runs
// it. Non-positive entries (unknown majors) are ignored; with no known
// major the bound grammar's own major is the answer.
func EffectiveMajor(present []int) int {
	effective := 0
	for _, m := range present {
		if m > 0 && (effective == 0 || m < effective) {
			effective = m
		}
	}
	if effective == 0 {
		return Major
	}
	return effective
}
