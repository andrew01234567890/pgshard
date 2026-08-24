package pgparser

// EffectiveMajor is the grammar major a router must target while shard
// groups of several majors serve, e.g. during a rolling major upgrade: the
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
