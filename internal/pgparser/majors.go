package pgparser

// EffectiveMajor is the grammar major a router must target while shard
// groups of several majors serve.
//
// The rule: during a rolling major upgrade it is the lowest still-present
// major, so no statement is accepted that an old-major group would refuse.
// It flips to the new major only once every group runs it. Non-positive
// entries (unknown majors) are ignored; with no known major the bound
// grammar's own major is the answer.
//
// What calls it is router.ServerVersion, which passes the majors of the
// live shard sets together with this router's own grammar major, so the
// version a client is told is the surface it will actually get. What still
// does not exist is the other half: the router binds one grammar at build
// time (internal/pgparser/pg18) and cannot swap it, so a cluster fully on a
// later major is still parsed -- and still described -- by the earlier one.
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
