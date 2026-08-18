// Package pooler implements the per-shard connection pooler: per-role pools of
// PostgreSQL backends authenticated with forwarded SCRAM keys, a gRPC Execute
// relay that mirrors pgwire messages, generation/epoch fencing, reservations
// for session-bound work, and a two-stage drain.
package pooler
