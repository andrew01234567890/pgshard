// Package router turns authenticated pgwire sessions into pooler calls: it
// authenticates clients with SCRAM verifiers from the catalog, plans each
// statement onto a shard (single-shard only for unsharded databases today),
// relays wire messages to that shard's pooler and replays logical session
// state (SET, prepared statements) whenever the pooled backend changes.
package router
