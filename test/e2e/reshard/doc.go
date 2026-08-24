// Package reshard holds the kind end-to-end suite for resharding: target
// group provisioning, non-serving isolation, cancellation, and table
// placement workflows (shard key changes).
// Run with: go test -tags e2e -count=1 -v ./test/e2e/reshard/...
package reshard
