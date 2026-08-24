// Package upgrade holds the kind end-to-end suite for online 18 -> 19
// major upgrades: the blue/green shard-set replacement under a ledger
// workload, rollback before retirement, the catalog-group flip behind the
// stable catalog Service, and a chaos variant that kills the controller
// and the promoted primary mid-upgrade.
// Run with: go test -tags e2e -count=1 -v ./test/e2e/upgrade/...
// Both the pg18 and pg19 images must be loaded into the kind cluster.
package upgrade
