// Package pgwire implements the server side of the PostgreSQL frontend/backend
// protocol (versions 3.0 and 3.2) for the pgshard router. It owns connection
// startup, authentication, the simple and extended query message loops and
// graceful drain; query execution is delegated to an Executor.
package pgwire
