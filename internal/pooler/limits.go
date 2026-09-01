package pooler

// MaxMessageBytes is the largest protobuf message the router and pooler
// exchange, and so the largest a single Bind value, DataRow or COPY chunk
// may be once encoded.
//
// It is 4 MiB because that is grpc-go's default and therefore what pgshard
// has always enforced -- silently, since nothing set it. Naming it changes
// no behaviour; it makes the boundary a decision that can be found, tested
// and reported instead of a dependency's default surfacing as a lost
// connection.
//
// PostgreSQL itself accepts protocol messages up to 1 GiB, so this is a
// real narrowing of the contract and is documented as such. Raising it is
// not a matter of changing this number: row channels and writer flushes
// are bounded by item count, not bytes, so a larger limit would let a
// handful of rows hold hundreds of megabytes in the router. Byte-weighted
// admission comes first -- see PGS-499.
const MaxMessageBytes = 4 << 20
