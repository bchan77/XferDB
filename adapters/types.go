package adapters

// TypePostgres and TypeYugabyte are the adapter types that speak
// PostgreSQL's SQL and wire protocol. YugabyteDB (its YSQL API) runs the
// postgres adapter, with its own behaviour where the two differ: logical
// replication and the live migration checks (#158).
const (
	TypePostgres = "postgres"
	TypeYugabyte = "yugabyte"
)

// PostgresFamily reports whether an adapter type is PostgreSQL or a
// database that speaks its SQL (YugabyteDB): anything that behaves like a
// postgres source or target, such as listing or creating databases, or
// converting values when it stores them.
func PostgresFamily(adapterType string) bool {
	return adapterType == TypePostgres || adapterType == TypeYugabyte
}
