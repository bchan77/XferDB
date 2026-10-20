package mongo

import "testing"

// TestDatabaseFromURI covers URIs url.Parse rejects but the driver accepts:
// a backslash in the password (a shell escape pasted into the web form) used
// to lose the database and surface as "database name cannot be empty".
func TestDatabaseFromURI(t *testing.T) {
	for dsn, want := range map[string]string{
		`mongodb://mongo:Yugabyte123\!@192.168.21.133:27017/shop`:               "shop",
		"mongodb://mongo:Yugabyte123!@192.168.21.133:27017/shop":                "shop",
		"mongodb://u:p%40ss@h1:27017,h2:27017/mydb?replicaSet=rs0&authSource=x": "mydb",
		"mongodb+srv://u:p@cluster.example.net/app?retryWrites=true":            "app",
		"mongodb://u:p@w/rd@host:27017/db":                                      "db",
		"mongodb://host:27017/my%20db":                                          "my db",
		"mongodb://host:27017":                                                  "",
		"mongodb://host:27017/":                                                 "",
		"mongodb://host:27017/?tls=true":                                        "",
		"not a uri":                                                             "",
	} {
		if got := DatabaseFromURI(dsn); got != want {
			t.Errorf("DatabaseFromURI(%q) = %q, want %q", dsn, got, want)
		}
	}
}
