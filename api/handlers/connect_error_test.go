package handlers

import (
	"errors"
	"strings"
	"testing"
)

func TestFriendlyConnectErrorOracle(t *testing.T) {
	cases := map[string]string{
		"ORA-01017: invalid credential or not authorized; logon denied":                              "username and password",
		"ORA-12514: TNS:listener does not currently know of service requested in connect descriptor": "service name",
		"ORA-28000: The account is locked.":                                                          "ACCOUNT UNLOCK",
		"ORA-01045: Login denied. User XFERDB lacks CREATE SESSION privilege":                        "CREATE SESSION",
	}
	for msg, want := range cases {
		if got := friendlyConnectError(errors.New(msg)); !strings.Contains(got, want) {
			t.Errorf("friendlyConnectError(%q) = %q, want a hint about %q", msg, got, want)
		}
	}
}
