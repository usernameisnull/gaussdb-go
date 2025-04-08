package gaussdbgo_test

import (
	"context"
	"os"
	"testing"

	"github.com/HuaweiCloudDeveloper/gaussdb-go"
	_ "github.com/HuaweiCloudDeveloper/gaussdb-go/stdlib"
)

func skipCockroachDB(t testing.TB, msg string) {
	conn, err := gaussdbgo.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())

	if conn.GaussdbConn().ParameterStatus("crdb_version") != "" {
		t.Skip(msg)
	}
}
