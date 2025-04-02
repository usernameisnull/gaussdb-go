package zeronull_test

import (
	"context"
	"os"
	"testing"

	"github.com/HuaweiCloudDeveloper/gaussdb-go"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/pgtype/zeronull"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/pgxtest"
	"github.com/stretchr/testify/require"
)

var defaultConnTestRunner pgxtest.ConnTestRunner

func init() {
	defaultConnTestRunner = pgxtest.DefaultConnTestRunner()
	defaultConnTestRunner.CreateConfig = func(ctx context.Context, t testing.TB) *pgx.ConnConfig {
		config, err := pgx.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
		require.NoError(t, err)
		return config
	}
	defaultConnTestRunner.AfterConnect = func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		zeronull.Register(conn.TypeMap())
	}
}
