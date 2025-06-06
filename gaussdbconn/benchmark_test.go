package gaussdbconn_test

import (
	"bytes"
	"context"
	"os"
	"testing"

	gaussdbgo "github.com/HuaweiCloudDeveloper/gaussdb-go"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbconn"
	"github.com/stretchr/testify/require"
)

func BenchmarkConnect(b *testing.B) {
	benchmarks := []struct {
		name string
		env  string
	}{
		// todo: unix socket to improve
		//{"Unix socket", "GAUSSDB_TEST_UNIX_SOCKET_CONN_STRING"},
		{"TCP", gaussdbgo.EnvGaussdbTestTcpConnString},
	}

	for _, bm := range benchmarks {
		bm := bm
		b.Run(bm.name, func(b *testing.B) {
			connString := os.Getenv(bm.env)
			if connString == "" {
				b.Skipf("Skipping due to missing environment variable %v", bm.env)
			}

			for i := 0; i < b.N; i++ {
				conn, err := gaussdbconn.Connect(context.Background(), connString)
				require.Nil(b, err)

				err = conn.Close(context.Background())
				require.Nil(b, err)
			}
		})
	}
}

func BenchmarkExec(b *testing.B) {
	expectedValues := [][]byte{[]byte("hello"), []byte("42"), []byte("2019-01-01")}
	benchmarks := []struct {
		name string
		ctx  context.Context
	}{
		// Using an empty context other than context.Background() to compare
		// performance
		{"background context", context.Background()},
		{"empty context", context.TODO()},
	}

	for _, bm := range benchmarks {
		bm := bm
		b.Run(bm.name, func(b *testing.B) {
			conn, err := gaussdbconn.Connect(bm.ctx, os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
			require.Nil(b, err)
			defer closeConn(b, conn)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				mrr := conn.Exec(bm.ctx, "select 'hello'::text as a, 42::int4 as b, '2019-01-01'::date")

				for mrr.NextResult() {
					rr := mrr.ResultReader()

					rowCount := 0
					for rr.NextRow() {
						rowCount++
						if len(rr.Values()) != len(expectedValues) {
							b.Fatalf("unexpected number of values: %d", len(rr.Values()))
						}
						for i := range rr.Values() {
							if !bytes.Equal(rr.Values()[i], expectedValues[i]) {
								b.Fatalf("unexpected values: %s %s", rr.Values()[i], expectedValues[i])
							}
						}
					}
					_, err = rr.Close()

					if err != nil {
						b.Fatal(err)
					}
					if rowCount != 1 {
						b.Fatalf("unexpected rowCount: %d", rowCount)
					}
				}

				err := mrr.Close()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkExecPossibleToCancel(b *testing.B) {
	conn, err := gaussdbconn.Connect(context.Background(), os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
	require.Nil(b, err)
	defer closeConn(b, conn)

	expectedValues := [][]byte{[]byte("hello"), []byte("42"), []byte("2019-01-01")}

	b.ResetTimer()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < b.N; i++ {
		mrr := conn.Exec(ctx, "select 'hello'::text as a, 42::int4 as b, '2019-01-01'::date")

		for mrr.NextResult() {
			rr := mrr.ResultReader()

			rowCount := 0
			for rr.NextRow() {
				rowCount++
				if len(rr.Values()) != len(expectedValues) {
					b.Fatalf("unexpected number of values: %d", len(rr.Values()))
				}
				for i := range rr.Values() {
					if !bytes.Equal(rr.Values()[i], expectedValues[i]) {
						b.Fatalf("unexpected values: %s %s", rr.Values()[i], expectedValues[i])
					}
				}
			}
			_, err = rr.Close()

			if err != nil {
				b.Fatal(err)
			}
			if rowCount != 1 {
				b.Fatalf("unexpected rowCount: %d", rowCount)
			}
		}

		err := mrr.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExecPrepared(b *testing.B) {
	expectedValues := [][]byte{[]byte("hello"), []byte("42"), []byte("2019-01-01")}

	benchmarks := []struct {
		name string
		ctx  context.Context
	}{
		// Using an empty context other than context.Background() to compare
		// performance
		{"background context", context.Background()},
		{"empty context", context.TODO()},
	}

	for _, bm := range benchmarks {
		bm := bm
		b.Run(bm.name, func(b *testing.B) {
			conn, err := gaussdbconn.Connect(bm.ctx, os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
			require.Nil(b, err)
			defer closeConn(b, conn)

			_, err = conn.Prepare(bm.ctx, "ps1", "select 'hello'::text as a, 42::int4 as b, '2019-01-01'::date", nil)
			require.Nil(b, err)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				rr := conn.ExecPrepared(bm.ctx, "ps1", nil, nil, nil)

				rowCount := 0
				for rr.NextRow() {
					rowCount++
					if len(rr.Values()) != len(expectedValues) {
						b.Fatalf("unexpected number of values: %d", len(rr.Values()))
					}
					for i := range rr.Values() {
						if !bytes.Equal(rr.Values()[i], expectedValues[i]) {
							b.Fatalf("unexpected values: %s %s", rr.Values()[i], expectedValues[i])
						}
					}
				}
				_, err = rr.Close()

				if err != nil {
					b.Fatal(err)
				}
				if rowCount != 1 {
					b.Fatalf("unexpected rowCount: %d", rowCount)
				}
			}
		})
	}
}

func BenchmarkExecPreparedPossibleToCancel(b *testing.B) {
	conn, err := gaussdbconn.Connect(context.Background(), os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
	require.Nil(b, err)
	defer closeConn(b, conn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err = conn.Prepare(ctx, "ps1", "select 'hello'::text as a, 42::int4 as b, '2019-01-01'::date", nil)
	require.Nil(b, err)

	expectedValues := [][]byte{[]byte("hello"), []byte("42"), []byte("2019-01-01")}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rr := conn.ExecPrepared(ctx, "ps1", nil, nil, nil)

		rowCount := 0
		for rr.NextRow() {
			rowCount += 1
			if len(rr.Values()) != len(expectedValues) {
				b.Fatalf("unexpected number of values: %d", len(rr.Values()))
			}
			for i := range rr.Values() {
				if !bytes.Equal(rr.Values()[i], expectedValues[i]) {
					b.Fatalf("unexpected values: %s %s", rr.Values()[i], expectedValues[i])
				}
			}
		}
		_, err = rr.Close()

		if err != nil {
			b.Fatal(err)
		}
		if rowCount != 1 {
			b.Fatalf("unexpected rowCount: %d", rowCount)
		}
	}
}

// func BenchmarkChanToSetDeadlinePossibleToCancel(b *testing.B) {
// 	conn, err := gaussdbconn.Connect(context.Background(), os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
// 	require.Nil(b, err)
// 	defer closeConn(b, conn)

// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()

// 	b.ResetTimer()

// 	for i := 0; i < b.N; i++ {
// 		conn.ChanToSetDeadline().Watch(ctx)
// 		conn.ChanToSetDeadline().Ignore()
// 	}
// }
