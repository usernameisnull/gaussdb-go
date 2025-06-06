package stdlib_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/HuaweiCloudDeveloper/gaussdb-go"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbconn"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbtype"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbxpool"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/stdlib"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/tracelog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openDB(t testing.TB) *sql.DB {
	config, err := gaussdbgo.ParseConfig(os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
	require.NoError(t, err)
	return stdlib.OpenDB(*config)
}

func closeDB(t testing.TB, db *sql.DB) {
	err := db.Close()
	require.NoError(t, err)
}

func testWithAllQueryExecModes(t *testing.T, f func(t *testing.T, db *sql.DB)) {
	for _, mode := range []gaussdbgo.QueryExecMode{
		gaussdbgo.QueryExecModeCacheStatement,
		gaussdbgo.QueryExecModeCacheDescribe,
		gaussdbgo.QueryExecModeDescribeExec,
		gaussdbgo.QueryExecModeExec,
		gaussdbgo.QueryExecModeSimpleProtocol,
	} {
		t.Run(mode.String(),
			func(t *testing.T) {
				config, err := gaussdbgo.ParseConfig(os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
				require.NoError(t, err)

				config.DefaultQueryExecMode = mode
				db := stdlib.OpenDB(*config)
				defer func() {
					err := db.Close()
					require.NoError(t, err)
				}()

				f(t, db)

				ensureDBValid(t, db)
			},
		)
	}
}

// Do a simple query to ensure the DB is still usable. This is of less use in stdlib as the connection pool should
// cover broken connections.
func ensureDBValid(t testing.TB, db *sql.DB) {
	var sum, rowCount int32

	rows, err := db.Query("select generate_series(1,$1)", 10)
	require.NoError(t, err)
	defer rows.Close()

	for rows.Next() {
		var n int32
		rows.Scan(&n)
		sum += n
		rowCount++
	}

	require.NoError(t, rows.Err())

	if rowCount != 10 {
		t.Error("Select called onDataRow wrong number of times")
	}
	if sum != 55 {
		t.Error("Wrong values returned")
	}
}

type preparer interface {
	Prepare(query string) (*sql.Stmt, error)
}

func prepareStmt(t *testing.T, p preparer, sql string) *sql.Stmt {
	stmt, err := p.Prepare(sql)
	require.NoError(t, err)
	return stmt
}

func closeStmt(t *testing.T, stmt *sql.Stmt) {
	err := stmt.Close()
	require.NoError(t, err)
}

func TestSQLOpen(t *testing.T) {
	tests := []struct {
		driverName string
	}{
		{driverName: "gaussdb"},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.driverName, func(t *testing.T) {
			db, err := sql.Open(tt.driverName, os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
			require.NoError(t, err)
			closeDB(t, db)
		})
	}
}

func TestSQLOpenFromPool(t *testing.T) {
	pool, err := gaussdbxpool.New(context.Background(), os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	db := stdlib.OpenDBFromPool(pool)
	ensureDBValid(t, db)

	db.Close()
}

func TestNormalLifeCycle(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	stmt := prepareStmt(t, db, "select 'foo', n from generate_series($1::int, $2::int) n")
	defer closeStmt(t, stmt)

	rows, err := stmt.Query(int32(1), int32(10))
	require.NoError(t, err)

	rowCount := int64(0)

	for rows.Next() {
		rowCount++

		var s string
		var n int64
		err := rows.Scan(&s, &n)
		require.NoError(t, err)

		if s != "foo" {
			t.Errorf(`Expected "foo", received "%v"`, s)
		}
		if n != rowCount {
			t.Errorf("Expected %d, received %d", rowCount, n)
		}
	}
	require.NoError(t, rows.Err())

	require.EqualValues(t, 10, rowCount)

	err = rows.Close()
	require.NoError(t, err)

	ensureDBValid(t, db)
}

func TestStmtExec(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	tx, err := db.Begin()
	require.NoError(t, err)

	createStmt := prepareStmt(t, tx, "create temporary table t(a varchar not null)")
	_, err = createStmt.Exec()
	require.NoError(t, err)
	closeStmt(t, createStmt)

	insertStmt := prepareStmt(t, tx, "insert into t values($1::text)")
	result, err := insertStmt.Exec("foo")
	require.NoError(t, err)

	n, err := result.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
	closeStmt(t, insertStmt)

	ensureDBValid(t, db)
}

func TestQueryCloseRowsEarly(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	stmt := prepareStmt(t, db, "select 'foo', n from generate_series($1::int, $2::int) n")
	defer closeStmt(t, stmt)

	rows, err := stmt.Query(int32(1), int32(10))
	require.NoError(t, err)

	// Close rows immediately without having read them
	err = rows.Close()
	require.NoError(t, err)

	// Run the query again to ensure the connection and statement are still ok
	rows, err = stmt.Query(int32(1), int32(10))
	require.NoError(t, err)

	rowCount := int64(0)

	for rows.Next() {
		rowCount++

		var s string
		var n int64
		err := rows.Scan(&s, &n)
		require.NoError(t, err)
		if s != "foo" {
			t.Errorf(`Expected "foo", received "%v"`, s)
		}
		if n != rowCount {
			t.Errorf("Expected %d, received %d", rowCount, n)
		}
	}
	require.NoError(t, rows.Err())
	require.EqualValues(t, 10, rowCount)

	err = rows.Close()
	require.NoError(t, err)

	ensureDBValid(t, db)
}

func TestConnExec(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		_, err := db.Exec("create temporary table t(a varchar not null)")
		require.NoError(t, err)

		result, err := db.Exec("insert into t values('hey')")
		require.NoError(t, err)

		n, err := result.RowsAffected()
		require.NoError(t, err)
		require.EqualValues(t, 1, n)
	})
}

func TestConnQuery(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		rows, err := db.Query("select 'foo', n from generate_series($1::int, $2::int) n", int32(1), int32(10))
		require.NoError(t, err)

		rowCount := int64(0)

		for rows.Next() {
			rowCount++

			var s string
			var n int64
			err := rows.Scan(&s, &n)
			require.NoError(t, err)
			if s != "foo" {
				t.Errorf(`Expected "foo", received "%v"`, s)
			}
			if n != rowCount {
				t.Errorf("Expected %d, received %d", rowCount, n)
			}
		}
		require.NoError(t, rows.Err())
		require.EqualValues(t, 10, rowCount)

		err = rows.Close()
		require.NoError(t, err)
	})
}

func TestConnConcurrency(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		_, err := db.Exec("create table t (id integer primary key, str text, dur_str interval)")
		require.NoError(t, err)

		defer func() {
			_, err := db.Exec("drop table t")
			require.NoError(t, err)
		}()

		var wg sync.WaitGroup

		concurrency := 50
		errChan := make(chan error, concurrency)

		for i := 1; i <= concurrency; i++ {
			wg.Add(1)

			go func(idx int) {
				defer wg.Done()

				ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
				defer cancel()

				str := strconv.Itoa(idx)
				duration := time.Duration(idx) * time.Second
				_, err := db.ExecContext(ctx, "insert into t values($1)", idx)
				if err != nil {
					errChan <- fmt.Errorf("insert failed: %d %w", idx, err)
					return
				}
				_, err = db.ExecContext(ctx, "update t set str = $1 where id = $2", str, idx)
				if err != nil {
					errChan <- fmt.Errorf("update 1 failed: %d %w", idx, err)
					return
				}
				_, err = db.ExecContext(ctx, "update t set dur_str = $1 where id = $2", duration, idx)
				if err != nil {
					errChan <- fmt.Errorf("update 2 failed: %d %w", idx, err)
					return
				}

				errChan <- nil
			}(i)
		}
		wg.Wait()
		for i := 1; i <= concurrency; i++ {
			err := <-errChan
			require.NoError(t, err)
		}

		for i := 1; i <= concurrency; i++ {
			wg.Add(1)

			go func(idx int) {
				defer wg.Done()

				ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
				defer cancel()

				var id int
				var str string
				var duration gaussdbtype.Interval
				err := db.QueryRowContext(ctx, "select id,str,dur_str from t where id = $1", idx).Scan(&id, &str, &duration)
				if err != nil {
					errChan <- fmt.Errorf("select failed: %d %w", idx, err)
					return
				}
				if id != idx {
					errChan <- fmt.Errorf("id mismatch: %d %d", idx, id)
					return
				}
				if str != strconv.Itoa(idx) {
					errChan <- fmt.Errorf("str mismatch: %d %s", idx, str)
					return
				}
				expectedDuration := gaussdbtype.Interval{
					Microseconds: int64(idx) * time.Second.Microseconds(),
					Valid:        true,
				}
				if duration != expectedDuration {
					errChan <- fmt.Errorf("duration mismatch: %d %v", idx, duration)
					return
				}

				errChan <- nil
			}(i)
		}
		wg.Wait()
		for i := 1; i <= concurrency; i++ {
			err := <-errChan
			require.NoError(t, err)
		}
	})
}

func TestConnQueryDifferentScanPlansIssue781(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		var s string
		var b bool

		rows, err := db.Query("select true, 'foo'")
		require.NoError(t, err)

		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&b, &s))
		assert.Equal(t, true, b)
		assert.Equal(t, "foo", s)
	})
}

func TestConnQueryNull(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		rows, err := db.Query("select $1::int", nil)
		require.NoError(t, err)

		rowCount := int64(0)

		for rows.Next() {
			rowCount++

			var n sql.NullInt64
			err := rows.Scan(&n)
			require.NoError(t, err)
			if n.Valid != false {
				t.Errorf("Expected n to be null, but it was %v", n)
			}
		}
		require.NoError(t, rows.Err())
		require.EqualValues(t, 1, rowCount)

		err = rows.Close()
		require.NoError(t, err)
	})
}

func TestConnQueryRowByteSlice(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		expected := []byte{222, 173, 190, 239}
		var actual []byte

		err := db.QueryRow(`select E'\\xdeadbeef'::bytea`).Scan(&actual)
		require.NoError(t, err)
		require.EqualValues(t, expected, actual)
	})
}

func TestConnQueryFailure(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		_, err := db.Query("select 'foo")
		require.Error(t, err)
		require.IsType(t, new(gaussdbconn.GaussdbError), err)
	})
}

func TestConnSimpleSlicePassThrough(t *testing.T) {
	if gaussdbgo.IsTestingWithOpengauss() {
		t.Skip("skip opengauss, no 'cardinality' function")
	}
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		var n int64
		err := db.QueryRow("select cardinality($1::text[])", []string{"a", "b", "c"}).Scan(&n)
		require.NoError(t, err)
		assert.EqualValues(t, 3, n)
	})
}

func TestConnQueryScanGoArray(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		m := gaussdbtype.NewMap()

		var a []int64
		err := db.QueryRow("select '{1,2,3}'::bigint[]").Scan(m.SQLScanner(&a))
		require.NoError(t, err)
		assert.Equal(t, []int64{1, 2, 3}, a)
	})
}

func TestConnQueryScanArray(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		m := gaussdbtype.NewMap()

		var a gaussdbtype.Array[int64]
		err := db.QueryRow("select '{1,2,3}'::bigint[]").Scan(m.SQLScanner(&a))
		require.NoError(t, err)
		assert.Equal(t, gaussdbtype.Array[int64]{Elements: []int64{1, 2, 3}, Dims: []gaussdbtype.ArrayDimension{{Length: 3, LowerBound: 1}}, Valid: true}, a)

		err = db.QueryRow("select null::bigint[]").Scan(m.SQLScanner(&a))
		require.NoError(t, err)
		assert.Equal(t, gaussdbtype.Array[int64]{Elements: nil, Dims: nil, Valid: false}, a)
	})
}

func TestConnQueryScanRange(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		m := gaussdbtype.NewMap()

		var r gaussdbtype.Range[gaussdbtype.Int4]
		err := db.QueryRow("select int4range(1, 5)").Scan(m.SQLScanner(&r))
		require.NoError(t, err)
		assert.Equal(
			t,
			gaussdbtype.Range[gaussdbtype.Int4]{
				Lower:     gaussdbtype.Int4{Int32: 1, Valid: true},
				Upper:     gaussdbtype.Int4{Int32: 5, Valid: true},
				LowerType: gaussdbtype.Inclusive,
				UpperType: gaussdbtype.Exclusive,
				Valid:     true,
			},
			r)
	})
}

// Test type that gaussdbgo would handle natively in binary, but since it is not a
// database/sql native type should be passed through as a string
func TestConnQueryRowGaussdbBinary(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		sql := "select $1::int4[]"
		expected := "{1,2,3}"
		var actual string

		err := db.QueryRow(sql, expected).Scan(&actual)
		require.NoError(t, err)
		require.EqualValues(t, expected, actual)
	})
}

func TestConnQueryRowUnknownType(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		sql := "select $1::point"
		expected := "(1,2)"
		var actual string

		err := db.QueryRow(sql, expected).Scan(&actual)
		require.NoError(t, err)
		require.EqualValues(t, expected, actual)
	})
}

func TestConnQueryJSONIntoByteSlice(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		_, err := db.Exec(`
		create temporary table docs(
			body json not null
		);

		insert into docs(body) values('{"foo": "bar"}');
`)
		require.NoError(t, err)

		sql := `select * from docs`
		expected := []byte(`{"foo": "bar"}`)
		var actual []byte

		err = db.QueryRow(sql).Scan(&actual)
		if err != nil {
			t.Errorf("Unexpected failure: %v (sql -> %v)", err, sql)
		}

		if !bytes.Equal(actual, expected) {
			t.Errorf(`Expected "%v", got "%v" (sql -> %v)`, string(expected), string(actual), sql)
		}

		_, err = db.Exec(`drop table docs`)
		require.NoError(t, err)
	})
}

func TestConnExecInsertByteSliceIntoJSON(t *testing.T) {
	// Not testing with simple protocol because there is no way for that to work. A []byte will be considered binary data
	// that needs to escape. No way to know whether the destination is really a text compatible or a bytea.

	db := openDB(t)
	defer closeDB(t, db)

	_, err := db.Exec(`
		create temporary table docs(
			body json not null
		);
`)
	require.NoError(t, err)

	expected := []byte(`{"foo": "bar"}`)

	_, err = db.Exec(`insert into docs(body) values($1)`, expected)
	require.NoError(t, err)

	var actual []byte
	err = db.QueryRow(`select body from docs`).Scan(&actual)
	require.NoError(t, err)

	if !bytes.Equal(actual, expected) {
		t.Errorf(`Expected "%v", got "%v"`, string(expected), string(actual))
	}

	_, err = db.Exec(`drop table docs`)
	require.NoError(t, err)
}

func TestTransactionLifeCycle(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		_, err := db.Exec("create temporary table t(a varchar not null)")
		require.NoError(t, err)

		tx, err := db.Begin()
		require.NoError(t, err)

		_, err = tx.Exec("insert into t values('hi')")
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)

		var n int64
		err = db.QueryRow("select count(*) from t").Scan(&n)
		require.NoError(t, err)
		require.EqualValues(t, 0, n)

		tx, err = db.Begin()
		require.NoError(t, err)

		_, err = tx.Exec("insert into t values('hi')")
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		err = db.QueryRow("select count(*) from t").Scan(&n)
		require.NoError(t, err)
		require.EqualValues(t, 1, n)
	})
}

func TestConnBeginTxIsolation(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		var defaultIsoLevel string
		err := db.QueryRow("show transaction_isolation").Scan(&defaultIsoLevel)
		require.NoError(t, err)

		supportedTests := []struct {
			sqlIso     sql.IsolationLevel
			gaussdbIso string
		}{
			{sqlIso: sql.LevelDefault, gaussdbIso: defaultIsoLevel},
			{sqlIso: sql.LevelReadUncommitted, gaussdbIso: "read uncommitted"},
			{sqlIso: sql.LevelReadCommitted, gaussdbIso: "read committed"},
			{sqlIso: sql.LevelRepeatableRead, gaussdbIso: "repeatable read"},
			{sqlIso: sql.LevelSnapshot, gaussdbIso: "repeatable read"},
			// todo GaussDB目前功能上不支持此隔离级别，等价于REPEATABLE READ (参考：https://support.huaweicloud.com/intl/zh-cn/centralized-devg-v2-gaussdb/gaussdb_42_0501.html)
			{sqlIso: sql.LevelSerializable, gaussdbIso: "repeatable read"},
		}
		for i, tt := range supportedTests {
			func() {
				tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: tt.sqlIso})
				if err != nil {
					t.Errorf("%d. BeginTx failed: %v", i, err)
					return
				}
				defer tx.Rollback()

				var gaussdbIso string
				err = tx.QueryRow("show transaction_isolation").Scan(&gaussdbIso)
				if err != nil {
					t.Errorf("%d. QueryRow failed: %v", i, err)
				}

				if gaussdbIso != tt.gaussdbIso {
					t.Errorf("%d. gaussdbIso => %s, want %s", i, gaussdbIso, tt.gaussdbIso)
				}
			}()
		}

		unsupportedTests := []struct {
			sqlIso sql.IsolationLevel
		}{
			{sqlIso: sql.LevelWriteCommitted},
			{sqlIso: sql.LevelLinearizable},
		}
		for i, tt := range unsupportedTests {
			tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: tt.sqlIso})
			if err == nil {
				t.Errorf("%d. BeginTx should have failed", i)
				tx.Rollback()
			}
		}
	})
}

func TestConnBeginTxReadOnly(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
		require.NoError(t, err)
		defer tx.Rollback()

		var gaussdbReadOnly string
		err = tx.QueryRow("show transaction_read_only").Scan(&gaussdbReadOnly)
		if err != nil {
			t.Errorf("QueryRow failed: %v", err)
		}

		if gaussdbReadOnly != "on" {
			t.Errorf("gaussdbReadOnly => %s, want %s", gaussdbReadOnly, "on")
		}
	})
}

func TestBeginTxContextCancel(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		_, err := db.Exec("drop table if exists t")
		require.NoError(t, err)

		ctx, cancelFn := context.WithCancel(context.Background())

		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)

		_, err = tx.Exec("create table t(id serial)")
		require.NoError(t, err)

		cancelFn()

		err = tx.Commit()
		if err != context.Canceled && err != sql.ErrTxDone {
			t.Fatalf("err => %v, want %v or %v", err, context.Canceled, sql.ErrTxDone)
		}

		var n int
		err = db.QueryRow("select count(*) from t").Scan(&n)
		if gaussdbErr, ok := err.(*gaussdbconn.GaussdbError); !ok || gaussdbErr.Code != "42P01" {
			t.Fatalf(`err => %v, want GaussdbError{Code: "42P01"}`, err)
		}
	})
}

func TestConnRaw(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		conn, err := db.Conn(context.Background())
		require.NoError(t, err)

		var n int
		err = conn.Raw(func(driverConn any) error {
			conn := driverConn.(*stdlib.Conn).Conn()
			return conn.QueryRow(context.Background(), "select 42").Scan(&n)
		})
		require.NoError(t, err)
		assert.EqualValues(t, 42, n)
	})
}

func TestConnPingContextSuccess(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		err := db.PingContext(context.Background())
		require.NoError(t, err)
	})
}

func TestConnPrepareContextSuccess(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		stmt, err := db.PrepareContext(context.Background(), "select now()")
		require.NoError(t, err)
		err = stmt.Close()
		require.NoError(t, err)
	})
}

func TestConnMultiplePrepareAndDeallocate(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		sql := "select 42"
		stmt1, err := db.PrepareContext(context.Background(), sql)
		require.NoError(t, err)
		stmt2, err := db.PrepareContext(context.Background(), sql)
		require.NoError(t, err)
		err = stmt1.Close()
		require.NoError(t, err)

		var preparedStmtCount int64
		err = db.QueryRowContext(context.Background(), "select count(*) from pg_prepared_statements where statement = $1", sql).Scan(&preparedStmtCount)
		require.NoError(t, err)
		require.EqualValues(t, 1, preparedStmtCount)

		err = stmt2.Close() // err isn't as useful as it should be as database/sql will ignore errors from Deallocate.
		require.NoError(t, err)

		err = db.QueryRowContext(context.Background(), "select count(*) from pg_prepared_statements where statement = $1", sql).Scan(&preparedStmtCount)
		require.NoError(t, err)
		require.EqualValues(t, 0, preparedStmtCount)
	})
}

func TestConnExecContextSuccess(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		_, err := db.ExecContext(context.Background(), "drop table if exists exec_context_test; create table exec_context_test(id serial primary key)")
		require.NoError(t, err)
	})
}

func TestConnQueryContextSuccess(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		rows, err := db.QueryContext(context.Background(), "select * from generate_series(1,10) n")
		require.NoError(t, err)

		for rows.Next() {
			var n int64
			err := rows.Scan(&n)
			require.NoError(t, err)
		}
		require.NoError(t, rows.Err())
	})
}

func TestRowsColumnTypeDatabaseTypeName(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		rows, err := db.Query("select 42::bigint")
		require.NoError(t, err)

		columnTypes, err := rows.ColumnTypes()
		require.NoError(t, err)
		require.Len(t, columnTypes, 1)

		if columnTypes[0].DatabaseTypeName() != "INT8" {
			t.Errorf("columnTypes[0].DatabaseTypeName() => %v, want %v", columnTypes[0].DatabaseTypeName(), "INT8")
		}

		err = rows.Close()
		require.NoError(t, err)
	})
}

func TestStmtExecContextSuccess(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	_, err := db.Exec("create temporary table t(id int primary key)")
	require.NoError(t, err)

	stmt, err := db.Prepare("insert into t(id) values ($1::int4)")
	require.NoError(t, err)
	defer stmt.Close()

	_, err = stmt.ExecContext(context.Background(), 42)
	require.NoError(t, err)

	ensureDBValid(t, db)
}

func TestStmtExecContextCancel(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	_, err := db.Exec("create temporary table t(id int primary key)")
	require.NoError(t, err)

	stmt, err := db.Prepare("insert into t(id) select $1::int4 from pg_sleep(5)")
	require.NoError(t, err)
	defer stmt.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = stmt.ExecContext(ctx, 42)
	if !gaussdbconn.Timeout(err) {
		t.Errorf("expected timeout error, got %v", err)
	}

	ensureDBValid(t, db)
}

func TestStmtQueryContextSuccess(t *testing.T) {
	db := openDB(t)
	defer closeDB(t, db)

	stmt, err := db.Prepare("select * from generate_series(1,$1::int4) n")
	require.NoError(t, err)
	defer stmt.Close()

	rows, err := stmt.QueryContext(context.Background(), 5)
	require.NoError(t, err)

	for rows.Next() {
		var n int64
		if err := rows.Scan(&n); err != nil {
			t.Error(err)
		}
	}

	if rows.Err() != nil {
		t.Error(rows.Err())
	}

	ensureDBValid(t, db)
}

func TestRowsColumnTypes(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		columnTypesTests := []struct {
			Name     string
			TypeName string
			Length   struct {
				Len int64
				OK  bool
			}
			DecimalSize struct {
				Precision int64
				Scale     int64
				OK        bool
			}
			ScanType reflect.Type
		}{
			{
				Name:     "a",
				TypeName: "INT8",
				Length: struct {
					Len int64
					OK  bool
				}{
					Len: 0,
					OK:  false,
				},
				DecimalSize: struct {
					Precision int64
					Scale     int64
					OK        bool
				}{
					Precision: 0,
					Scale:     0,
					OK:        false,
				},
				ScanType: reflect.TypeOf(int64(0)),
			}, {
				Name:     "bar",
				TypeName: "TEXT",
				Length: struct {
					Len int64
					OK  bool
				}{
					Len: math.MaxInt64,
					OK:  true,
				},
				DecimalSize: struct {
					Precision int64
					Scale     int64
					OK        bool
				}{
					Precision: 0,
					Scale:     0,
					OK:        false,
				},
				ScanType: reflect.TypeOf(""),
			}, {
				Name:     "dec",
				TypeName: "NUMERIC",
				Length: struct {
					Len int64
					OK  bool
				}{
					Len: 0,
					OK:  false,
				},
				DecimalSize: struct {
					Precision int64
					Scale     int64
					OK        bool
				}{
					Precision: 9,
					Scale:     2,
					OK:        true,
				},
				ScanType: reflect.TypeOf(float64(0)),
			}, {
				Name:     "d",
				TypeName: "1266",
				Length: struct {
					Len int64
					OK  bool
				}{
					Len: 0,
					OK:  false,
				},
				DecimalSize: struct {
					Precision int64
					Scale     int64
					OK        bool
				}{
					Precision: 0,
					Scale:     0,
					OK:        false,
				},
				ScanType: reflect.TypeOf(""),
			},
		}

		rows, err := db.Query("SELECT 1::bigint AS a, text 'bar' AS bar, 1.28::numeric(9, 2) AS dec, '12:00:00'::timetz as d")
		require.NoError(t, err)

		columns, err := rows.ColumnTypes()
		require.NoError(t, err)
		assert.Len(t, columns, 4)

		for i, tt := range columnTypesTests {
			c := columns[i]
			if c.Name() != tt.Name {
				t.Errorf("(%d) got: %s, want: %s", i, c.Name(), tt.Name)
			}
			if c.DatabaseTypeName() != tt.TypeName {
				t.Errorf("(%d) got: %s, want: %s", i, c.DatabaseTypeName(), tt.TypeName)
			}
			l, ok := c.Length()
			if l != tt.Length.Len {
				t.Errorf("(%d) got: %d, want: %d", i, l, tt.Length.Len)
			}
			if ok != tt.Length.OK {
				t.Errorf("(%d) got: %t, want: %t", i, ok, tt.Length.OK)
			}
			p, s, ok := c.DecimalSize()
			if p != tt.DecimalSize.Precision {
				t.Errorf("(%d) got: %d, want: %d", i, p, tt.DecimalSize.Precision)
			}
			if s != tt.DecimalSize.Scale {
				t.Errorf("(%d) got: %d, want: %d", i, s, tt.DecimalSize.Scale)
			}
			if ok != tt.DecimalSize.OK {
				t.Errorf("(%d) got: %t, want: %t", i, ok, tt.DecimalSize.OK)
			}
			if c.ScanType() != tt.ScanType {
				t.Errorf("(%d) got: %v, want: %v", i, c.ScanType(), tt.ScanType)
			}
		}
	})
}

func TestQueryLifeCycle(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		rows, err := db.Query("SELECT 'foo', n FROM generate_series($1::int, $2::int) n WHERE 3 = $3", 1, 10, 3)
		require.NoError(t, err)

		rowCount := int64(0)

		for rows.Next() {
			rowCount++
			var (
				s string
				n int64
			)

			err := rows.Scan(&s, &n)
			require.NoError(t, err)

			if s != "foo" {
				t.Errorf(`Expected "foo", received "%v"`, s)
			}

			if n != rowCount {
				t.Errorf("Expected %d, received %d", rowCount, n)
			}
		}
		require.NoError(t, rows.Err())

		err = rows.Close()
		require.NoError(t, err)

		rows, err = db.Query("select 1 where false")
		require.NoError(t, err)

		rowCount = int64(0)

		for rows.Next() {
			rowCount++
		}
		require.NoError(t, rows.Err())
		require.EqualValues(t, 0, rowCount)

		err = rows.Close()
		require.NoError(t, err)
	})
}

func TestScanJSONIntoJSONRawMessage(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {
		var msg json.RawMessage

		err := db.QueryRow("select '{}'::json").Scan(&msg)
		require.NoError(t, err)
		require.EqualValues(t, []byte("{}"), []byte(msg))
	})
}

type testLog struct {
	lvl  tracelog.LogLevel
	msg  string
	data map[string]any
}

type testLogger struct {
	logs []testLog
}

func (l *testLogger) Log(ctx context.Context, lvl tracelog.LogLevel, msg string, data map[string]any) {
	l.logs = append(l.logs, testLog{lvl: lvl, msg: msg, data: data})
}

func TestRegisterConnConfig(t *testing.T) {
	connConfig, err := gaussdbgo.ParseConfig(os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
	require.NoError(t, err)

	logger := &testLogger{}
	connConfig.Tracer = &tracelog.TraceLog{Logger: logger, LogLevel: tracelog.LogLevelInfo}

	// Issue 947: Register and unregister a ConnConfig and ensure that the
	// returned connection string is not reused.
	connStr := stdlib.RegisterConnConfig(connConfig)
	require.Equal(t, "registeredConnConfig0", connStr)
	stdlib.UnregisterConnConfig(connStr)

	connStr = stdlib.RegisterConnConfig(connConfig)
	defer stdlib.UnregisterConnConfig(connStr)
	require.Equal(t, "registeredConnConfig1", connStr)

	db, err := sql.Open("gaussdb", connStr)
	require.NoError(t, err)
	defer closeDB(t, db)

	var n int64
	err = db.QueryRow("select 1").Scan(&n)
	require.NoError(t, err)

	l := logger.logs[len(logger.logs)-1]
	assert.Equal(t, "Query", l.msg)
	assert.Equal(t, "select 1", l.data["sql"])
}

func TestConnQueryRowConstraintErrors(t *testing.T) {
	testWithAllQueryExecModes(t, func(t *testing.T, db *sql.DB) {

		_, err := db.Exec(`
			drop table if exists defer_test;
			create table defer_test (
			id text primary key,
			n int not null, unique (n),
			unique (n) deferrable initially deferred )`)
		require.NoError(t, err)
		// todo: gaussdb needs function parameter, https://doc.hcs.huawei.com/db/en-us/gaussdb/24.7.30.10/devg-dist/gaussdb-12-0595.html
		_, err = db.Exec(`drop function if exists test_trigger() cascade`)
		require.NoError(t, err)

		_, err = db.Exec(`create function test_trigger() returns trigger language plpgsql as $$
		begin
		if new.n = 4 then
			raise exception 'n cant be 4!';
		end if;
		return new;
	end$$`)
		require.NoError(t, err)
		// todo: gaussdb use `procedure` key word not `function`
		_, err = db.Exec(`create constraint trigger test
			after insert or update on defer_test
			deferrable initially deferred
			for each row
			execute procedure test_trigger()`)
		require.NoError(t, err)

		_, err = db.Exec(`insert into defer_test (id, n) values ('a', 1), ('b', 2), ('c', 3)`)
		require.NoError(t, err)

		var id string
		err = db.QueryRow(`insert into defer_test (id, n) values ('e', 4) returning id`).Scan(&id)
		assert.Error(t, err)
	})
}

func TestOptionBeforeAfterConnect(t *testing.T) {
	config, err := gaussdbgo.ParseConfig(os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
	require.NoError(t, err)

	var beforeConnConfigs []*gaussdbgo.ConnConfig
	var afterConns []*gaussdbgo.Conn
	db := stdlib.OpenDB(*config,
		stdlib.OptionBeforeConnect(func(ctx context.Context, connConfig *gaussdbgo.ConnConfig) error {
			beforeConnConfigs = append(beforeConnConfigs, connConfig)
			return nil
		}),
		stdlib.OptionAfterConnect(func(ctx context.Context, conn *gaussdbgo.Conn) error {
			afterConns = append(afterConns, conn)
			return nil
		}))
	defer closeDB(t, db)

	// Force it to close and reopen a new connection after each query
	db.SetMaxIdleConns(0)

	_, err = db.Exec("select 1")
	require.NoError(t, err)

	_, err = db.Exec("select 1")
	require.NoError(t, err)

	require.Len(t, beforeConnConfigs, 2)
	require.Len(t, afterConns, 2)

	// Note: BeforeConnect creates a shallow copy, so the config contents will be the same but we wean to ensure they
	// are different objects, so can't use require.NotEqual
	require.False(t, config == beforeConnConfigs[0])
	require.False(t, beforeConnConfigs[0] == beforeConnConfigs[1])
}

func TestRandomizeHostOrderFunc(t *testing.T) {
	config, err := gaussdbgo.ParseConfig("gaussdb://host1,host2,host3")
	require.NoError(t, err)

	// Test that at some point we connect to all 3 hosts
	hostsNotSeenYet := map[string]struct{}{
		"host1": {},
		"host2": {},
		"host3": {},
	}

	// If we don't succeed within this many iterations, something is certainly wrong
	for i := 0; i < 100000; i++ {
		connCopy := *config
		stdlib.RandomizeHostOrderFunc(context.Background(), &connCopy)

		delete(hostsNotSeenYet, connCopy.Host)
		if len(hostsNotSeenYet) == 0 {
			return
		}

	hostCheckLoop:
		for _, h := range []string{"host1", "host2", "host3"} {
			if connCopy.Host == h {
				continue
			}
			for _, f := range connCopy.Fallbacks {
				if f.Host == h {
					continue hostCheckLoop
				}
			}
			require.Failf(t, "got configuration from RandomizeHostOrderFunc that did not have all the hosts", "%+v", connCopy)
		}
	}

	require.Fail(t, "did not get all hosts as primaries after many randomizations")
}

func TestResetSessionHookCalled(t *testing.T) {
	var mockCalled bool

	connConfig, err := gaussdbgo.ParseConfig(os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
	require.NoError(t, err)

	db := stdlib.OpenDB(*connConfig, stdlib.OptionResetSession(func(ctx context.Context, conn *gaussdbgo.Conn) error {
		mockCalled = true

		return nil
	}))

	defer closeDB(t, db)

	err = db.Ping()
	require.NoError(t, err)

	err = db.Ping()
	require.NoError(t, err)

	require.True(t, mockCalled)
}

// todo checkConn is deprecated, .PID() has problem similar to TestFatalTxError
/*func TestCheckIdleConn(t *testing.T) {
	// stdlib/sql.go#L102, register here
	controllerConn, err := sql.Open("gaussdb", os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
	require.NoError(t, err)
	defer closeDB(t, controllerConn)

	db, err := sql.Open("gaussdb", os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
	require.NoError(t, err)
	defer closeDB(t, db)

	var conns []*sql.Conn
	for i := 0; i < 3; i++ {
		c, err := db.Conn(context.Background())
		require.NoError(t, err)
		conns = append(conns, c)
	}

	require.EqualValues(t, 3, db.Stats().OpenConnections)

	var pids []uint32
	for _, c := range conns {
		err := c.Raw(func(driverConn any) error {
			pids = append(pids, driverConn.(*stdlib.Conn).Conn().GaussdbConn().PID())
			return nil
		})
		require.NoError(t, err)
		err = c.Close()
		require.NoError(t, err)
	}

	// The database/sql connection pool seems to automatically close idle connections to only keep 2 alive.
	// require.EqualValues(t, 3, db.Stats().OpenConnections)

	_, err = controllerConn.ExecContext(context.Background(), `select pg_terminate_backend(n) from unnest($1::int[]) n`, pids)
	require.NoError(t, err)

	// All conns are dead they don't know it and neither does the pool. But because of database/sql automatically closing
	// idle connections we can't be sure how many we should have. require.EqualValues(t, 3, db.Stats().OpenConnections)

	// Wait long enough so the pool will realize it needs to check the connections.
	time.Sleep(time.Second)

	// Pool should try all existing connections and find them dead, then create a new connection which should successfully ping.
	err = db.PingContext(context.Background())
	require.NoError(t, err)

	// The original 3 conns should have been terminated and the a new conn established for the ping.
	require.EqualValues(t, 1, db.Stats().OpenConnections)
	c, err := db.Conn(context.Background())
	require.NoError(t, err)

	var cPID uint32
	err = c.Raw(func(driverConn any) error {
		cPID = driverConn.(*stdlib.Conn).Conn().GaussdbConn().PID()
		return nil
	})
	require.NoError(t, err)
	err = c.Close()
	require.NoError(t, err)

	require.NotContains(t, pids, cPID)
}*/
