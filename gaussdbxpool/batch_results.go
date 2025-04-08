package gaussdbxpool

import (
	"github.com/HuaweiCloudDeveloper/gaussdb-go"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbconn"
)

type errBatchResults struct {
	err error
}

func (br errBatchResults) Exec() (gaussdbconn.CommandTag, error) {
	return gaussdbconn.CommandTag{}, br.err
}

func (br errBatchResults) Query() (pgx.Rows, error) {
	return errRows{err: br.err}, br.err
}

func (br errBatchResults) QueryRow() pgx.Row {
	return errRow{err: br.err}
}

func (br errBatchResults) Close() error {
	return br.err
}

type poolBatchResults struct {
	br pgx.BatchResults
	c  *Conn
}

func (br *poolBatchResults) Exec() (gaussdbconn.CommandTag, error) {
	return br.br.Exec()
}

func (br *poolBatchResults) Query() (pgx.Rows, error) {
	return br.br.Query()
}

func (br *poolBatchResults) QueryRow() pgx.Row {
	return br.br.QueryRow()
}

func (br *poolBatchResults) Close() error {
	err := br.br.Close()
	if br.c != nil {
		br.c.Release()
		br.c = nil
	}
	return err
}
