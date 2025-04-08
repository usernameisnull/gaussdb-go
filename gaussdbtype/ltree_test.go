package gaussdbtype_test

import (
	"context"
	"testing"

	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbtype"
	"github.com/HuaweiCloudDeveloper/gaussdb-go/gaussdbxtest"
)

func TestLtreeCodec(t *testing.T) {
	skipCockroachDB(t, "Server does not support type ltree")

	gaussdbxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, gaussdbxtest.KnownOIDQueryExecModes, "ltree", []gaussdbxtest.ValueRoundTripTest{
		{
			Param:  "A.B.C",
			Result: new(string),
			Test:   isExpectedEq("A.B.C"),
		},
		{
			Param:  gaussdbtype.Text{String: "", Valid: true},
			Result: new(gaussdbtype.Text),
			Test:   isExpectedEq(gaussdbtype.Text{String: "", Valid: true}),
		},
	})
}
