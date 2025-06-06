package gaussdbtype_test

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/HuaweiCloudDeveloper/gaussdb-go"
)

type Player struct {
	Name     string
	Position string
}

type Team struct {
	Name    string
	Players []Player
}

// This example uses a single query to return parent and child records.
func Example_childRecords() {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := gaussdbgo.Connect(ctx, os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	// Setup example schema and data.
	_, err = conn.Exec(ctx, `
create temporary table teams (
	name text primary key
);

create temporary table players (
	name text primary key,
	team_name text,
	position text
);

insert into teams (name) values
	('Alpha'),
	('Beta');

insert into players (name, team_name, position) values
	('Adam', 'Alpha', 'wing'),
	('Bill', 'Alpha', 'halfback'),
	('Charlie', 'Alpha', 'fullback'),
	('Don', 'Beta', 'halfback'),
	('Edgar', 'Beta', 'halfback'),
	('Frank', 'Beta', 'fullback')
`)
	if err != nil {
		fmt.Printf("Unable to setup example schema and data: %v", err)
		return
	}

	rows, _ := conn.Query(ctx, `
select t.name,
	(select array_agg(row(p.name, position) order by p.name) from players p where p.team_name = t.name)
from teams t
order by t.name
`)
	teams, err := gaussdbgo.CollectRows(rows, gaussdbgo.RowToStructByPos[Team])
	if err != nil {
		fmt.Printf("CollectRows error: %v", err)
		return
	}

	for _, team := range teams {
		fmt.Println(team.Name)
		for _, player := range team.Players {
			fmt.Printf("  %s: %s\n", player.Name, player.Position)
		}
	}

	// Output:
	// Alpha
	//   Adam: wing
	//   Bill: halfback
	//   Charlie: fullback
	// Beta
	//   Don: halfback
	//   Edgar: halfback
	//   Frank: fullback
}
