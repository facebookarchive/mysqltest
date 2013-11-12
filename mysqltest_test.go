package mysqltest_test

import (
	"testing"

	"github.com/ParsePlatform/go.mysqltest"
)

func test(t *testing.T, answer int) {
	t.Parallel()
	mysql, db := mysqltest.NewServerDB(t, "metadb")
	defer mysql.Stop()

	const id = 1
	_, err := db.Exec(`
create table metatable (
  id int,
  answer int
) ENGINE = MEMORY;
  `)
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := db.Prepare("insert into metatable values (?, ?)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	if _, err := stmt.Exec(id, answer); err != nil {
		t.Fatal(err)
	}
}

// Testing that multiple instances don't stomp on each other.
func TestOne(t *testing.T) {
	test(t, 42)
}

func TestTwo(t *testing.T) {
	test(t, 43)
}

func TestThree(t *testing.T) {
	test(t, 44)
}
