package papergres

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestTableObj struct {
	PersonName string `db:"person_name"`
	PersonId   int64  `db:"person_id"`
}

func TestGetFields(t *testing.T) {
	obj := TestTableObj{
		PersonName: "Cristiano",
		PersonId:   1099,
	}

	fields := fields(obj)

	assert.NotEmpty(t, fields, "Fields are empty")
	assert.Equal(t, fields[0].Name, "PersonName", "Not equal")
	assert.Equal(t, fields[0].Typeof, "string", "Not equal")
	assert.Equal(t, fields[0].Value, "Cristiano", "Not equal")
	assert.Equal(t, fields[0].Tag, "person_name", "Not equal")
	assert.Equal(t, fields[1].Name, "PersonId", "Not equal")
	assert.Equal(t, fields[1].Typeof, "int64", "Not equal")
	assert.Equal(t, fields[1].Value, int64(1099), "Not equal")
	assert.Equal(t, fields[1].Tag, "person_id", "Not equal")
}
