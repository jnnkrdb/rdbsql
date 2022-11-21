// This package is wrapper for postgresql db connections. To use it with your own structs,
// you have to add an column-tag to your structs and an ID field.
//
// This package is currently in a BETA-state.
package postgresql

import "fmt"

// postgres connectionstring object, json ready, no encryption
type PostgresConnection struct {
	Host         string `json:"host"`
	Port         string `json:"port"`
	User         string `json:"user"`
	Password     string `json:"password"`
	DatabaseName string `json:"dbname"`
	SSLMode      string `json:"sslode"`
}

// receive the connectionstring of a PostgresConnection object
func (pc PostgresConnection) ConnectionString() string {

	if pc.Host != "" &&
		pc.Port != "" &&
		pc.User != "" &&
		pc.Password != "" &&
		pc.DatabaseName != "" {

		return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			pc.Host,
			pc.Port,
			pc.User,
			pc.Password,
			pc.DatabaseName)

	} else {

		return ""
	}
}
