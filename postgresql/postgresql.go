// This package is wrapper for postgresql db connections. To use it with your own structs,
// you have to add an column-tag to your structs and an ID field.
//
// This package is currently in a BETA-state.
package postgresql

import (
	"database/sql"
	"reflect"

	"github.com/jnnkrdb/corerdb/prtcl"

	_ "github.com/lib/pq"
)

// sqlite3 struct to group informations, json ready
type Postgres struct {
	db         *sql.DB
	Connection PostgresConnection `json:"connection"`
}

// connect to the sqlite database file
func (psql *Postgres) Connect(pc PostgresConnection) {

	prtcl.Log.Println("connecting to database at", pc.Host+":"+pc.Port)

	psql.Connection = pc

	if psql.Connection.ConnectionString() != "" {

		if tmpDB, err := sql.Open("postgres", psql.Connection.ConnectionString()); err != nil {

			prtcl.Log.Println("error connecting to database:", err)

			prtcl.PrintObject(psql, tmpDB, err)

		} else {

			psql.db = tmpDB

			psql.CheckConnection()
		}
	}
}

// check connection to dbfile
func (psql *Postgres) CheckConnection() error {

	prtcl.Log.Println("checking connection to:", psql.Connection.Host+":"+psql.Connection.Port)

	err := psql.db.Ping()

	if err != nil {

		prtcl.PrintObject(psql, err)
	}

	return err
}

// get the db pointer from the sqlite3 pkg
func (psql Postgres) DB() *sql.DB {

	return psql.db
}

// disconnect from the currently connected file
func (psql Postgres) Disconnect() error {

	prtcl.Log.Println("connection closed:", psql.Connection.Host+":"+psql.Connection.Port)

	return psql.db.Close()
}

// ----------------------------------------------------------------------------------------------------- SQL STATEMENTS

// this function generates a "select" statement, where the estimated object
// is exactly one object, not from a list. The function requires the object-
// struct to have an "ID" field.
//
// Parameters:
//   - `tblName` : string > the name of the table, where tho object is estimated
//   - `objPointer` : interface{} | *struct > pointer to the object, is used to get the struct-informations
//   - `obj` : interface{} | struct > the same object, but not as a pointer, where the data should be stored
func (psql Postgres) SelectObject(tblName string, objPointer, obj interface{}) (err error) {

	sqlStatement := "SELECT "

	// this one is new, test
	// obj := reflect.ValueOf(objPointer).Elem()

	for i := 0; i < reflect.ValueOf(objPointer).Elem().NumField()-1; i++ {

		sqlStatement += reflect.TypeOf(obj).Field(i).Tag.Get("column") + ", "
	}

	sqlStatement += reflect.TypeOf(obj).Field(reflect.ValueOf(objPointer).Elem().NumField()-1).Tag.Get("column") + " FROM " + tblName + " WHERE id=?;"

	args := make([]interface{}, reflect.ValueOf(objPointer).Elem().NumField())

	row := psql.DB().QueryRow(sqlStatement, reflect.ValueOf(objPointer).Elem().FieldByName("ID").Int())

	for i := 0; i < len(args); i++ {

		args[i] = reflect.ValueOf(objPointer).Elem().Field(i).Addr().Interface()
	}

	if err = row.Scan(args...); err != nil {

		prtcl.PrintObject(psql, objPointer, obj, sqlStatement, args, row, err)

		return
	}

	prtcl.Log.Println("selected | collected rows: ", 1)

	return
}

// this function generates a "select" statement, where the estimated object
// is a list of objects. The function does not require the object struct to have an "ID" field.
//
// Parameters:
//   - `tblName` : string > the name of the table, where tho object is estimated
//   - `objPointer` : interface{} | *[]struct > contains the list of the array
func (psql Postgres) SelectObjects(tblName string, objPointer interface{}) (err error) {

	sqlStatement := "SELECT * FROM " + tblName + ";"

	var rows *sql.Rows = nil

	if rows, err = psql.DB().Query(sqlStatement); err != nil {

		prtcl.PrintObject(psql, objPointer, sqlStatement, rows, err)

	} else {

		destv := reflect.ValueOf(objPointer).Elem()

		args := make([]interface{}, destv.Type().Elem().NumField())

		var rowscount int = 0

		for rows.Next() {

			rowp := reflect.New(destv.Type().Elem())

			rowv := rowp.Elem()

			for i := 0; i < rowv.NumField(); i++ {

				args[i] = rowv.Field(i).Addr().Interface()
			}

			if err = rows.Scan(args...); err != nil {

				prtcl.PrintObject(psql, objPointer, sqlStatement, rows, destv, args, rowscount, rowp, rowv, err)

				return
			}

			destv.Set(reflect.Append(destv, rowv))

			rowscount++
		}

		prtcl.Log.Println("selected | collected rows:", rowscount)
	}

	return
}

// this function uses a specified sql-statement to select multiple objects. the statement will be given through the
// variable sqlStatement <string>
//
// Parameters:
//   - `sqlStatement` : string > the sql stateent, which is used to receive the objects
//   - `objPointer` : interface{} | *[]struct > contains the list of the objects
func (psql Postgres) SpecificSelectObjects(sqlStatement string, objPointer interface{}) (err error) {

	prtcl.Log.Println("SpecificSelectObjects:", sqlStatement)

	var rows *sql.Rows = nil

	if rows, err = psql.DB().Query(sqlStatement); err != nil {

		prtcl.PrintObject(psql, sqlStatement, objPointer, rows, err)

	} else {

		destv := reflect.ValueOf(objPointer).Elem()

		args := make([]interface{}, destv.Type().Elem().NumField())

		var rowscount int = 0

		for rows.Next() {

			rowp := reflect.New(destv.Type().Elem())

			rowv := rowp.Elem()

			for i := 0; i < rowv.NumField(); i++ {

				args[i] = rowv.Field(i).Addr().Interface()
			}

			if err = rows.Scan(args...); err != nil {

				prtcl.PrintObject(psql, sqlStatement, objPointer, rows, destv, args, rowp, rowv, err)

				return
			}

			destv.Set(reflect.Append(destv, rowv))

			rowscount++
		}

		prtcl.Log.Println("specific selected | collected rows:", rowscount)
	}

	return
}

// this function uses a specified sql-statement to select multiple objects. the statement will be given through the
// variable sqlStatement <string>
//
// Parameters:
//   - `sqlStatement` : string > the sql stateent, which is used to receive the objects
//   - `objPointer` : interface{} | *struct > contains the pointer to the object -> *[]struct
func (psql Postgres) SpecificSelectObject(sqlStatement string, objPointer interface{}) (err error) {

	prtcl.Log.Println("SpecificSelectObject:", sqlStatement)

	args := make([]interface{}, reflect.ValueOf(objPointer).Elem().NumField())

	row := psql.DB().QueryRow(sqlStatement)

	for i := 0; i < len(args); i++ {

		args[i] = reflect.ValueOf(objPointer).Elem().Field(i).Addr().Interface()
	}

	if err = row.Scan(args...); err != nil {

		prtcl.PrintObject(psql, objPointer, sqlStatement, objPointer, args, row, err)
	}

	prtcl.Log.Println("specific selected | collected rows:", 1)

	return
}

// this function generates an "insert" statement, where the given struct
// is inserted into the given table. The function requires the object struct to have an "ID" field,
// so the struct receives the new id.
//
// Parameters:
//   - `tblName` : string > the name of the table, where tho object is estimated
//   - `objPointer` : interface{} | *struct > pointer to the object, is used to get the struct-informations
//   - `obj` : interface{} | struct > the same object, but not as a pointer, is used to get the struct-informations
func (psql Postgres) InsertObject(tblName string, objPointer, obj interface{}) (err error) {

	sqlStatement := "INSERT INTO " + tblName + " ( "

	// this one is new, test
	// obj := reflect.ValueOf(objPointer).Elem()

	for i := 1; i < reflect.ValueOf(objPointer).Elem().NumField(); i++ {

		sqlStatement += reflect.TypeOf(obj).Field(i).Tag.Get("column")

		if i != reflect.ValueOf(objPointer).Elem().NumField()-1 {

			sqlStatement += ", "
		}
	}

	sqlStatement += " ) VALUES ( "

	for i := 1; i < reflect.ValueOf(objPointer).Elem().NumField(); i++ {

		sqlStatement += "?"

		if i != reflect.ValueOf(objPointer).Elem().NumField()-1 {

			sqlStatement += ", "
		}
	}

	sqlStatement += " );"

	var statement *sql.Stmt = nil

	if statement, err = psql.DB().Prepare(sqlStatement); err != nil {

		prtcl.PrintObject(psql, objPointer, obj, sqlStatement, statement, err)

	} else {

		args := make([]interface{}, reflect.ValueOf(objPointer).Elem().NumField()-1)

		for i := 0; i < len(args); i++ {

			args[i] = reflect.ValueOf(objPointer).Elem().Field(i + 1).Interface()
		}

		var result sql.Result = nil

		if result, err = statement.Exec(args...); err != nil {

			prtcl.PrintObject(psql, objPointer, obj, sqlStatement, statement, args, result, err)

		} else {

			var id int64 = 0

			if id, err = result.LastInsertId(); err != nil {

				prtcl.PrintObject(psql, objPointer, obj, sqlStatement, statement, args, result, id, err)

			} else {

				reflect.ValueOf(objPointer).Elem().FieldByName("ID").SetInt(id)

				prtcl.Log.Println("inserted | new id: ", id)
			}
		}
	}

	return
}

// this function generates an "update" statement, where the given struct updates the values
// in the given table. The function requires the object struct to have an "ID" field,
// so the table receives the new values.
//
// Parameters:
//   - `tblName` : string > the name of the table, where tho object is estimated
//   - `objPointer` : interface{} | *struct > pointer to the object, is used to store the values
//   - `obj` : interface{} | struct > the same object, but not as a pointer, is used to get the struct-informations
func (psql Postgres) UpdateObject(tblName string, objPointer, obj interface{}) (err error) {

	sqlStatement := "UPDATE " + tblName + " SET "

	// this one is new, test
	// obj := reflect.ValueOf(objPointer).Elem()

	for i := 1; i < reflect.ValueOf(objPointer).Elem().NumField(); i++ {

		sqlStatement += reflect.TypeOf(obj).Field(i).Tag.Get("column") + "=?"

		if i != reflect.ValueOf(objPointer).Elem().NumField()-1 {

			sqlStatement += ", "
		}
	}

	sqlStatement += " WHERE id=?;"

	var statement *sql.Stmt = nil

	if statement, err = psql.DB().Prepare(sqlStatement); err != nil {

		prtcl.PrintObject(psql, objPointer, obj, sqlStatement, statement, err)

	} else {

		args := make([]interface{}, reflect.ValueOf(objPointer).Elem().NumField())

		for i := 1; i < len(args); i++ {

			args[i-1] = reflect.ValueOf(objPointer).Elem().Field(i).Interface()
		}

		args[len(args)-1] = reflect.ValueOf(objPointer).Elem().Field(0).Interface()

		var result sql.Result = nil

		if result, err = statement.Exec(args...); err != nil {

			prtcl.PrintObject(psql, objPointer, obj, sqlStatement, statement, args, result, err)

		} else {

			var rowsaffected int64 = 0

			if rowsaffected, err = result.RowsAffected(); err != nil {

				prtcl.PrintObject(psql, objPointer, obj, sqlStatement, statement, args, result, rowsaffected, err)

			} else {

				prtcl.Log.Println("updated | updated rows: ", rowsaffected)
			}
		}
	}

	return
}

// this function generates an "delete" statement, where the given struct is used to delete the object
// from the given table. The function requires the object struct to have an "ID" field with a valid value.
//
// Parameters:
//   - `tblName` : string > the name of the table, where tho object is estimated
//   - `obj` : interface{} | struct > the object with an ID-field and an valid value
func (psql Postgres) DeleteObject(tblName string, obj interface{}) (err error) {

	sqlStatement := "DELETE FROM " + tblName + " WHERE id=?;"

	var statement *sql.Stmt = nil

	if statement, err = psql.DB().Prepare(sqlStatement); err != nil {

		prtcl.PrintObject(psql, obj, sqlStatement, statement, err)

	} else {

		var result sql.Result = nil

		if result, err = statement.Exec(reflect.ValueOf(obj).Elem().FieldByName("ID").Interface()); err != nil {

			prtcl.PrintObject(psql, obj, sqlStatement, statement, result, err)

		} else {

			var rowsaffected int64 = 0

			if rowsaffected, err = result.RowsAffected(); err != nil {

				prtcl.PrintObject(psql, obj, sqlStatement, statement, result, rowsaffected, err)

			} else {

				prtcl.Log.Println("deleted | updated rows: ", rowsaffected)
			}
		}
	}

	return
}

/*
// this function generates an "delete" statement, where the given id is used to delete the object
// from the given table. The function requires the tablename and the id ob the object.
//
// Parameters:
//   - `tblName` : string > the name of the table, where tho object is estimated
//   - `id` : string > id of the object
func (psql SQLite3) DeleteByID(tblName string, id string) (err error) {

	sqlStatement := "DELETE FROM " + tblName + " WHERE id=?;"

	var statement *sql.Stmt = nil

	if statement, err = psql.DB().Prepare(sqlStatement); err != nil {

		prtcl.PrintObject(psql, id, sqlStatement, statement, err)

	} else {

		var result sql.Result = nil

		if result, err = statement.Exec(id); err != nil {

			prtcl.PrintObject(psql, id, sqlStatement, statement, result, err)

		} else {

			var rowsaffected int64 = 0

			if rowsaffected, err = result.RowsAffected(); err != nil {

				prtcl.PrintObject(psql, id, sqlStatement, statement, result, rowsaffected, err)

			} else {

				prtcl.Log.Println("deleted | updated rows: ", rowsaffected)
			}
		}
	}

	return
}
*/
