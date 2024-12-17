package main

import (
	"fmt"
	"io"
	"os"
	"strings"
)

type Pair struct {
	Name string
	Type string
}

type SqlType struct {
	TableName string
	Items     []Pair
}

func snake_to_pascal(snakeStr string) string {
	// Split the string by underscore
	words := strings.Split(snakeStr, "_")

	// Create a new slice to hold the capitalized words
	var pascalWords []string

	// Capitalize each word
	for _, word := range words {
		if len(word) > 0 {
			capitalizedWord := strings.Title(word) // Capitalize the first letter
			pascalWords = append(pascalWords, capitalizedWord)
		}
	}

	s := strings.Join(pascalWords, "")
	s = strings.ReplaceAll(s, "Id", "ID")

	// Join the capitalized words together
	return s
}

func sqltype_to_gotype(sql_type_name string) string {
	switch sql_type_name {
	case "bigserial":
		fallthrough
	case "bigint":
		return "int64"
	case "int":
		return "int32"
	case "text":
		return "string"
	case "date":
		fallthrough
	case "time":
		fallthrough
	case "timestamp(0)":
		return "time.Time"
	case "uuid":
		return "pgxuuid.uuid"
	case "boolean":
		return "bool"

	default:
		panic(fmt.Sprintf("No conversion for type '%s'", sql_type_name))
	}
}

func plural_to_single(s string) string {
	return strings.TrimSuffix(s, "s")
}

func (self *SqlType) typeStruct() string {
	format := `type %s struct {
%s
}
`

	itemStr := ""

	for _, v := range self.Items {
		itemStr += "\t"
		itemStr += snake_to_pascal(v.Name)
		itemStr += "\t"
		itemStr += sqltype_to_gotype(v.Type)
		itemStr += "\t"
		itemStr += fmt.Sprintf("`db:\"%s\"`", v.Name)
		itemStr += "\n"
	}

	t := snake_to_pascal(plural_to_single(self.TableName))

	s := fmt.Sprintf(format, t, itemStr)
	return s
}

func (self *SqlType) createFunc() string {
	s := ""

	single := plural_to_single(self.TableName)
	pascal := snake_to_pascal(single)

	s += fmt.Sprintf("func Create%s(%s *%s, db DB) error {\n", pascal, single, pascal)

	sql := "\tq := `INSERT INTO %s (%s)\nVALUES (%s)\n%s`\n\n"

	columns := []string{}
	numbers := []string{}

	i := 1
	hasID := false
	hasVersion := false
	for _, v := range self.Items {
		if v.Name == "id" {
			hasID = true
			continue
		}

		if v.Name == "version" {
			hasVersion = true
			continue
		}

		columns = append(columns, v.Name)
		numbers = append(numbers, fmt.Sprintf("$%d", i))
		i += 1
	}

	columnStr := strings.Join(columns, ", ")
	numberStr := strings.Join(numbers, ", ")

	returnStr := ""
	if hasID && hasVersion {
		returnStr = "id, version"
	} else if hasID {
		returnStr = "id"
	} else if hasVersion {
		returnStr = "version"
	}

	if returnStr != "" {
		returnStr = "RETURNING " + returnStr
	}

	s += fmt.Sprintf(sql, self.TableName, columnStr, numberStr, returnStr)

	s += "\tctx, cancel := context.WithTimeout(context.Background(), time.Second*3)\n\tdefer cancel()\n\n"

	db_query := "\terr := db.QueryRow(ctx, q, %s).Scan(%s)\n\n"

	query_fields := []string{}
	scan_fields := []string{}
	for _, v := range self.Items {
		if v.Name == "version" || v.Name == "id" {
			scan_fields = append(scan_fields, fmt.Sprintf("&%s.%s", single, pascal))
			continue
		}

		query_fields = append(query_fields, fmt.Sprintf("%s.%s", single, v.Name))
	}

	s += fmt.Sprintf(db_query, strings.Join(query_fields, ", "), strings.Join(scan_fields, ", "))

	s += "\tif err != nil {\n\t\treturn err\n\t}\n\n"

	s += "\treturn nil\n"

	s += "}\n"

	return s
}

func (self *SqlType) getFunc() string {
	s := ""

	single := plural_to_single(self.TableName)
	pascal := snake_to_pascal(single)

	s += fmt.Sprintf("func Get%s(id int64, db DB) (*%s, error) {\n", pascal, pascal)

	s += fmt.Sprintf("\tq := `SELECT * FROM %s WHERE id = $1`\n\n", self.TableName)

	s += "\tctx, cancel := context.WithTimeout(context.Background(), time.Second*3)\n\tdefer cancel()\n\n"

	s += "\trows, err := db.Query(ctx, q, id)\n\tif err != nil {\n\t\treturn nil, err\n\t}\n\n"

	s += fmt.Sprintf("\t%s, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[%s])\n", single, pascal)

	s += "\tif err != nil {\n\t\treturn nil, err\n\t}\n\n"

	s += fmt.Sprintf("\treturn &%s, nil\n", single)

	s += "}\n"

	return s
}

func (self *SqlType) updateFunc() string {
	s := ""

	single := plural_to_single(self.TableName)
	pascal := snake_to_pascal(single)

	s += fmt.Sprintf("func Update%s(%s *%s, db DB) error {\n", pascal, single, pascal)

	query := "\tq := `UPDATE %s\nSET %s\nWHERE %s\nRETURNING version`\n\n"

	columns := []string{}
	i := 1
	for _, v := range self.Items {
		if v.Name == "id" {
			continue
		}

		if v.Name == "version" {
			continue
		}

		columns = append(columns, fmt.Sprintf("%s = $%d", v.Name, i))
		i += 1
	}

	columnStr := strings.Join(columns, ", ")
	columnStr += ", version = version + 1"
	whereStr := fmt.Sprintf("id = $%d", i)
	i += 1
	whereStr += fmt.Sprintf(" AND version = $%d", i)

	s += fmt.Sprintf(query, self.TableName, columnStr, whereStr)

	s += "\tctx, cancel := context.WithTimeout(context.Background(), time.Second*3)\n\tdefer cancel()\n\n"

	query_row := "\terr := db.QueryRow(ctx, q, %s).Scan(%s)\n\n"

	fields := []string{}
	for _, v := range self.Items {
		if v.Name == "version" || v.Name == "id" {
			continue
		}

		fields = append(fields, fmt.Sprintf("%s.%s", single, snake_to_pascal(v.Name)))
	}
	fields = append(fields, fmt.Sprintf("%s.ID", single))
	fields = append(fields, fmt.Sprintf("%s.Version", single))

	s += fmt.Sprintf(query_row, strings.Join(fields, ", "), fmt.Sprintf("&%s.Version", single))

	s += "\tif err != nil {\n\t\treturn err\n\t}\n\n"

	s += "\treturn nil\n"

	s += "}\n"

	return s
}

func (self *SqlType) deleteFunc() string {
	s := ""

	single := plural_to_single(self.TableName)
	pascal := snake_to_pascal(single)

	s += fmt.Sprintf("func Delete%s(id int64, db DB) error {\n", pascal)
	s += fmt.Sprintf("\tq := `DELETE FROM %s WHERE id = $1`\n\n", self.TableName)

	s += "\tctx, cancel := context.WithTimeout(context.Background(), time.Second*3)\n\tdefer cancel()\n\n"

	s += "\tresult, err := db.Exec(ctx, q, id)\n"
	s += "\tif err != nil {\n\t\treturn err\n\t}\n\n"

	s += "\trowsAffected := result.RowsAffected()\n\n"

	s += "\tif rowsAffected == 0 {\n\t\treturn pgx.ErrNoRows\n\t}\n\n"

	s += "\treturn nil\n"
	s += "}\n"

	return s
}

func main() {
	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		panic("stdin is fucked")
	}

	my_string := string(data)

	sql_type := SqlType{}

	for _, line := range strings.Split(my_string, "\n") {
		if line == "" {
			continue
		}

		if strings.HasPrefix(line, "--") {
			continue
		}

		if strings.HasPrefix(line, "CREATE") {
			words := strings.Split(line, " ")
			sql_type.TableName = words[len(words)-2]
			continue
		}

		if strings.HasPrefix(line, ");") {
			break
		}

		words := strings.Split(strings.TrimSpace(line), " ")
		p := Pair{
			Name: words[0],
			Type: words[1],
		}
		sql_type.Items = append(sql_type.Items, p)
	}

	final_string := "package data\n\n"
	final_string += sql_type.typeStruct() + "\n"
	final_string += sql_type.createFunc() + "\n"
	final_string += sql_type.getFunc() + "\n"
	final_string += sql_type.updateFunc() + "\n"
	final_string += sql_type.deleteFunc() + "\n"
	fmt.Print(final_string)
}
