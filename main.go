package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

const VERSION = "v0.1.3"

type Item struct {
	Name    string
	Type    string
	NotNull bool
}

type SqlType struct {
	TableName string
	Items     []Item
}

func snake_to_pascal(snakeStr string) string {
	// Split the string by underscore
	words := strings.Split(snakeStr, "_")

	// Create a new slice to hold the capitalized words
	var pascalWords []string

	// Capitalize each word
	for _, word := range words {
		if len(word) > 0 {
			caser := cases.Title(language.AmericanEnglish)
			capitalizedWord := caser.String(word) // Capitalize the first letter
			pascalWords = append(pascalWords, capitalizedWord)
		}
	}

	s := strings.Join(pascalWords, "")
	s = strings.ReplaceAll(s, "Id", "ID")

	// Join the capitalized words together
	return s
}

func remove_trailing_comma(s string) string {
	return strings.TrimSuffix(s, ",")
}

func sqltype_to_gotype(sql_type_name string) string {
	real_name := remove_trailing_comma(sql_type_name)

	switch real_name {
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

func (st *SqlType) typeStruct() string {
	format := `type %s struct {
%s
}
`

	itemStr := ""

	for _, v := range st.Items {
		itemStr += "\t"
		itemStr += snake_to_pascal(v.Name)
		itemStr += "\t"
		if !v.NotNull {
			itemStr += "*"
		}
		itemStr += sqltype_to_gotype(v.Type)
		itemStr += "\t"
		itemStr += fmt.Sprintf("`db:\"%s\"`", v.Name)
		itemStr += "\n"
	}

	t := snake_to_pascal(plural_to_single(st.TableName))

	s := fmt.Sprintf(format, t, itemStr)
	return s
}

func (st *SqlType) createFunc() string {
	s := ""

	single := plural_to_single(st.TableName)
	pascal := snake_to_pascal(single)

	s += fmt.Sprintf("func Create%s(%s *%s, db DB) error {\n", pascal, single, pascal)

	sql := "\tq := `INSERT INTO %s (%s)\nVALUES (%s)\n%s`\n\n"

	columns := []string{}
	numbers := []string{}

	i := 1
	hasID := false
	hasVersion := false
	for _, v := range st.Items {
		if v.Name == "id" {
			hasID = true
			continue
		}

		if v.Name == "version" {
			hasVersion = true
			continue
		}

		if v.Name == "created_at" || v.Name == "edited_at" {
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

	s += fmt.Sprintf(sql, st.TableName, columnStr, numberStr, returnStr)

	s += "\tctx, cancel := context.WithTimeout(context.Background(), time.Second*3)\n\tdefer cancel()\n\n"

	db_query := "\terr := db.QueryRow(ctx, q, %s).Scan(%s)\n\n"

	query_fields := []string{}
	scan_fields := []string{}
	for _, v := range st.Items {
		if v.Name == "version" || v.Name == "id" {
			scan_fields = append(scan_fields, fmt.Sprintf("&%s.%s", single, snake_to_pascal(v.Name)))
			continue
		}

		query_fields = append(query_fields, fmt.Sprintf("%s.%s", single, snake_to_pascal(v.Name)))
	}

	s += fmt.Sprintf(db_query, strings.Join(query_fields, ", "), strings.Join(scan_fields, ", "))

	s += "\tif err != nil {\n\t\treturn err\n\t}\n\n"

	s += "\treturn nil\n"

	s += "}\n"

	return s
}

func (st *SqlType) getFunc() string {
	hasID := false
	for _, v := range st.Items {
		if v.Name == "id" {
			hasID = true
		}
	}

	if !hasID {
		return ""
	}

	s := ""

	single := plural_to_single(st.TableName)
	pascal := snake_to_pascal(single)

	s += fmt.Sprintf("func Get%s(id int64, db DB) (*%s, error) {\n", pascal, pascal)

	s += fmt.Sprintf("\tq := `SELECT * FROM %s WHERE id = $1`\n\n", st.TableName)

	s += "\tctx, cancel := context.WithTimeout(context.Background(), time.Second*3)\n\tdefer cancel()\n\n"

	s += "\trows, err := db.Query(ctx, q, id)\n\tif err != nil {\n\t\treturn nil, err\n\t}\n\n"

	s += fmt.Sprintf("\t%s, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[%s])\n", single, pascal)

	s += "\tif err != nil {\n\t\treturn nil, err\n\t}\n\n"

	s += fmt.Sprintf("\treturn &%s, nil\n", single)

	s += "}\n"

	return s
}

func (st *SqlType) updateFunc() string {
	s := ""

	single := plural_to_single(st.TableName)
	pascal := snake_to_pascal(single)

	s += fmt.Sprintf("func Update%s(%s *%s, db DB) error {\n", pascal, single, pascal)

	query := "\tq := `UPDATE %s\nSET %s\nWHERE %s\n%s`\n\n"

	columns := []string{}
	i := 1
	hasID := false
	hasVersion := false
	for _, v := range st.Items {
		if v.Name == "id" {
			hasID = true
		}

		if v.Name == "version" {
			hasVersion = true
			continue
		}

		if v.Name == "created_at" || v.Name == "created_by" {
			continue
		}

		if v.Name == "edited_at" {
			columns = append(columns, "edited_at = NOW()")
			continue
		}

		columns = append(columns, fmt.Sprintf("%s = $%d", v.Name, i))
		i += 1
	}

	if !hasID {
		return ""
	}

	columnStr := strings.Join(columns, ", ")
	if hasVersion {
		columnStr += ", version = version + 1"
	}
	whereStr := fmt.Sprintf("id = $%d", i)
	i += 1
	whereStr += fmt.Sprintf(" AND version = $%d", i)

	returnStr := ""
	if hasVersion {
		returnStr = "version"
	} else {
		return ""
	}
	if returnStr != "" {
		returnStr = "RETURNING " + returnStr
	}

	s += fmt.Sprintf(query, st.TableName, columnStr, whereStr, returnStr)

	s += "\tctx, cancel := context.WithTimeout(context.Background(), time.Second*3)\n\tdefer cancel()\n\n"

	query_row := "\terr := db.QueryRow(ctx, q, %s).Scan(%s)\n\n"

	fields := []string{}
	for _, v := range st.Items {
		if v.Name == "version" || v.Name == "id" || v.Name == "created_at" || v.Name == "created_by" || v.Name == "edited_at" {
			continue
		}

		fields = append(fields, fmt.Sprintf("%s.%s", single, snake_to_pascal(v.Name)))
	}
	if hasID {
		fields = append(fields, fmt.Sprintf("%s.ID", single))
	}
	if hasVersion {
		fields = append(fields, fmt.Sprintf("%s.Version", single))
	}

	s += fmt.Sprintf(query_row, strings.Join(fields, ", "), fmt.Sprintf("&%s.Version", single))

	s += "\tif err != nil {\n\t\treturn err\n\t}\n\n"

	s += "\treturn nil\n"

	s += "}\n"

	return s
}

func (st *SqlType) deleteFunc() string {
	hasID := false
	for _, v := range st.Items {
		if v.Name == "id" {
			hasID = true
		}
	}

	if !hasID {
		return ""
	}

	s := ""

	single := plural_to_single(st.TableName)
	pascal := snake_to_pascal(single)

	s += fmt.Sprintf("func Delete%s(id int64, db DB) error {\n", pascal)
	s += fmt.Sprintf("\tq := `DELETE FROM %s WHERE id = $1`\n\n", st.TableName)

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
	version := flag.Bool("version", false, "show version")
	input_file := flag.String("i", "", "input file path")
	output_file := flag.String("o", "", "output file path")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "sql2godb - SQL to Go code generator\n\n")
		fmt.Fprintf(os.Stderr, "Usage: sql2godb [-i] [-o]\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nIf no input or output file is specified, stdin and stdout will be used.\n")
	}

	flag.Parse()

	if *version {
		fmt.Printf("sql2godb %s", VERSION)
		return
	}

	ifile := os.Stdin
	ofile := os.Stdout

	if *input_file != "" {
		file, err := os.OpenFile(*input_file, os.O_RDONLY, 0644)
		if err != nil {
			fmt.Errorf("Cannot open file %v\n", err)
			return
		}

		ifile = file
	}

	if *output_file != "" {
		file, err := os.OpenFile(*output_file, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			fmt.Errorf("Cannot open file %v", err)
			return
		}

		ofile = file
	}

	data, err := io.ReadAll(ifile)
	if err != nil {
		panic("stdin is fucked")
	}

	my_string := string(data)

	sql_type := SqlType{}

	var buffer bytes.Buffer

	fmt.Fprint(&buffer, "package data\n\n")
	is_in_type := false

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
			is_in_type = true
			continue
		}

		if strings.HasPrefix(line, ");") {
			if is_in_type {
				final_string := ""
				final_string += sql_type.typeStruct() + "\n"
				final_string += sql_type.createFunc() + "\n"
				final_string += sql_type.getFunc() + "\n"
				final_string += sql_type.updateFunc() + "\n"
				final_string += sql_type.deleteFunc() + "\n"
				fmt.Fprint(&buffer, final_string)
				is_in_type = false
				sql_type = SqlType{}
			}

			continue
		}

		trimmed_line := strings.TrimLeft(line, " ")
		if strings.HasPrefix(trimmed_line, "PRIMARY KEY") || strings.HasPrefix(trimmed_line, "UNIQUE") {
			continue
		}

		is_not_null := strings.Contains(line, "NOT NULL") || strings.Contains(line, "PRIMARY KEY")

		words := strings.Split(strings.TrimSpace(line), " ")
		p := Item{
			Name:    words[0],
			Type:    words[1],
			NotNull: is_not_null,
		}
		sql_type.Items = append(sql_type.Items, p)
	}

	fmt.Fprint(ofile, buffer.String())
}
