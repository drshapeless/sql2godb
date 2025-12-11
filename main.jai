main :: () {
    ok, args := parse_arguments(Argument);

    files := file_list(args.input_dir);

    for files {
        if ends_with(it, ".up.sql") {
            content := parse_sql(it);
            write_entire_file(sprint("%/%", args.output_dir, get_go_filename(it)), content);
        }
    }
}

Argument :: struct {
    input_dir: string; @"?Input directory"
    output_dir: string; @"?Output directory"
}

Schema_Member :: struct {
    name: string;
    type: string;
    go_name: string;
}

Schema :: struct {
    name: string;
    go_name: string;
    var_name: string;
    members: []Schema_Member;
}

Parser :: struct {
    tokens: []Token;
    pos: int;
    schemas: [..]Schema;
}

parse_sql :: (file_name: string) -> string {
    s := read_entire_file(file_name);

    tokens := tokenize(s);

    parser := Parser.{
        tokens = tokens
    };

    while parser.pos < parser.tokens.count {
        skip_comments(*parser);
        read_schema(*parser);
        skip_comments(*parser);
    }

    sb: String_Builder;
    print(*sb, COMMON_HEADER);

    for * sc: parser.schemas {
        fill_names(sc);
        write_struct(*sb, sc);
        write_create_func(*sb, sc);
        write_get_func(*sb, sc);
        write_update_func(*sb, sc);
        write_delete_func(*sb, sc);
    }

    return builder_to_string(*sb);
}

COMMON_HEADER :: #string DONE
package data

import (
    "context"
    "database/sql"
    "time"

    "github.com/georgysavva/scany/v2/sqlscan"
)

DONE

read_schema :: (using parser: *Parser) {
    if tokens.count - pos < 2 {
        pos = tokens.count;
        return;
    }

    if tokens[pos].type == .KEYWORD_CREATE && tokens[pos + 1].type == .KEYWORD_TABLE {
        schema: Schema;

        pos += 2;
        while pos < tokens.count {
            if tokens[pos].type == .IDENT {
                schema.name = tokens[pos].str;
                break;
            }
            pos += 1;
        }

        skip_pass_token_type(parser, .L_PAREN);

        semicolon_index := find_token(parser, .SEMICOLON);
        assert(semicolon_index >= 0);

        members: [..]Schema_Member;
        while 1 {
            m: Schema_Member;
            // This must be an identifier, skip the check
            m.name = tokens[pos].str;

            if to_upper_copy(tokens[pos + 1].str) == {
            case "TEXT";
                m.type = "string";
            case "INTEGER";
                m.type = "int64";
            case "REAL";
                m.type = "float64";
            case "BLOB";
                m.type = "[]byte";
            case;
                assert(false, "Invalid type");
            }

            array_add(*members, m);

            comma_index := find_token(parser, .COMMA);

            if comma_index < 0 || comma_index > semicolon_index {
                break;
            } else {
                pos = comma_index + 1;
            }
        }

        schema.members = members;
        array_add(*schemas, schema);
    }

    skip_pass_token_type(parser, .SEMICOLON);
}

// If not found, return -1
find_token :: (parser: *Parser, type: Token_Type) -> int {
    pos := parser.pos;

    while pos < parser.tokens.count {
        if parser.tokens[pos].type == type {
            return pos;
        }

        pos += 1;
    }

    return -1;
}

skip_pass_token_type :: (using parser: *Parser, type: Token_Type) {
    while pos < tokens.count {
        if tokens[pos].type == type {
            break;
        }
        pos += 1;
    }

    parser.pos += 1;
}

skip_comments :: (using parser: *Parser) {
    while pos < tokens.count {
        if tokens[pos].type == .LINE_COMMENT || tokens[pos].type == .BLOCK_COMMENT {
            pos += 1;
        } else {
            break;
        }
    }
}

fill_names :: (sc: *Schema) {
    sc.go_name = plural_to_single(snake_to_pascal(sc.name));
    vname := copy_string(sc.go_name);
    vname[0] = to_lower(vname[0]);
    sc.var_name = plural_to_single(vname);

    for * m: sc.members {
        m.go_name = snake_to_pascal(m.name);
    }
}

snake_to_pascal :: (s: string) -> string {
    should_be_cap := true;
    sb: String_Builder;
    for c: s {
        if c == #char "_" {
            should_be_cap = true;
        } else if should_be_cap {
            append(*sb, to_upper(c));
            should_be_cap = false;
        } else if c == #char "d" && it_index > 0 && s[it_index - 1] == #char "i" && (it_index + 1 == s.count || s[it_index + 1] == #char "_") {
            append(*sb, to_upper(c));
        } else {
            append(*sb, c);
        }
    }

    return builder_to_string(*sb);
}

plural_to_single :: (s: string) -> string {
    if ends_with(s, "s") {
        return trim_right(s, "s");
    }

    return s;
}

write_struct :: (sb: *String_Builder, sc: *Schema) {
    print(sb, "type % struct {\n", sc.go_name);
    for m: sc.members {
        print(sb, "\t%\t% `db:\"%\"`\n", m.go_name, m.type, m.name);
    }
    print(sb, "}\n\n");
}

get_go_filename :: (name: string) -> string {
    CREATE :: "create_";
    TABLE_UP :: "_table.up.sql";

    i := find_index_from_left(name, CREATE);

    s := slice(name, i + CREATE.count, name.count - i - CREATE.count - TABLE_UP.count);

    return sprint("%.go", s);
}

EXCLUDE_MEMBERS :: string.[
"id",
"created_at",
"updated_at",
"version"
];

RETURN_MEMBERS :: string.[
"id",
"version"
];

match_strings :: (s: string, matches: []string) -> bool {
    for matches {
        if s == it {
            return true;
        }
    }

    return false;
}

CONTEXT_STR :: #string DONE
    ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
    defer cancel()
DONE

EN_STR :: #string DONE
    if err != nil {
        return err
    }
    return nil
DONE

repeat_string :: (s: string, count: int, separator := ", ") -> string {
    sb: String_Builder;
    i := count;
    while i > 0 {
        append(*sb, s);
        if i > 1 {
            append(*sb, separator);
        }
        i -= 1;
    }

    return builder_to_string(*sb);
}

join_sql_name :: (mems: []Schema_Member, suffix := "", separator := ", ") -> string {
    sb: String_Builder;

    for mems {
        append(*sb, it.name);
        append(*sb, suffix);
        if it_index < mems.count - 1 {
            append(*sb, separator);
        }
    }

    return builder_to_string(*sb);
}

join_go_name :: (mems: []Schema_Member, prefix: string = "") -> string {
    sb: String_Builder;

    for mems {
        append(*sb, prefix);
        append(*sb, it.go_name);
        if it_index < mems.count - 1 {
            append(*sb, ", ");
        }
    }
    return builder_to_string(*sb);
}

write_create_func :: (sb: *String_Builder, sc: *Schema) {
    useful_mems: [..]Schema_Member;
    ret_mems: [..]Schema_Member;
    for sc.members {
        if !match_strings(it.name, EXCLUDE_MEMBERS) {
            array_add(*useful_mems, it);
        }

        if match_strings(it.name, RETURN_MEMBERS) {
            array_add(*ret_mems, it);
        }
    }

    print(sb, "func Create%(% *%, db *sql.DB) error {\n", sc.go_name, sc.var_name, sc.go_name);
    print(sb, "    q := `INSERT INTO % (%) ", sc.name, join_sql_name(useful_mems));
    print(sb, "VALUES (%) ", repeat_string("?", useful_mems.count));
    print(sb, "RETURNING %`\n\n", join_sql_name(ret_mems));

    append(sb, CONTEXT_STR);
    append(sb, "\n");

    print(sb, "    err := db.QueryRowContext(ctx, q, %)", join_go_name(useful_mems, sprint("%.", sc.var_name)));

    print(sb, ".Scan(%)\n\n", join_go_name(ret_mems, sprint("&%.", sc.var_name)));
    append(sb, EN_STR);
    append(sb, "}\n\n");
}

ROWS_STR :: #string DONE
    rows, err := db.QueryContext(ctx, q, id)
    if err != nil {
        return nil, err
    }
DONE

write_get_func :: (sb: *String_Builder, sc: *Schema) {
    // This function assume every schema has a field named "id"
    id_mem: Schema_Member;
    for sc.members {
        if it.name == "id" {
            id_mem = it;
        }
    }

    print(sb, "func Get%(% %, db *sql.DB) (*%, error) {\n", sc.go_name, id_mem.name, id_mem.type, sc.go_name);

    print(sb, "    q := `SELECT * FROM % WHERE id = ?`\n\n", sc.name);
    append(sb, CONTEXT_STR);
    append(sb, "\n");

    append(sb, ROWS_STR);

    print(sb, "    var % %\n", sc.var_name, sc.go_name);
    print(sb, "    err = sqlscan.ScanOne(&%, rows)\n", sc.var_name);
    append(sb, "    if err != nil {\n        return nil, err\n    }\n\n");
    print(sb, "    return &%, nil\n", sc.var_name);

    append(sb, "}\n\n");
}

write_update_func :: (sb: *String_Builder, sc: *Schema) {
    // This function assume the sc has id and version
    useful_mems: [..]Schema_Member;
    ret_mems: [..]Schema_Member;
    for sc.members {
        if !match_strings(it.name, EXCLUDE_MEMBERS) {
            array_add(*useful_mems, it);
        }

        if match_strings(it.name, RETURN_MEMBERS) {
            array_add(*ret_mems, it);
        }
    }

    print(sb, "func Update%(% *%, db *sql.DB) error {\n", sc.go_name, sc.var_name, sc.go_name);
    print(sb, "    q := `UPDATE % SET %, version = version + 1 WHERE % RETURNING version`", sc.name, join_sql_name(useful_mems, " = ?"), join_sql_name(ret_mems, " = ?", " AND "));

    append(sb, "\n\n");
    append(sb, CONTEXT_STR);
    append(sb, "\n");

    print(sb, "    err := db.QueryRowContext(ctx, q, %, %.Version).Scan(&%.Version)", join_go_name(useful_mems, sprint("%.", sc.var_name)), sc.var_name, sc.var_name);

    append(sb, "\n\n");

    append(sb, "    if err != nil {\n        return err\n    }\n\n");
    append(sb, "    return nil\n");
    append(sb, "}\n\n");
}

write_delete_func :: (sb: *String_Builder, sc: *Schema) {
    // This function assume every schema has a field named "id"
    id_mem: Schema_Member;
    for sc.members {
        if it.name == "id" {
            id_mem = it;
        }
    }

    print(sb, "func Delete%(id %, db *sql.DB) error {\n", sc.go_name, id_mem.type);
    print(sb, "    q := `DELETE FROM % WHERE id = ?`\n\n", sc.name);

    append(sb, CONTEXT_STR);
    append(sb, "\n\n");

    append(sb, "    result, err := db.ExecContext(ctx, q, id)\n");
    append(sb, "    if err != nil {\n        return err\n    }\n\n");

    append(sb, "    rowsAffected, _ := result.RowsAffected()\n\n");
    append(sb, "    if rowsAffected == 0 {\n        return sql.ErrNoRows\n    }\n\n");
    append(sb, "    return nil\n");
    append(sb, "}\n");
}

#import "Basic";
#import "File";
#import "File_Utilities";
#import,file "lexer.jai";
#import "String";
#import "Command_Line";
