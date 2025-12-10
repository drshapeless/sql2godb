main :: () {
    ok, args := parse_arguments(Argument);

    files := file_list(args.input_dir);

    for files {
        if ends_with(it, ".up.sql") {
            content := parse_sql(it);
            write_entire_file(sprint("%/%.go", args.output_dir, get_go_filename(it)), content);
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
}

Schema :: struct {
    name: string;
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

    for sc: parser.schemas {
        write_struct(*sb, sc);
    }

    return builder_to_string(*sb);
}

COMMON_HEADER :: #string DONE
package data

import (
    "context"
    "database/sql"

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

snake_to_pascal :: (s: string) -> string {
    should_be_cap := true;
    sb: String_Builder;
    for c: s {
        if c == #char "_" {
            should_be_cap = true;
        } else if should_be_cap {
            append(*sb, to_upper(c));
            should_be_cap = false;
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

write_struct :: (sb: *String_Builder, schema: Schema) {
    print(sb, "type % struct {\n", plural_to_single(snake_to_pascal(schema.name)));
    for m: schema.members {
        print(sb, "\t%\t%\n", snake_to_pascal(m.name), m.type);
    }
    print(sb, "}\n");
}

get_go_filename :: (name: string) -> string {
    CREATE :: "create_";
    TABLE_UP :: "_table.up.sql";

    i := find_index_from_left(name, CREATE);

    s := slice(name, i + CREATE.count, name.count - i - CREATE.count - TABLE_UP.count);

    return sprint("%.go", s);
}

#import "Basic";
#import "File";
#import "File_Utilities";
#import,file "lexer.jai";
#import "String";
#import "Command_Line";
