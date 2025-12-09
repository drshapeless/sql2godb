main :: () {
    files := file_list("migrations");

    for files {
        if ends_with(it, ".up.sql") {
            parse_sql(it);
        }
    }
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

parse_sql :: (file_name: string) {
    s := read_entire_file(file_name);

    tokens := tokenize(s);

    parser := Parser.{
        tokens = tokens
    };

    while parser.pos < parser.tokens.count {
        read_schema(*parser);
        skip_comments(*parser);
    }

    for parser.schemas {
        log("%", it);
    }
}

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

#import "Basic";
#import "File";
#import "File_Utilities";
#import,file "lexer.jai";
#import "String";
