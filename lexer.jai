Token_Type :: enum {
    ERROR;
    IDENT;
    STRING;
    NUMBER;
    L_PAREN;
    R_PAREN;
    SEMICOLON;
    COMMA;
    COMMENT;

    OP_ADD;
    OP_SUBTRACT;
    OP_MULTIPLY;
    OP_DIVIDE;
    OP_MODULO;

    OP_BIT_AND;
    OP_BIT_OR;
    OP_BIT_XOR;

    OP_EQUAL;
    OP_GRATER;
    OP_LESS;
    OP_GRATER_AND_EQUAL;
    OP_LESS_AND_EQUAL;
    OP_NOT_EQUAL;

    OP_ADD_EQUALS;
    OP_SUBTRACT_EQUALS;
    OP_MULTIPLY_EQUALS;
    OP_DIVIDE_EQUALS;
    OP_MODULO_EQUALS;
    OP_BIT_AND_EQUALS;
    OP_BIT_OR_EQUALS;
    OP_BIT_EXCLUSIVE_EQUALS;
}

Token :: struct {
    type: Token_Type = .ERROR;
    str:  string;
    line: s32;
    col:  s32;
}

Lexer :: struct {
    current_line_number: s32;
    current_column_number: s32;

    input: string;
    input_cursor: s64;

    tokens: [..]Token;
}

report_error :: (lexer: *Lexer, format: string, arguments: .. Any) {
    log(format, .. arguments);

    log("... at line %, character %.\n", lexer.current_line_number, lexer.current_column_number);
}

eat_character :: (using lexer: *Lexer) {
    if input.data[input_cursor] == #char "\n" {
        current_line_number += 1;
        current_column_number = 0;
    }

    input_cursor += 1;
    current_column_number += 1;
}

peek_next_character :: (lexer: *Lexer) -> u8, bool {
    if lexer.input_cursor >= lexer.input.count {
        return 0, false;
    }

    return lexer.input.data[lexer.input_cursor], true;
}

tokenize :: (input: string) -> []Token {
    lexer: Lexer;
    lexer.input = input;

    while 1 {
        c, ok := peek_next_character(*lexer);
        if !ok break;

        if is_space(c) {
            skip_space(*lexer);
        } else if is_alpha(c) {
            read_identifier(*lexer);
        } else if is_digit(c) {
            read_number(*lexer);
        } else if c == #char "'" || c == #char "\"" {
            read_string(*lexer);
        } else {
            read_operator(*lexer);
        }
    }

    return lexer.tokens;
}

skip_space :: (lexer: *Lexer) {
    while 1 {
        c, ok := peek_next_character(lexer);
        if !ok break;

        if is_space(c) {
            eat_character(lexer);
        } else {
            break;
        }
    }
}

read_identifier :: (lexer: *Lexer) {
    start_cursor := lexer.input_cursor;
    t := Token.{
        type = .IDENT,
        line = lexer.current_line_number,
        col  = lexer.current_column_number,
    };
    while 1 {
        c, ok := peek_next_character(lexer);
        if !ok break;

        // Before calling this function, the first char is guarentee
        // to be an alpha, is_alnum also include underscore
        if is_alnum(c) {
            eat_character(lexer);
        } else {
            break;
        }
    }
    t.str = slice(lexer.input, start_cursor, lexer.input_cursor - start_cursor);
    array_add(*lexer.tokens, t);
}

read_number :: (lexer: *Lexer) {
    start := lexer.input_cursor;
    t := Token.{
        type = .NUMBER,
        line = lexer.current_line_number,
        col  = lexer.current_column_number,
    };

    dot_count := 0;
    while 1 {
        c, ok := peek_next_character(lexer);
        if !ok break;

        if is_digit(c) {
            eat_character(lexer);
        } else if c == #char "." {
            dot_count += 1;
            if dot_count > 1 {
                report_error(lexer, "number with 2 dots");
                exit(-1);
            }
            eat_character(lexer);
        } else {
            break;
        }
    }

    c, ok := peek_next_character(lexer);

    if ok && (c == #char "E"|| c == #char "e") {
        eat_character(lexer);
        negative_count := 0;
        while 1 {
            c, ok := peek_next_character(lexer);
            if !ok break;

            if is_digit(c) {
                eat_character(lexer);
            } else if c == #char "-" {
                negative_count += 1;
                if negative_count > 1 {
                    report_error(lexer, "number with 2 minus");
                    exit(-1);
                }
                eat_character(lexer);
            } else {
                break;
            }
        }
    }

    t.str = slice(lexer.input, start, lexer.input_cursor - start);

    array_add(*lexer.tokens, t);
}

read_string :: (lexer: *Lexer) {
    quote_char, ok := peek_next_character(lexer);
    if !ok return; // This check is redundant

    start := lexer.input_cursor;
    t := Token.{
        type = .STRING,
        line = lexer.current_line_number,
        col  = lexer.current_column_number,
    };

    eat_character(lexer);

    prev_char: u8;
    while 1 {
        c, ok := peek_next_character(lexer);
        if !ok break;

        eat_character(lexer);
        if c == quote_char && prev_char != #char "\\" {
            break;
        }
        prev_char = c;
    }

    t.str = slice(lexer.input, start, lexer.input_cursor - start);
    array_add(*lexer.tokens, t);
}

read_operator :: (lexer: *Lexer) {
    c, ok := peek_next_character(lexer);
    if !ok return;

    start := lexer.input_cursor;
    t := Token.{
        line = lexer.current_line_number,
        col = lexer.current_column_number,
    };

    eat_character(lexer);
    t.str = slice(lexer.input, start, 1);

    nc := peek_next_character(lexer);

    if c == {
    case #char "(";
        t.type = .L_PAREN;
    case #char ")";
        t.type = .R_PAREN;
    case #char ";";
        t.type = .SEMICOLON;
    case #char "+";
        if nc == #char "=" {
            t.type = .OP_ADD_EQUALS;
            eat_character(lexer);
            t.str.count = 2;
        } else {
            t.type = .OP_ADD;
        }
    case #char "-";
        if nc == #char "=" {
            t.type = .OP_SUBTRACT_EQUALS;
            eat_character(lexer);
            t.str.count = 2;
        } else if nc == #char "-" {
            t.type = .COMMENT;
            eat_character(lexer);

            // This is a line comment, read until the end of line
            while 1 {
                comment_char, ok := peek_next_character(lexer);
                if !ok break;

                eat_character(lexer);
                if comment_char == #char "\n" {
                    break;
                }
            }
            t.str.count = lexer.input_cursor - start - 1;
        } else {
            t.type = .OP_SUBTRACT;
        }
    case #char "*";
        if nc == #char "=" {
            // Calling this as multiply is weird, * means a wildcard too.
            t.type = .OP_MULTIPLY_EQUALS;
            eat_character(lexer);
            t.str.count = 2;
        } else {
            t.type = .OP_MULTIPLY;
        }
    case #char "/";
        if nc == #char "=" {
            t.type = .OP_DIVIDE_EQUALS;
            eat_character(lexer);
            t.str.count = 2;
        } else if nc == #char "*" {
            // Block comment
            t.type = .COMMENT;
            while 1 {
                comment_char, ok := peek_next_character(lexer);
                if !ok break;

                eat_character(lexer);

                if comment_char == #char "*" {
                    next_char, ok := peek_next_character(lexer);
                    if !ok break;
                    if next_char == #char "/" {
                        eat_character(lexer);
                        break;
                    }
                }
            }
            t.str.count = lexer.input_cursor - start;

        } else {
            t.type = .OP_DIVIDE;
        }
    case #char "%";
        if nc == #char "=" {
            t.type = .OP_MODULO_EQUALS;
            eat_character(lexer);
            t.str.count = 2;
        } else {
            t.type = .OP_MODULO;
        }
    case #char "&";
        if nc == #char "=" {
            t.type = .OP_BIT_AND_EQUALS;
            eat_character(lexer);
            t.str.count = 2;
        } else {
            t.type = .OP_BIT_AND;
        }
    case #char "|";
        if nc == #char "=" {
            t.type = .OP_BIT_OR_EQUALS;
            eat_character(lexer);
            t.str.count = 2;
        } else {
            t.type = .OP_BIT_OR;
        }
    case #char "^";
        if nc == #char "=" {
            t.type = .OP_BIT_EXCLUSIVE_EQUALS;
            eat_character(lexer);
            t.str.count = 2;
        } else {
            t.type = .OP_BIT_XOR;
        }
    case #char "<";
        if nc == #char "=" {
            t.type = .OP_LESS_AND_EQUAL;
            eat_character(lexer);
            t.str.count = 2;
        } else if nc == #char ">" {
            t.type = .OP_NOT_EQUAL;
            eat_character(lexer);
            t.str.count = 2;
        } else {
            t.type = .OP_LESS;
        }
    case #char ">";
        if nc == #char "=" {
            t.type = .OP_GRATER_AND_EQUAL;
            eat_character(lexer);
            t.str.count = 2;
        } else {
            t.type = .OP_GRATER;
        }
    case #char "=";
        t.type = .OP_EQUAL;
    case #char ",";
        t.type = .COMMA;
    case;
        report_error(lexer, "Unsupported token");
    }

    array_add(*lexer.tokens, t);
}

#import "String";
#import "Basic";
