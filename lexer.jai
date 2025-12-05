Token_Type :: enum {
    ERROR;
    IDENT;
    STRING;
    NUMBER;
    OPERATOR;
    L_PAREN;
    R_PAREN;
    SEMICOLON;
    COMMENT;
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
    current_character_index += 1;
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
        } else if c == {
        case #char "\"";
            #through;
        case #char "'";
            read_string(*lexer);
        }
    }
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
        }
    }
    t.str = slice(lexer.input, start_cursor, lexer.input_cursor - start_cursor + 1);
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

    t.str = slice(lexer.input, start, lexer.input_cursor - start + 1);

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

    t.str = slice(lexer.input, start, lexer.input_cursor - start + 1);
    array_add(*lexer.tokens, t);
}

#import "String";
#import "Basic";
