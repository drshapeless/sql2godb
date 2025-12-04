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
            assert(dot_count <= 1, "Error at % %, number with 2 dots", t.line, t.col);
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
                assert(negative_count <= 1, "Error at % %, number with 2 minus", t.line, t.col);
                eat_character(lexer);
            } else {
                break;
            }
        }
    }

    t.str = slice(lexer.input, start, lexer.input_cursor - start + 1);

    array_add(*lexer.tokens, t);
}

#import "String";
#import "Basic";
