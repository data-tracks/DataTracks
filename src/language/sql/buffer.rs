use logos::Lexer;

use crate::language::sql::lex::Token;

pub struct BufferedLexer<'source> {
    lexer: &'source mut Lexer<'source, Token>,
    buffer: Vec<Token>,
    error: Option<()>,
}

impl<'source> BufferedLexer<'source> {
    // pops and returns the oldest token, which was stored on the buffer
    pub(crate) fn consume_buffer(&mut self) -> Result<Token, String> {
        match self.buffer.pop() {
            None => Err("Not enough tokens".to_string()),
            Some(t) => Ok(t)
        }
    }
    pub(crate) fn buffer(&mut self, token: Token) {
        self.buffer.push(token);
    }

    pub fn next(&mut self) -> Result<Token, String> {
        if !self.buffer.is_empty() {
            return self.consume_buffer()
        }
        match self.lexer.next() {
            None => Err("No more values".to_string()),
            Some(res) => match res {
                Ok(tok) => Ok(tok),
                Err(e) => {
                    self.error = Some(e);
                    Err("Error while tokenizing query".to_string())
                }
            }
        }
    }

    pub fn next_buf(&mut self) -> Result<Token, String> {
        let tok = self.next();
        if let Ok(tok) = tok {
            self.buffer.push(tok.clone());
            Ok(tok)
        } else {
            tok
        }
    }


    pub(crate) fn new(lexer: &'source mut Lexer<'source, Token>) -> Self {
        BufferedLexer { lexer, buffer: vec![], error: None }
    }
}