pub struct BufferedReader {
    string: String,
    position: usize,
}

impl BufferedReader {
    pub(crate) fn consume_until(&mut self, stop: char) -> String {
        let mut temp = String::default();
        while let Some(char) = self.next() {
            if char == stop {
                return temp;
            }
            temp.push(char);
        }
        temp
    }

    pub(crate) fn consume_if_next(&mut self, char: char) {
        self.consume_spaces();

        if let Some(c) = self.peek_next() {
            if c == char {
                self.next();
                self.consume_spaces();
            }
        }
    }
}

impl BufferedReader {
    pub(crate) fn new(string: String) -> BufferedReader {
        BufferedReader {
            string,
            position: 0,
        }
    }

    pub(crate) fn next(&mut self) -> Option<char> {
        self.position += 1;

        self.string.chars().nth(self.position - 1)
    }
    pub(crate) fn peek_next(&mut self) -> Option<char> {
        self.peek(1)
    }

    pub(crate) fn peek(&mut self, pos: usize) -> Option<char> {
        self.string.chars().nth(self.position + pos - 1)
    }

    pub(crate) fn consume_spaces(&mut self) {
        while let Some(char) = self.peek_next() {
            if char == ' ' {
                self.next();
            } else {
                return;
            }
        }
    }
}
