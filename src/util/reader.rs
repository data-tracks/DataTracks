pub struct BufferedReader{
    string: String,
    position: usize
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
}

impl BufferedReader {
    pub(crate) fn new(string: String) -> BufferedReader {
        BufferedReader{ string, position: 0 }
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
}