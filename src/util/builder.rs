pub struct StringBuilder {
    string: String,
}

impl StringBuilder {
    pub fn new() -> Self {
        StringBuilder{ string: String::new() }
    }

    pub fn build(&mut self) -> String {
        self.string.clone()
    }

    pub fn build_and_clear(&mut self) -> String {
        let result = self.build();
        self.clear();
        result
    }

    pub fn append(&mut self, s: char) {
        self.string.push(s);
    }

    pub fn clear(&mut self) {
        self.string.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.string.is_empty()
    }
}