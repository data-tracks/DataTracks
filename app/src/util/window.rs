use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
pub enum WindowType {
    Thumbling,
}

impl Display for WindowType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            WindowType::Thumbling => f.write_str("THUMBLING".to_uppercase().as_str()),
        }
    }
}
