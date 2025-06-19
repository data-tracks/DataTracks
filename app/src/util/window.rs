use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
pub enum WindowType {
    Thumbling,
    Sliding,
    Hopping,
    Session,
}

impl Display for WindowType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            WindowType::Thumbling => f.write_str("THUMBLING".to_uppercase().as_str()),
            WindowType::Sliding => f.write_str("SLIDING".to_uppercase().as_str()),
            WindowType::Hopping => f.write_str("HOPPING".to_uppercase().as_str()),
            WindowType::Session => f.write_str("SESSION".to_uppercase().as_str()),
        }
    }
}
