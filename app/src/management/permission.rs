use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum ApiPermission {
    Admin
}

impl Display for ApiPermission {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ApiPermission::Admin => "admin",
        })
    }
}