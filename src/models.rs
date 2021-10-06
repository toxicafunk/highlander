use teloxide::types::Chat;
use teloxide::types::InputMedia;

use std::env;
use std::sync::Arc;

use sqlite::Connection;

#[derive(Debug, Clone)]
pub struct Status {
    pub action: bool,
    pub respond: bool,
    pub text: String,
}

impl Status {
    pub fn new(status: &Status) -> Self {
        Self {
            action: status.action,
            respond: status.respond,
            text: status.text.clone(),
        }
    }
}

#[derive(Debug)]
pub struct SDO {
    pub chat: Arc<Chat>,
    pub msg_id: i32,
    pub file_type: String,
    pub unique_id: String,
    pub file_id: Option<String>,
}

pub enum HResponse {
    Media(Vec<InputMedia>),
    URL(Vec<String>),
    Text(String),
}

pub fn create_connection() -> Connection {
    let db_path = match env::var("HIGHLANDER_DB_PATH") {
        Ok(path) => path,
        Err(_) => String::from("."),
    };

    let connection: Connection = ok!(sqlite::open(format!("{}/attachments.db", db_path)));
    connection
}
