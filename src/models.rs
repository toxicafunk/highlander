use teloxide::types::InputMedia;
use teloxide::types::Chat;

use std::sync::Arc;
use std::env;

use sqlite::Connection;

pub struct Status {
    pub action: bool,
    pub respond: bool,
    pub text: String
}

#[derive(Debug)]
pub struct SDO {
    pub chat: Arc<Chat>,
    pub msg_id: i32,
    pub file_type: String,
    pub unique_id: String,
    pub file_id: Option<String>
}

pub enum HResponse {
    Media(Vec<InputMedia>),
    URL(Vec<String>)
}

pub fn create_connection() -> Connection {
    let db_path = match env::var("HIGHLANDER_DB_PATH") {
        Ok(path) => path,
        Err(_) => String::from("."),
    };

    let connection: Connection = ok!(sqlite::open(format!("{}/attachments.db", db_path)));
    connection
}