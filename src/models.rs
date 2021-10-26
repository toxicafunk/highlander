use teloxide::types::Chat;
use teloxide::types::InputMedia;

use std::sync::Arc;

use serde::{Serialize, Deserialize};

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

#[derive(Debug, Clone)]
pub struct SDO {
    pub chat: Arc<Chat>,
    pub msg_id: i32,
    pub file_type: String,
    pub unique_id: String,
    pub file_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Media {
    pub unique_id: String,
    pub chat_id: i64,
    pub msg_id: i32,
    pub file_type: String,
    pub file_id: String,
    pub timestamp: i64
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct User {
    pub user_id: i64,
    pub chat_id: i64,
    pub user_name: String,
    pub chat_name: String,
    pub timestamp: i64
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Mapping {
    pub unique_id: String,
    pub chat_id: i64,
    pub api_id: i64,
    pub timestamp: i64
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ColFam {
    MediaCF(Media),
    UserCF(User),
    MappingCF(Mapping)
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Group {
    pub supergroup_id: i64,
    pub chat_id: i64,
    pub offset: i64,
    pub timestamp: i64
}

pub enum HResponse {
    Ban(Vec<User>),
    Media(Vec<InputMedia>),
    URL(Vec<String>),
    Text(String),
}
