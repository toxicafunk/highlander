use teloxide::types::InputMedia;

pub struct Status {
    pub action: bool,
    pub respond: bool,
    pub text: String
}

#[derive(Debug)]
pub struct SDO {
    pub chat_id: i64,
    pub msg_id: i32,
    pub file_type: String,
    pub unique_id: String,
    pub file_id: Option<String>
}

pub enum HResponse {
    Media(Vec<InputMedia>),
    URL(Vec<String>)
}