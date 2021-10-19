use sqlite::{Connection, Error, Value};

use std::env;
use std::sync::Arc;

use rtdlib::types::UpdateDeleteMessages;
use teloxide::types::{Chat, ChatKind, User};

use super::models::*;
use super::repository::*;

const INSERT: &str = "INSERT INTO mappings (api_id, chat_id, unique_id) VALUES (?, ?, ?)";

fn cursor_to_bool(cursor: Result<Option<&[Value]>, Error>, label: &str) -> bool {
    match cursor {
        Err(e) => {
            log::error!("{}: {}", label, e);
            false
        }
        Ok(row) => match row {
            None => false,
            Some(_) => true,
        },
    }
}

#[derive(Clone)]
pub struct SQLiteRepo {
    connection: Arc<Connection>,
}

impl Repository for SQLiteRepo {
    fn init() -> Self {
        let db_path = match env::var("HIGHLANDER_DB_PATH") {
            Ok(path) => path,
            Err(_) => String::from("."),
        };

        let connection: Connection = ok!(sqlite::open(format!("{}/attachments.db", db_path)));
        SQLiteRepo {
            connection: Arc::new(connection),
        }
    }

    fn chat_user_exists(&self, user: &User, chat: Arc<Chat>) -> bool {
        let select = "SELECT user_name, chat_name FROM users WHERE user_id = ? AND chat_id = ?";
        match self.connection.prepare(select) {
            Err(e) => {
                log::error!("Store user: {}", e);
                false
            }
            Ok(mut select_stmt) => {
                ok!(select_stmt.bind(1, user.id));
                ok!(select_stmt.bind(2, chat.id));
                let mut select_cursor = select_stmt.cursor();
                cursor_to_bool(select_cursor.next(), "chat_user_exists")
            }
        }
    }

    fn update_user_timestamp(&self, user: &User, chat: Arc<Chat>) -> bool {
        let update = "UPDATE users SET timestamp=CURRENT_TIMESTAMP WHERE user_id=? AND chat_id=?";
        let mut update_stmt = ok!(self.connection.prepare(update));
        ok!(update_stmt.bind(1, user.id));
        ok!(update_stmt.bind(2, chat.id));
        let mut update_cursor = update_stmt.cursor();
        match update_cursor.next() {
            Err(e) => {
                log::error!("update_user_timestamp: {}", e);
                false
            }
            Ok(row) => match row {
                None => false,
                Some(_) => true,
            },
        }
    }

    fn insert_user(&self, user: &User, chat: Arc<Chat>) -> bool {
        log::info!("insert_user...");
        match &chat.kind {
            ChatKind::Public(public) => {
                let unknown = String::from("Unknown");
                let chat_name = public.title.as_ref().unwrap_or(&unknown) as &str;
                let insert = "INSERT INTO users (user_id, chat_id, user_name, chat_name) VALUES (?, ?, ?, ?)";
                let mut insert_stmt = ok!(self.connection.prepare(insert));
                ok!(insert_stmt.bind(1, user.id));
                ok!(insert_stmt.bind(2, chat.id));
                ok!(insert_stmt.bind(
                    3,
                    user.username
                        .as_ref()
                        .unwrap_or(&user.first_name.to_string()) as &str
                ));
                ok!(insert_stmt.bind(4, &chat_name as &str));

                let mut cursor = insert_stmt.cursor();
                cursor_to_bool(cursor.next(), "insert_user")
            }
            ChatKind::Private(_) => false, //private.username.as_ref().unwrap_or(&unknown)
        }
    }

    fn item_exists(&self, sdo: SDO, is_media: bool) -> Vec<Value> {
        let select = if is_media {
            "SELECT chat_id, msg_id, unique_id FROM media WHERE chat_id = ? AND unique_id = ?"
        } else {
            "SELECT chat_id, msg_id, unique_id FROM {} WHERE chat_id = ? AND unique_id = ?"
        };

        let empty_vec = Vec::new();

        match self.connection.prepare(select) {
            Err(e) => {
                log::error!("Handle message: {}", e);
                empty_vec
            }
            Ok(mut select_stmt) => {
                ok!(select_stmt.bind(1, sdo.chat.id));
                ok!(select_stmt.bind(2, sdo.unique_id.as_str()));
                let mut select_cursor = select_stmt.cursor();
                match select_cursor.next() {
                    Err(e) => {
                        log::error!("media_exists: {}", e);
                        empty_vec
                    }
                    Ok(row) => match row {
                        None => empty_vec,
                        Some(r) => {
                            let new_vec = r.to_vec();
                            new_vec.to_owned()
                        }
                    },
                }
            }
        }
    }

    fn insert_item(&self, sdo: SDO, is_media: bool) -> bool {
        let insert = if is_media {
            "INSERT INTO media (chat_id, msg_id, file_type, unique_id, file_id) VALUES (?, ?, ?, ?, ?)"
        } else {
            "INSERT INTO urls (chat_id, msg_id, unique_id) VALUES (?, ?, ?)"
        };

        let mut insert_stmt = ok!(self.connection.prepare(insert));
        if is_media {
            ok!(insert_stmt.bind(1, sdo.chat.id));
            ok!(insert_stmt.bind(2, f64::from(sdo.msg_id)));
            ok!(insert_stmt.bind(3, sdo.file_type.as_str()));
            ok!(insert_stmt.bind(4, sdo.unique_id.as_str()));
            ok!(insert_stmt.bind(5, ok!(sdo.file_id).as_str()));
        } else {
            ok!(insert_stmt.bind(1, sdo.chat.id));
            ok!(insert_stmt.bind(2, f64::from(sdo.msg_id)));
            ok!(insert_stmt.bind(3, sdo.unique_id.as_str()));
        };
        let mut cursor = insert_stmt.cursor();
        match cursor.next() {
            Err(e) => {
                log::error!("insert_item: is_media {} {}", is_media, e);
                false
            }
            Ok(_) => {
                log::info!(
                    "Stored {} - {} - {}",
                    sdo.chat.id,
                    sdo.msg_id,
                    sdo.unique_id
                );
                true
            }
        }
    }

    fn insert_duplicate(&self, acc: &Status, sdo: SDO) -> bool {
        log::info!(
            "Duplicate: {} - {} - {}",
            sdo.chat.id,
            sdo.unique_id,
            acc.text
        );
        let insert = "INSERT INTO duplicates (chat_id, unique_id, file_type, file_id, msg_id) VALUES (?, ?, ?, ?, ?)";
        let mut insert_stmt = ok!(self.connection.prepare(insert));
        ok!(insert_stmt.bind(1, sdo.chat.id));
        ok!(insert_stmt.bind(2, sdo.unique_id.as_str()));
        ok!(insert_stmt.bind(3, sdo.file_type.as_str()));
        ok!(insert_stmt.bind(4, sdo.file_id.unwrap_or(String::from("")).as_str()));
        ok!(insert_stmt.bind(5, f64::from(sdo.msg_id)));

        let mut cursor = insert_stmt.cursor();
        match cursor.next() {
            Ok(_) => true,
            Err(e) => {
                log::error!("insert_duplicate: {}", e);
                false
            }
        }
    }

    fn delete_item(&self, deleted_messages: UpdateDeleteMessages) -> () {
        let chat_id = deleted_messages.chat_id();
        let delete_media = "DELETE FROM media WHERE unique_id = (SELECT unique_id FROM mappings WHERE api_id = ? and chat_id = ?)";
        let delete_urls = "DELETE FROM urls WHERE unique_id = (SELECT unique_id FROM mappings WHERE api_id = ? and chat_id = ?)";

        for msg_id in deleted_messages.message_ids() {
            let mut delete_media_stmt = ok!(self.connection.prepare(delete_media));
            ok!(delete_media_stmt.bind(1, *msg_id));
            ok!(delete_media_stmt.bind(2, chat_id));
            let mut delete_media_cursor = delete_media_stmt.cursor();
            match delete_media_cursor.next() {
                Err(e) => log::error!("{}", e),
                Ok(r) => match r {
                    None => (),
                    Some(_) => log::info!("Delete media message {} on chat {}", msg_id, chat_id),
                },
            }

            let mut delete_urls_stmt = ok!(self.connection.prepare(delete_urls));
            ok!(delete_urls_stmt.bind(1, *msg_id));
            ok!(delete_urls_stmt.bind(2, chat_id));
            let mut delete_urls_cursor = delete_urls_stmt.cursor();
            match delete_urls_cursor.next() {
                Err(e) => log::error!("{}", e),
                Ok(r) => match r {
                    None => (),
                    Some(_) => log::info!("Delete url message {} on chat {}", msg_id, chat_id),
                },
            }
        }
    }

    fn insert_mapping(&self, id: i64, chat_id: i64, unique_id: &str) -> bool {
        let mut insert_stmt = ok!(self.connection.prepare(INSERT));

        ok!(insert_stmt.bind(1, id));
        ok!(insert_stmt.bind(2, chat_id));
        ok!(insert_stmt.bind(3, unique_id));

        let mut cursor = insert_stmt.cursor();
        match cursor.next() {
            Err(e) => {
                log::error!("{}", e);
                false
            }
            Ok(r) => match r {
                None => false,
                Some(e) => {
                    log::error!("{:?}", e);
                    true
                }
            },
        }
    }
}
