use sqlite::Connection;

use regex::Regex;
use lazy_static::lazy_static;

use teloxide::prelude::*;
use teloxide::types::{Chat, ChatKind, MessageKind, MediaKind, User};

use super::models::*;
use std::sync::Arc;

fn extract_last250(text: &str) -> &str {
    let l = text.len();
    let i = if l > 250 { l - 250} else { 0 };
    text.get(i..l).unwrap_or("")
}

pub fn detect_duplicates(connection: &Connection, message: &UpdateWithCx<AutoSend<Bot>, Message>, user: &User) -> Status {
    let update: &Message = &message.update;
    let kind: MessageKind = update.kind.clone();
    let chat: Arc<Chat> = Arc::new(update.chat.clone());
    let msg_id: i32 = update.id;

    store_user(connection, user, chat.clone());

    let success = "Media will be unique for 5 days";
    let mut status = Status { action: false, respond: false, text: success.to_string() };

    let r: Status = match kind {
        MessageKind::Common(msg_common) => match msg_common.media_kind {
            MediaKind::Text(text) => {
                lazy_static! {
                        static ref RE: Regex = ok!(Regex::new("(http|ftp|https)://([\\w_-]+(?:(?:\\.[\\w_-]+)+))([\\w.,@?^=%&:/~+#-]*[\\w@?^=%&/~+#-])?"));
                    }
                let t = &*text.text;
                RE.captures_iter(t).fold(status, |acc, cap| {
                    let url = &cap[0];
                    log::info!("Detected url: {}", url);
                    let chat = chat.clone();
                    let sdo = SDO { chat, msg_id, file_type: String::from("url"), unique_id: extract_last250(url).into(), file_id: None };
                    handle_message(&connection, acc, sdo, "urls")
                })
            },
            MediaKind::Animation(animation) => {
                let file_unique_id = animation.animation.file_unique_id;
                let file_id = animation.animation.file_id;
                log::info!("Animation: {:?}", update);
                let caption = &*animation.caption.unwrap_or(update.id.to_string());
                status.text = caption.into();
                let chat = chat.clone();
                let sdo = SDO { chat, msg_id, file_type: String::from("animation"), unique_id: file_unique_id, file_id: Some(file_id) };
                handle_message(&connection, status, sdo, "media")
            },
            MediaKind::Audio(audio) => {
                let file_unique_id = audio.audio.file_unique_id;
                let file_id = audio.audio.file_id;
                log::info!("Audio: {:?}", update);
                let caption = &*audio.caption.unwrap_or(update.id.to_string());
                status.text = caption.into();
                let chat = chat.clone();
                let sdo = SDO { chat, msg_id, file_type: String::from("audio"), unique_id: file_unique_id, file_id: Some(file_id) };
                handle_message(&connection, status, sdo, "media")
            },
            MediaKind::Document(document) => {
                let file_unique_id = document.document.file_unique_id;
                let file_id = document.document.file_id;
                log::info!("Document: {:?}", update);
                let caption = &*document.caption.unwrap_or(update.id.to_string());
                status.text = caption.into();
                let chat = chat.clone();
                let sdo = SDO { chat, msg_id, file_type: String::from("document"), unique_id: file_unique_id, file_id: Some(file_id) };
                handle_message(&connection, status, sdo, "media")
            },
            MediaKind::Photo(photo) => {
                log::info!("Photo: {:?}", update);
                let caption = &*photo.caption.unwrap_or(update.id.to_string());
                status.text = caption.into();
                photo.photo.iter().fold(status, |acc, p| {
                    let file_unique_id = &*p.file_unique_id;
                    let file_id = &*p.file_id;
                    let chat = chat.clone();
                    let sdo = SDO { chat, msg_id, file_type: String::from("photo"), unique_id: file_unique_id.into(), file_id: Some(file_id.into()) };
                    handle_message(&connection, acc, sdo, "media")
                })
            },
            MediaKind::Video(video) => {
                let file_unique_id = video.video.file_unique_id;
                let file_id = video.video.file_id;
                let caption = &*video.caption.unwrap_or(update.id.to_string());
                log::info!("Video: {:?}", update);
                status.text = caption.into();
                let chat = chat.clone();
                let sdo = SDO { chat, msg_id, file_type: String::from("video"), unique_id: file_unique_id, file_id: Some(file_id) };
                handle_message(&connection, status, sdo, "media")
            },
            MediaKind::Voice(voice) => {
                let file_unique_id = voice.voice.file_unique_id;
                let file_id = voice.voice.file_id;
                log::info!("Voice: {:?}", update);
                let caption = &*voice.caption.unwrap_or(update.id.to_string());
                status.text = caption.into();
                let chat = chat.clone();
                let sdo = SDO { chat, msg_id, file_type: String::from("voice"), unique_id: file_unique_id, file_id: Some(file_id) };
                handle_message(&connection, status, sdo, "media")
            },
            _ => {
                log::info!("Other attachment");
                status
            }
        },
        _ => {
            log::info!("Not interesting");
            status
        }
    };
    r
}

fn store_user(connection: &Connection, user: &User, chat: Arc<Chat>) {
    let select = "SELECT user_name, chat_name FROM users WHERE user_id = ? AND chat_id = ?";
    let mut select_stmt = ok!(connection.prepare(select));
    ok!(select_stmt.bind(1, user.id));
    ok!(select_stmt.bind(2, chat.id));
    let mut select_cursor = select_stmt.cursor();
    let row = ok!(select_cursor.next());
    let unknown = String::from("Unknown");

    match row {
        Some(_) => {
            let update = "UPDATE users SET timestamp=CURRENT_TIMESTAMP WHERE user_id=? AND chat_id=?";
            let mut update_stmt = ok!(connection.prepare(update));
            ok!(update_stmt.bind(1, user.id));
            ok!(update_stmt.bind(2, chat.id));
            let mut update_cursor = update_stmt.cursor();
            ok!(update_cursor.next());
        },
        None => {
            log::info!("Storing user...");
            match &chat.kind {
                ChatKind::Public(public) => {
                    let chat_name = public.title.as_ref().unwrap_or(&unknown) as &str;
                    let insert = "INSERT INTO users (user_id, chat_id, user_name, chat_name) VALUES (?, ?, ?, ?)";
                    let mut insert_stmt = ok!(connection.prepare(insert));
                    ok!(insert_stmt.bind(1, user.id));
                    ok!(insert_stmt.bind(2, chat.id));
                    ok!(insert_stmt.bind(3, user.username.as_ref().unwrap_or(&user.first_name.to_string()) as &str));
                    ok!(insert_stmt.bind(4, &chat_name as &str));

                    let mut cursor = insert_stmt.cursor();
                    ok!(cursor.next());
                },
                ChatKind::Private(_) => ()
                //private.username.as_ref().unwrap_or(&unknown)
            };
        }
    }
}
fn handle_message(connection: &Connection, acc: Status, sdo: SDO, table: &str) -> Status {
    let is_media = table == "media";
    let select = format!("SELECT chat_id, msg_id, unique_id FROM {} WHERE chat_id = ? AND unique_id = ?", table);
    let mut select_stmt = ok!(connection.prepare(select));
    ok!(select_stmt.bind(1, sdo.chat.id));
    ok!(select_stmt.bind(2, sdo.unique_id.as_str()));
    let mut select_cursor = select_stmt.cursor();
    let row = ok!(select_cursor.next());

    let insert = if is_media {
        format!("INSERT INTO {} (chat_id, msg_id, file_type, unique_id, file_id) VALUES (?, ?, ?, ?, ?)", table)
    } else {
        format!("INSERT INTO {} (chat_id, msg_id, unique_id) VALUES (?, ?, ?)", table)
    };

    log::info!("table: {}, SDO: {:?}", table, sdo);
    match row {
        None => {
            let mut insert_stmt = ok!(connection.prepare(insert));
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
            ok!(cursor.next());
            log::info!("Stored {} - {} - {} - {}", sdo.chat.id, sdo.msg_id, sdo.unique_id, acc.text);
            acc
        },
        Some(r) => {
            log::info!("Duplicate: {} - {} - {}", sdo.chat.id, sdo.unique_id, acc.text);
            log::info!("{:?}", r);
            let insert = "INSERT INTO duplicates (chat_id, unique_id, file_type, file_id) VALUES (?, ?, ?, ?)";
            let mut insert_stmt = ok!(connection.prepare(insert));
            ok!(insert_stmt.bind(1, sdo.chat.id));
            ok!(insert_stmt.bind(2, sdo.unique_id.as_str()));
            ok!(insert_stmt.bind(3, sdo.file_type.as_str()));
            ok!(insert_stmt.bind(4, sdo.file_id.unwrap_or(String::from("")).as_str()));

            let mut cursor = insert_stmt.cursor();
            match cursor.next() {
                Ok(_) => (),
                Err(_) => (),
            };

            let orig_chat_id = match r[0].as_integer(){
                Some(c) => match c.to_string().strip_prefix("-100") {
                    Some(s) => s.parse::<i64>().unwrap_or(c),
                    None => 0,
                }
                None => 0,
            };
            let orig_msg_id = ok!(r[1].as_integer());
            log::info!("{} - {}", orig_chat_id, orig_msg_id);
            Status { action: true, respond: true, text: format!("Mensaje Duplicado: {} ya se ha compartido en los ultimos 5 dias.\nVer mensaje original: https://t.me/c/{}/{}", table, orig_chat_id, orig_msg_id) }
        }
    }
}

#[cfg(test)]
mod tests {
    use regex::Regex;
    use crate::duplicates::extract_last250;

    #[test]
    fn url_regex() {
        let t1 = "hola https://twitter.com/plaforscience/status/1379526168513277960";
        let t2 = "hola https://twitter.com/plaforscience/status/1379526168513277960 y ademas https://youtu.be/GCI0NMgVfPk";
        let t3 = "https://drive.google.com/file/d/1t3_HeKZDIMEJl5_Y_l7uuIt4IeebCN7e/view?usp=sharing";

        let re: Regex = ok!(Regex::new("(http|ftp|https)://([\\w_-]+(?:(?:\\.[\\w_-]+)+))([\\w.,@?^=%&:/~+#-]*[\\w@?^=%&/~+#-])?"));

        let caps = re.captures_iter(t1);
        //println!("Found: {}", caps.count());
        for i in caps {
            let url = &i[0];
            println!("{}", url);
            println!("{}", extract_last250(url))
        }

        for i in re.captures_iter(t2) {
            let url = &i[0];
            println!("{}", url);
            println!("{}", extract_last250(url))
        }

        for i in re.captures_iter(t3) {
            let url = &i[0];
            println!("{}", url);
            println!("{}", extract_last250(url))
        }

        assert_eq!(2 + 2, 4);
    }
}