use teloxide::prelude::*;
use teloxide::types::{MessageKind, MediaKind, Chat};
use sqlite::Connection;

use lazy_static::lazy_static;
use regex::Regex;

use std::env;

macro_rules! ok(($result:expr) => ($result.unwrap()));

struct Status {
    action: bool,
    respond: bool,
    text: String,
}

fn extract_last250(text: &str) -> &str {
    let l = text.len();
    let i = if l > 250 { l - 250} else { 0 };
    text.get(i..l).unwrap_or("")
}

#[tokio::main]
async fn main() {
    run().await;
}

async fn run() {
    teloxide::enable_logging!();
    log::info!("Starting Highlander bot...");

    let bot = Bot::from_env().auto_send();

    teloxide::repl(bot, |message| async move {
        let update = &message.update;
        log::info!("{:?}", update);
        let kind = update.kind.clone();
        let chat = &update.chat;

        let success = "Media will be unique for 7 days";
        let  status = Status { action: false, respond: false, text: success.to_string() };

        let db_path =  match env::var("HIGHLANDER_DB_PATH") {
            Ok(path) => path,
            Err(_) => String::from("."),
        };
        let connection: Connection = sqlite::open(format!("{}/attachments.db", db_path)).unwrap();

        let r: Status = match kind {
            MessageKind::Common(msg_common) => match msg_common.media_kind {
                MediaKind::Text(text) => {
                    lazy_static! {
                        static ref RE: Regex = Regex::new("(http|ftp|https)://([\\w_-]+(?:(?:\\.[\\w_-]+)+))([\\w.,@?^=%&:/~+#-]*[\\w@?^=%&/~+#-])?").unwrap();
                    }

                    let t = &*text.text;
                    log::info!("Text Message: {}", t);

                    RE.captures_iter(t).fold(status, |acc, cap| {
                        let url = &cap[0];
                        handle_message(chat, &connection, acc, extract_last250(url), "urls")
                    })
                },
                MediaKind::Animation(animation) => {
                    let file_unique_id = &*animation.animation.file_unique_id;
                    handle_message(chat, &connection, status, file_unique_id, "media")
                },
                MediaKind::Audio(audio) => {
                    let file_unique_id = &*audio.audio.file_unique_id;
                    handle_message(chat, &connection, status, file_unique_id, "media")
                },
                MediaKind::Document(document) => {
                    let file_unique_id = &*document.document.file_unique_id;
                    handle_message(chat, &connection, status, file_unique_id, "media")
                },
                MediaKind::Photo(photo) => {
                    photo.photo.iter().fold(status,|acc, p| {
                        let file_unique_id = &*p.file_unique_id;
                        handle_message(chat, &connection, acc, file_unique_id, "media")
                    })
                },
                MediaKind::Video(video) => {
                    let file_unique_id = &*video.video.file_unique_id;
                    handle_message(chat, &connection, status, file_unique_id, "media")
                },
                MediaKind::Voice(voice) => {
                    let file_unique_id = &*voice.voice.file_unique_id;
                    handle_message(chat, &connection, status, file_unique_id, "media")
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

        if r.respond {
            message.answer(r.text).await?;
        }

        if r.action {
            message.delete_message().await?;
        }
        respond(())
    })
        .await;
}

fn handle_message(chat: &Chat, connection: &Connection, acc: Status, file_unique_id: &str, table: &str) -> Status {
    let select = format!("SELECT unique_id FROM {} WHERE chat_id = ? AND unique_id = ?", table);
    let insert = format!("INSERT INTO {} (chat_id, unique_id) VALUES (?, ?)", table);
    let mut select_stmt = ok!(connection.prepare(select));
    ok!(select_stmt.bind(1, chat.id));
    ok!(select_stmt.bind(2, file_unique_id));
    let mut select_cursor = select_stmt.cursor();
    let row = ok!(select_cursor.next());

    match row {
        None => {
            let mut insert_stmt = ok!(connection.prepare(insert));
            ok!(insert_stmt.bind(1, chat.id));
            ok!(insert_stmt.bind(2, file_unique_id));
            let mut cursor = insert_stmt.cursor();
            ok!(cursor.next());
            //acc.respond = true;
            acc
        },
        Some(_) => {
            Status { action: true, respond: true, text: "Mensaje Duplicado: El archivo o url ya se ha compartido en los ultimos 5 dias.".to_string() }
        }
    }
}

#[cfg(test)]
mod tests {
    use regex::Regex;
    use crate::extract_last250;

    #[test]
    fn url_regex() {
        let t1 = "hola https://twitter.com/plaforscience/status/1379526168513277960";
        let t2 = "hola https://twitter.com/plaforscience/status/1379526168513277960 y ademas https://youtu.be/GCI0NMgVfPk";
        let t3 = "https://drive.google.com/file/d/1t3_HeKZDIMEJl5_Y_l7uuIt4IeebCN7e/view?usp=sharing";

        let re: Regex = Regex::new("(http|ftp|https)://([\\w_-]+(?:(?:\\.[\\w_-]+)+))([\\w.,@?^=%&:/~+#-]*[\\w@?^=%&/~+#-])?").unwrap();

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