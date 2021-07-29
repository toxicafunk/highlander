use teloxide::prelude::*;
use teloxide::types::{MessageKind, MediaKind};
use sqlite::Connection;

use lazy_static::lazy_static;
use regex::Regex;
use std::env;

use std::io::Write;
use chrono::Local;
use pretty_env_logger::env_logger::Builder;
use log::LevelFilter;

use teloxide::utils::command::BotCommand;

use tokio_stream::wrappers::UnboundedReceiverStream;
use teloxide::RequestError;

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

#[derive(BotCommand)]
#[command(rename = "lowercase", description = "These commands are supported:")]
enum Command {
    #[command(description = "display this text.")]
    Help,
    #[command(description = "handle a username.")]
    LastStored(u8),
    #[command(description = "handle a username and an age.", parse_with = "split")]
    UsernameAndAge { username: String, age: u8 },
}
#[tokio::main]
async fn main() {
    run().await;
}

async fn run() {
    //teloxide::enable_logging!();
    Builder::new()
        .format(|buf, record| {
            writeln!(buf,
                     "{} [{}] - {}",
                     Local::now().format("%Y-%m-%dT%H:%M:%S"),
                     record.level(),
                     record.args()
            )
        })
        .filter(None, LevelFilter::Info)
        .init();
    log::info!("Starting Highlander bot...");

    let bot = Bot::from_env().auto_send();

    Dispatcher::new(bot)
        .messages_handler(|rx: DispatcherHandlerRx<AutoSend<Bot>, Message>| {
            UnboundedReceiverStream::new(rx).for_each_concurrent(
                None,
                |message| async move {
                    let db_path = match env::var("HIGHLANDER_DB_PATH") {
                        Ok(path) => path,
                        Err(_) => String::from("."),
                    };
                    let connection: Connection = sqlite::open(format!("{}/attachments.db", db_path)).unwrap();
                    let r = detect_duplicates(&connection, &message);
                    if r.respond {
                        let mr = message.answer(r.text).await;
                        match mr {
                            Ok(m) => log::info!("Responded: {:?}", m),
                            Err(e) => log::error!("Error: {:?}", e)
                        }
                    }
                    if r.action {
                        let mr = message.delete_message().await;
                        match mr {
                            Ok(m) => log::info!("Deleted message: {:?}", m),
                            Err(e) => log::error!("Error: {:?}", e)
                        }
                    }

                    let txt_opt = message.update.text();
                    let bot_name = "ramirez";

                    match txt_opt {
                        Some(txt) => match Command::parse(txt, bot_name) {
                            Ok(command) => {
                                let cr = handle_command(&connection, command);
                                match cr {
                                    Ok(msg) => {
                                        log::info!("Response: {}", &msg);
                                        ok!(message.answer(msg).await);
                                    },
                                    Err(e) => log::error!("Error: {:?}", e)
                                }
                            }
                            Err(_) => ()
                        },
                        None => ()
                    }
                }
            )
        })
        .dispatch()
        .await;
}

fn detect_duplicates(connection: &Connection, message: &UpdateWithCx<AutoSend<Bot>, Message>) -> Status {
    let update = &message.update;
    let kind = update.kind.clone();
    let chat_id = update.chat.id;

    let success = "Media will be unique for 5 days";
    let mut status = Status { action: false, respond: false, text: success.to_string() };

    let r: Status = match kind {
        MessageKind::Common(msg_common) => match msg_common.media_kind {
            MediaKind::Text(text) => {
                lazy_static! {
                        static ref RE: Regex = Regex::new("(http|ftp|https)://([\\w_-]+(?:(?:\\.[\\w_-]+)+))([\\w.,@?^=%&:/~+#-]*[\\w@?^=%&/~+#-])?").unwrap();
                    }
                let t = &*text.text;
                RE.captures_iter(t).fold(status, |acc, cap| {
                    let url = &cap[0];
                    log::info!("Detected url: {}", url);
                    handle_message(chat_id, &connection, acc, extract_last250(url), "urls")
                })
            },
            MediaKind::Animation(animation) => {
                let file_unique_id = &*animation.animation.file_unique_id;
                log::info!("Animation: {:?}", update);
                let caption = &*animation.caption.unwrap_or(update.id.to_string());
                status.text = caption.into();
                handle_message(chat_id, &connection, status, file_unique_id, "media")
            },
            MediaKind::Audio(audio) => {
                let file_unique_id = &*audio.audio.file_unique_id;
                log::info!("Audio: {:?}", update);
                let caption = &*audio.caption.unwrap_or(update.id.to_string());
                status.text = caption.into();
                handle_message(chat_id, &connection, status, file_unique_id, "media")
            },
            MediaKind::Document(document) => {
                let file_unique_id = &*document.document.file_unique_id;
                log::info!("Document: {:?}", update);
                let caption = &*document.caption.unwrap_or(update.id.to_string());
                status.text = caption.into();
                handle_message(chat_id, &connection, status, file_unique_id, "media")
            },
            MediaKind::Photo(photo) => {
                log::info!("Photo: {:?}", update);
                let caption = &*photo.caption.unwrap_or(update.id.to_string());
                status.text = caption.into();
                photo.photo.iter().fold(status, |acc, p| {
                    let file_unique_id = &*p.file_unique_id;
                    handle_message(chat_id, &connection, acc, file_unique_id, "media")
                })
            },
            MediaKind::Video(video) => {
                let file_unique_id = &*video.video.file_unique_id;
                let caption = &*video.caption.unwrap_or(update.id.to_string());
                log::info!("Video: {:?}", update);
                status.text = caption.into();
                handle_message(chat_id, &connection, status, file_unique_id, "media")
            },
            MediaKind::Voice(voice) => {
                let file_unique_id = &*voice.voice.file_unique_id;
                log::info!("Voice: {:?}", update);
                let caption = &*voice.caption.unwrap_or(update.id.to_string());
                status.text = caption.into();
                handle_message(chat_id, &connection, status, file_unique_id, "media")
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

fn handle_message(chat_id: i64, connection: &Connection, acc: Status, file_unique_id: &str, table: &str) -> Status {
    let select = format!("SELECT unique_id FROM {} WHERE chat_id = ? AND unique_id = ?", table);
    let insert = format!("INSERT INTO {} (chat_id, unique_id) VALUES (?, ?)", table);
    let mut select_stmt = ok!(connection.prepare(select));
    ok!(select_stmt.bind(1, chat_id));
    ok!(select_stmt.bind(2, file_unique_id));
    let mut select_cursor = select_stmt.cursor();
    let row = ok!(select_cursor.next());

    match row {
        None => {
            let mut insert_stmt = ok!(connection.prepare(insert));
            ok!(insert_stmt.bind(1, chat_id));
            ok!(insert_stmt.bind(2, file_unique_id));
            let mut cursor = insert_stmt.cursor();
            ok!(cursor.next());
            log::info!("Stored {} - {} - {}", chat_id, file_unique_id, acc.text);
            acc
        },
        Some(_) => {
            log::info!("Duplicate: {} - {} - {}", chat_id, file_unique_id, acc.text);
            Status { action: true, respond: true, text: "Mensaje Duplicado: El archivo o url ya se ha compartido en los ultimos 5 dias.".to_string() }
        }
    }
}

fn handle_command(
    connection: &Connection,
    command: Command,
) -> Result<String, RequestError> {
    let r = match command {
        Command::Help => Command::descriptions(),
        Command::LastStored(num) => {
            let select = format!("SELECT * FROM media ORDER BY timestamp DESC limit {};", num);
            let mut vec = Vec::new();
            ok!(connection.iterate(select, |media| {
                vec.push(format!("{:?}", media));
                true
            }));
            format!("{:?}.", vec.join("\n"))
        }
        Command::UsernameAndAge { username, age } =>
            format!("Your username is @{} and age is {}.", username, age)
    };
    Ok(r)
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