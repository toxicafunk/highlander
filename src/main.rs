use teloxide::prelude::*;
use teloxide::types::{MessageKind, MediaKind, InputMedia, InputMediaVideo, InputMediaAnimation, InputMediaPhoto, InputMediaAudio, InputMediaDocument, InputFile};
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
    text: String
}

#[derive(Debug)]
struct SDO {
    chat_id: i64,
    msg_id: i32,
    file_type: String,
    unique_id: String,
    file_id: Option<String>
}

enum HResponse {
    Media(Vec<InputMedia>),
    URL(Vec<String>)
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
    #[command(description = "retrieves the las n media")]
    LastMediaStored(u8),
    #[command(description = "handle a username and an age.", parse_with = "split")]
    UsernameAndAge { username: String, age: u8 },
}
#[tokio::main]
async fn main() {
    run().await;
}

async fn run() {
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
                    let connection: Connection = ok!(sqlite::open(format!("{}/attachments.db", db_path)));
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
                                    Ok(hr) => match hr {
                                        HResponse::URL(urls) => {
                                            let ans = urls.join("\n");
                                            ok!(message.answer(ans).await);
                                        },
                                        HResponse::Media(vec) => {
                                            log::info!("Response size: {}", vec.len());
                                            ok!(message.answer_media_group(vec).await);
                                        }
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
    let msg_id = update.id;

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
                    let sdo = SDO { chat_id, msg_id, file_type: String::from("url"), unique_id: extract_last250(url).into(), file_id: None };
                    handle_message(&connection, acc, sdo, "urls")
                })
            },
            MediaKind::Animation(animation) => {
                let file_unique_id = animation.animation.file_unique_id;
                let file_id = animation.animation.file_id;
                log::info!("Animation: {:?}", update);
                let caption = &*animation.caption.unwrap_or(update.id.to_string());
                status.text = caption.into();
                let sdo = SDO { chat_id, msg_id, file_type: String::from("animation"), unique_id: file_unique_id, file_id: Some(file_id) };
                handle_message(&connection, status, sdo, "media")
            },
            MediaKind::Audio(audio) => {
                let file_unique_id = audio.audio.file_unique_id;
                let file_id = audio.audio.file_id;
                log::info!("Audio: {:?}", update);
                let caption = &*audio.caption.unwrap_or(update.id.to_string());
                status.text = caption.into();
                let sdo = SDO { chat_id, msg_id, file_type: String::from("audio"), unique_id: file_unique_id, file_id: Some(file_id) };
                handle_message(&connection, status, sdo, "media")
            },
            MediaKind::Document(document) => {
                let file_unique_id = document.document.file_unique_id;
                let file_id = document.document.file_id;
                log::info!("Document: {:?}", update);
                let caption = &*document.caption.unwrap_or(update.id.to_string());
                status.text = caption.into();
                let sdo = SDO { chat_id, msg_id, file_type: String::from("document"), unique_id: file_unique_id, file_id: Some(file_id) };
                handle_message(&connection, status, sdo, "media")
            },
            MediaKind::Photo(photo) => {
                log::info!("Photo: {:?}", update);
                let caption = &*photo.caption.unwrap_or(update.id.to_string());
                status.text = caption.into();
                photo.photo.iter().fold(status, |acc, p| {
                    let file_unique_id = &*p.file_unique_id;
                    let file_id = &*p.file_id;
                    let sdo = SDO { chat_id, msg_id, file_type: String::from("photo"), unique_id: file_unique_id.into(), file_id: Some(file_id.into()) };
                    handle_message(&connection, acc, sdo, "media")
                })
            },
            MediaKind::Video(video) => {
                let file_unique_id = video.video.file_unique_id;
                let file_id = video.video.file_id;
                let caption = &*video.caption.unwrap_or(update.id.to_string());
                log::info!("Video: {:?}", update);
                status.text = caption.into();
                let sdo = SDO { chat_id, msg_id, file_type: String::from("video"), unique_id: file_unique_id, file_id: Some(file_id) };
                handle_message(&connection, status, sdo, "media")
            },
            MediaKind::Voice(voice) => {
                let file_unique_id = voice.voice.file_unique_id;
                let file_id = voice.voice.file_id;
                log::info!("Voice: {:?}", update);
                let caption = &*voice.caption.unwrap_or(update.id.to_string());
                status.text = caption.into();
                let sdo = SDO { chat_id, msg_id, file_type: String::from("voice"), unique_id: file_unique_id, file_id: Some(file_id) };
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

fn handle_message(connection: &Connection, acc: Status, sdo: SDO, table: &str) -> Status {
    let select = format!("SELECT unique_id FROM {} WHERE chat_id = ? AND unique_id = ?", table);
    let insert = format!("INSERT INTO {} (chat_id, msg_id, file_type, unique_id, file_id) VALUES (?, ?, ?, ?, ?)", table);
    let mut select_stmt = ok!(connection.prepare(select));
    ok!(select_stmt.bind(1, sdo.chat_id));
    ok!(select_stmt.bind(2, sdo.unique_id.as_str()));
    let mut select_cursor = select_stmt.cursor();
    let row = ok!(select_cursor.next());

    log::info!("SDO: {:?}", sdo);
    match row {
        None => {
            let mut insert_stmt = ok!(connection.prepare(insert));
            ok!(insert_stmt.bind(1, sdo.chat_id));
            ok!(insert_stmt.bind(2, f64::from(sdo.msg_id)));
            ok!(insert_stmt.bind(3, sdo.file_type.as_str()));
            ok!(insert_stmt.bind(4, sdo.unique_id.as_str()));
            ok!(insert_stmt.bind(5, ok!(sdo.file_id).as_str()));
            let mut cursor = insert_stmt.cursor();
            ok!(cursor.next());
            log::info!("Stored {} - {} - {} - {}", sdo.chat_id, sdo.msg_id, sdo.unique_id, acc.text);
            acc
        },
        Some(_) => {
            log::info!("Duplicate: {} - {} - {}", sdo.chat_id, sdo.unique_id, acc.text);
            Status { action: true, respond: true, text: "Mensaje Duplicado: El archivo o url ya se ha compartido en los ultimos 5 dias.".to_string() }
        }
    }
}

fn handle_command(
    connection: &Connection,
    command: Command,
) -> Result<HResponse, RequestError> {
    let r = match command {
        Command::Help => HResponse::URL(vec![Command::descriptions()]),
        Command::LastMediaStored(num) => {
            let select = format!("SELECT * FROM media ORDER BY timestamp DESC limit {};", num);
            let mut vec = Vec::new();
            ok!(connection.iterate(select, |dbmedia| {
                let (_, file_type) = dbmedia[3];
                let (_, unique_id) = dbmedia[4];
                let (_, file_id) = dbmedia[5];
                let ftype = ok!(file_type);
                let im: InputMedia = match ftype {
                    "photo" => InputMedia::Photo(InputMediaPhoto { media: InputFile::FileId(ok!(file_id).into()), caption: Some(format!("Part of media {}", ok!(unique_id))), caption_entities: None, parse_mode: None }),
                    "video" => InputMedia::Video(InputMediaVideo { media: InputFile::FileId(ok!(file_id).into()), caption: Some(format!("Part of media {}", ok!(unique_id))), caption_entities: None, parse_mode: None, thumb: None, width: None, height: None, duration: None, supports_streaming: None }),
                    "audio" => InputMedia::Audio(InputMediaAudio { media: InputFile::FileId(ok!(file_id).into()), caption: Some(format!("Part of media {}", ok!(unique_id))), caption_entities: None, parse_mode: None, thumb: None, performer: None, title: None, duration: None }),
                    "animation" => InputMedia::Animation(InputMediaAnimation { media: InputFile::FileId(ok!(file_id).into()), caption: Some(format!("Part of media {}", ok!(unique_id))), caption_entities: None, parse_mode: None, width: None, height: None, duration: None, thumb: None }),
                    "document" => InputMedia::Document(InputMediaDocument { media: InputFile::FileId(ok!(file_id).into()), caption: Some(format!("Part of media {}", ok!(unique_id))), caption_entities: None, parse_mode: None, thumb: None, disable_content_type_detection: None }),
                    _ => InputMedia::Photo(InputMediaPhoto { media: InputFile::FileId(ok!(file_id).into()), caption: Some(format!("Part of media {}", ok!(unique_id))), caption_entities: None, parse_mode: None }),
                };
                vec.push(im);
                true
            }));
            HResponse::Media(vec)
        }
        Command::UsernameAndAge { username, age } =>
            HResponse::URL(vec![format!("Your username is @{} and age is {}.", username, age)])
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