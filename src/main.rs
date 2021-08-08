#[macro_use] mod macros;

use teloxide::prelude::*;
use teloxide::utils::command::BotCommand;
use teloxide::types::{MessageKind, MediaKind, ChatMember, ChatMemberStatus};

use sqlite::Connection;

use lazy_static::lazy_static;
use regex::Regex;
use std::env;

use std::io::Write;
use chrono::Local;
use pretty_env_logger::env_logger::Builder;
use log::LevelFilter;

use tokio_stream::wrappers::UnboundedReceiverStream;

use highlander::models::{HResponse, SDO, Status};
use highlander::commands::*;
use highlander::duplicates::handle_message;

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

                    match message.update.from() {
                        Some(user) => {
                            let member: ChatMember = ok!(message.requester.get_chat_member(message.update.chat.id, user.id).await);
                            let is_admin = match member.status() {
                                ChatMemberStatus::Administrator => true,
                                ChatMemberStatus::Creator => true,
                                _ => false
                            };
                            if is_admin {
                                let txt_opt = message.update.text();
                                let bot_name = "ramirez";

                                match txt_opt {
                                    Some(txt) => match Command::parse(txt, bot_name) {
                                        Ok(command) => {
                                            let cr = handle_command(&connection, command, message.update.chat_id());
                                            match cr {
                                                Ok(hr) => match hr {
                                                    HResponse::URL(urls) => {
                                                        let ans = urls.join("\n");
                                                        ok!(message.answer(ans).await);
                                                    },
                                                    HResponse::Media(vec) => {
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
