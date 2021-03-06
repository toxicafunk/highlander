use lazy_static::lazy_static;
use regex::Regex;

use std::sync::Arc;

use teloxide::prelude::*;
use teloxide::types::{Chat, MediaKind, MessageKind, User};

use crate::models::*;
use crate::repository::Repository;
use crate::rocksdb::RocksDBRepo;

pub fn extract_last250(text: &str) -> &str {
    let l = text.len();
    let i = if l > 250 { l - 250 } else { 0 };
    text.get(i..l).unwrap_or("")
}

pub fn detect_duplicates(db: RocksDBRepo, message: &Message, user: &User) -> Status {
    let kind: MessageKind = message.kind.clone();
    let chat: Arc<Chat> = Arc::new(message.chat.clone());
    let msg_id: i32 = message.id;
    //log::info!("Message received: {:?}", message);

    store_user(db.clone(), user, chat.clone());

    let success = "Media will be unique for 5 days";
    let mut status = Status {
        action: false,
        respond: false,
        text: success.to_string(),
    };

    let r: Status = match kind {
        MessageKind::Common(msg_common) => match msg_common.media_kind {
            MediaKind::Text(text) => {
                lazy_static! {
                    static ref RE: Regex = ok!(Regex::new("(http|ftp|https)://([\\w_-]+(?:(?:\\.[\\w_-]+)+))([\\w.,@?^=%&:/~+#-]*[\\w@?^=%&/~+#-])?"));
                }
                let t: &str = &*text.text;
                let mut statuses: Vec<(Status, &str)> = Vec::new();
                RE.captures_iter(t).for_each(|cap| {
                    let url = cap.get(0).unwrap().as_str();
                    log::info!("Detected url: {}", url);
                    let chat = chat.clone();
                    let unique_id = extract_last250(url).into();
                    let sdo = SDO {
                        chat,
                        msg_id,
                        file_type: String::from("url"),
                        unique_id: unique_id,
                        file_id: None,
                    };
                    let new_status = handle_message(db.clone(), &status, sdo, "urls");
                    statuses.push((new_status, url));
                });

                if statuses.len() == 1 {
                    statuses[0].0.clone()
                } else if statuses.len() > 1 {
                    let has_valid_url = statuses.iter().any(|el| !el.0.action);
                    log::info!("Has Valid Url: {}", has_valid_url);
                    if has_valid_url {
                        // At least 1 url is NOT duplicate
                        let mut result =
                            statuses
                                .into_iter()
                                .fold((status, t.to_string()), |acc, el| {
                                    log::info!("status: {:?}", acc.0);
                                    if el.0.action {
                                        let stat = acc.0.clone();
                                        let new_text = acc.1.replace(el.1, "DUPLICATED");
                                        (stat, new_text)
                                    } else {
                                        (el.0, acc.1)
                                    }
                                });
                        result.0.text = result.1.to_string();
                        result.0
                    } else {
                        statuses[0].0.clone()
                    }
                } else {
                    status
                }
            }
            /*MediaKind::Animation(animation) => {
                let file_unique_id = animation.animation.file_unique_id;
                let file_id = animation.animation.file_id;
                log::info!("Animation: {:?}", message);
                let caption = &*animation.caption.unwrap_or(message.id.to_string());
                status.text = caption.into();
                let chat = chat.clone();
                let sdo = SDO { chat, msg_id, file_type: String::from("animation"), unique_id: file_unique_id, file_id: Some(file_id) };
                handle_message(db, status, sdo, "media")
            },*/
            MediaKind::Audio(audio) => {
                let file_unique_id = audio.audio.file_unique_id;
                let file_id = audio.audio.file_id;
                log::info!("Audio: {:?}", message);
                let caption = &*audio.caption.unwrap_or(message.id.to_string());
                status.text = caption.into();
                let chat = chat.clone();
                let sdo = SDO {
                    chat,
                    msg_id,
                    file_type: String::from("audio"),
                    unique_id: file_unique_id,
                    file_id: Some(file_id),
                };
                handle_message(db, &status, sdo, "media")
            }
            MediaKind::Document(document) => {
                let file_unique_id = document.document.file_unique_id;
                let file_id = document.document.file_id;
                log::info!("Document: {:?}", message);
                let caption = &*document.caption.unwrap_or(message.id.to_string());
                status.text = caption.into();
                let chat = chat.clone();
                let sdo = SDO {
                    chat,
                    msg_id,
                    file_type: String::from("document"),
                    unique_id: file_unique_id,
                    file_id: Some(file_id),
                };
                handle_message(db, &status, sdo, "media")
            }
            MediaKind::Photo(photo) => {
                log::info!("Photo: {:?}", message);
                let caption = &*photo.caption.unwrap_or(message.id.to_string());
                status.text = caption.into();
                photo.photo.iter().fold(status, |acc, p| {
                    let file_unique_id = &*p.file_unique_id;
                    let file_id = &*p.file_id;
                    let chat = chat.clone();
                    let sdo = SDO {
                        chat,
                        msg_id,
                        file_type: String::from("photo"),
                        unique_id: file_unique_id.into(),
                        file_id: Some(file_id.into()),
                    };
                    handle_message(db.clone(), &acc, sdo, "media")
                })
            }
            MediaKind::Video(video) => {
                let file_unique_id = video.video.file_unique_id;
                let file_id = video.video.file_id;
                let caption = &*video.caption.unwrap_or(message.id.to_string());
                log::info!("Video: {:?}", message);
                status.text = caption.into();
                let chat = chat.clone();
                let sdo = SDO {
                    chat,
                    msg_id,
                    file_type: String::from("video"),
                    unique_id: file_unique_id,
                    file_id: Some(file_id),
                };
                handle_message(db, &status, sdo, "media")
            }
            MediaKind::Voice(voice) => {
                let file_unique_id = voice.voice.file_unique_id;
                let file_id = voice.voice.file_id;
                log::info!("Voice: {:?}", message);
                let caption = &*voice.caption.unwrap_or(message.id.to_string());
                status.text = caption.into();
                let chat = chat.clone();
                let sdo = SDO {
                    chat,
                    msg_id,
                    file_type: String::from("voice"),
                    unique_id: file_unique_id,
                    file_id: Some(file_id),
                };
                handle_message(db, &status, sdo, "media")
            }
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

fn store_user(db: RocksDBRepo, user: &User, chat: Arc<Chat>) -> bool {
    let chat = chat.clone();
    if db.chat_user_exists(user, chat.clone()) {
        log::info!("store_user: user {} exists on chat {}", user.id, chat.id);
        db.update_user_timestamp(user, chat)
    } else {
        log::info!("store_user: creating user {} exists on chat {}", user.id, chat.id);
        db.insert_user(user, chat)
    }
}

fn handle_message(db: RocksDBRepo, acc: &Status, sdo: SDO, table: &str) -> Status {
    let is_media = table == "media";
    match db.item_exists(sdo.clone(), is_media) {
        None => {
            log::info!("inserting new media: {:?}", sdo);
            db.insert_item(sdo, is_media);
            Status::new(acc)
        }
        Some(media) => {
            log::info!("duplicate media: {:?}", media);
            let chat_id = media.chat_id;
            db.insert_duplicate(sdo);
            let orig_chat_id = match chat_id.to_string().strip_prefix("-100") {
                Some(s) => s.parse::<i64>().unwrap_or(chat_id),
                None => 0,
            };
            let orig_msg_id = media.msg_id;
            log::info!("orginal {} - {}", orig_chat_id, orig_msg_id);
            Status { action: true, respond: true, text: format!("Mensaje Duplicado: {} ya se ha compartido en los ultimos 5 dias.\nVer mensaje original: https://t.me/c/{}/{}", table, orig_chat_id, orig_msg_id) }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::duplicates::extract_last250;
    use lazy_static::lazy_static;
    use regex::Regex;

    const T1: &str = "hola https://twitter.com/plaforscience/status/1379526168513277960";
    const T2: &str = "hola https://twitter.com/plaforscience/status/1379526168513277960 y ademas https://youtu.be/GCI0NMgVfPk";
    const T3: &str =
        "https://drive.google.com/file/d/1t3_HeKZDIMEJl5_Y_l7uuIt4IeebCN7e/view?usp=sharing";

    lazy_static! {
        static ref RE: Regex = ok!(Regex::new("(http|ftp|https)://([\\w_-]+(?:(?:\\.[\\w_-]+)+))([\\w.,@?^=%&:/~+#-]*[\\w@?^=%&/~+#-])?"));
    }

    #[test]
    fn captures_1_url() {
        let caps = RE.captures(T1).unwrap();
        let url = caps.get(0).unwrap().as_str();
        println!("T1: {}", url);
        println!("{}", extract_last250(url));

        assert_eq!(
            url,
            "https://twitter.com/plaforscience/status/1379526168513277960"
        );
    }

    #[test]
    fn captures_2_url() {
        let caps = RE.captures_iter(T2);
        let mut count = 0;
        for cap in caps {
            println!("{:?}", cap);
            let url = cap.get(0).unwrap().as_str();
            println!("T2: {}", url);
            println!("{}", extract_last250(url));
            count += 1;
        }

        assert_eq!(count, 2);
    }

    #[test]
    fn captures_3_url() {
        let caps = RE.captures_iter(T3);
        let mut count = 0;
        for cap in caps {
            println!("{:?}", cap);
            let url = cap.get(0).unwrap().as_str();
            println!("T3: {}", url);
            println!("{}", extract_last250(url));
            count += 1;
        }

        assert_eq!(count, 1);
    }
}
