use rtdlib::types::{
    Chat, ChatMembers, ChatType, MessageContent, MessageSender, TextEntityType,
    UpdateDeleteMessages, UpdateNewMessage,
};
use rtdlib::Tdlib;

use chrono::offset::Utc;
use lazy_static::lazy_static;
use regex::Regex;

use std::collections::VecDeque;
use std::sync::Arc;

use tokio::time::{sleep, Duration};

use super::duplicates::extract_last250;
use super::models::{Group, User};
use super::repository::Repository;
use super::rocksdb::RocksDBRepo;

const LIMIT: i64 = 200;

pub async fn tgram_listener(tdlib: Arc<Tdlib>, db: RocksDBRepo) -> () {
    let mut channel: VecDeque<Group> = VecDeque::new();
    loop {
        match tdlib.receive(5.0) {
            Some(response) => {
                let tdlib = tdlib.clone();
                let db = db.clone();
                //log::info!("Listener Response: {}", response);
                match serde_json::from_str::<serde_json::Value>(&response[..]) {
                    Ok(v) => {
                        //log::info!("General Listener Value: {}", v);
                        if v["@type"] == "updateNewMessage" {
                            log::info!("New Message Listener Value: {}", v);
                            let update_new_message = v.clone();
                            let new_messages: UpdateNewMessage =
                                ok!(serde_json::from_value(update_new_message));
                            let message = new_messages.message();
                            let id = message.id();
                            let chat_id = message.chat_id();

                            match message.content() {
                                MessageContent::MessageText(message_text) => {
                                    lazy_static! {
                                        static ref RE: Regex = ok!(Regex::new("(http|ftp|https)://([\\w_-]+(?:(?:\\.[\\w_-]+)+))([\\w.,@?^=%&:/~+#-]*[\\w@?^=%&/~+#-])?"));
                                    }

                                    let txt = message_text.text().text();
                                    message_text.text().entities().iter().for_each(|entity| {
                                        log::info!("Entity: {:?}", entity);
                                        match entity.type_() {
                                            TextEntityType::Url(_) => {
                                                RE.captures_iter(txt).for_each(|cap| {
                                                    let url = cap.get(0).unwrap().as_str();
                                                    let unique_id = extract_last250(url);
                                                    db.insert_mapping(id, chat_id, unique_id);
                                                });
                                            }
                                            _ => (),
                                        }
                                    })
                                }
                                MessageContent::MessageAudio(message_audio) => {
                                    let unique_id =
                                        message_audio.audio().audio().remote().unique_id();
                                    db.insert_mapping(id, chat_id, unique_id);
                                }
                                MessageContent::MessageDocument(message_document) => {
                                    let unique_id =
                                        message_document.document().document().remote().unique_id();
                                    db.insert_mapping(id, chat_id, unique_id);
                                }
                                MessageContent::MessagePhoto(message_photo) => {
                                    message_photo.photo().sizes().iter().for_each(|size| {
                                        let unique_id = size.photo().remote().unique_id().as_str();
                                        db.insert_mapping(id, chat_id, unique_id);
                                    })
                                }
                                MessageContent::MessageVideo(message_video) => {
                                    let unique_id =
                                        message_video.video().video().remote().unique_id();
                                    db.insert_mapping(id, chat_id, unique_id);
                                }
                                MessageContent::MessageVideoNote(message_video_note) => {
                                    let unique_id = message_video_note
                                        .video_note()
                                        .video()
                                        .remote()
                                        .unique_id();
                                    db.insert_mapping(id, chat_id, unique_id);
                                }
                                MessageContent::MessageVoiceNote(message_voice_note) => {
                                    let unique_id = message_voice_note
                                        .voice_note()
                                        .voice()
                                        .remote()
                                        .unique_id();
                                    db.insert_mapping(id, chat_id, unique_id);
                                }
                                _ => (),
                            }
                        }

                        if v["@type"] == "updateDeleteMessages" {
                            log::info!("Delete Listener Value: {}", v);
                            let update_delete_message = v.clone();
                            let deleted_messages: UpdateDeleteMessages =
                                ok!(serde_json::from_value(update_delete_message));
                            db.delete_item(deleted_messages)
                        }

                        if v["@type"] == "chat" {
                            let chat_json = v.clone();
                            log::info!("Chat: {}", chat_json);
                            let chat: Chat = ok!(serde_json::from_value(chat_json));
                            let chat = chat.clone();
                            let chat_id = chat.id();
                            let supergroup_id = match chat.type_() {
                                ChatType::BasicGroup(basic) => basic.basic_group_id(),
                                ChatType::Supergroup(group) => group.supergroup_id(),
                                ChatType::Private(private) => private.user_id(),
                                ChatType::Secret(secret) => secret.user_id(),
                                _ => 0,
                            };

                            let members_request = serde_json::json!({
                                "@type": "getSupergroupMembers",
                                "supergroup_id": supergroup_id,
                                "offset": 0,
                                "limit": 200
                            });

                            let group = Group {
                                supergroup_id,
                                chat_id,
                                offset: 0,
                                timestamp: Utc::now().timestamp(),
                            };
                            channel.push_back(group);
                            tdlib.send(members_request.to_string().as_str());
                            sleep(Duration::from_millis(2000)).await;
                        }

                        if v["@type"] == "chatMembers" {
                            let members_json = v.clone();
                            log::info!("chatMembers: {}", members_json);
                            match serde_json::from_value::<ChatMembers>(members_json) {
                                Ok(members) => match channel.pop_front() {
                                    Some(g) => {
                                        let total_count = members.total_count();
                                        log::info!(
                                            "chatMembers: total {} chat_id {} supergroup_id {}",
                                            total_count,
                                            g.chat_id,
                                            g.supergroup_id
                                        );
                                        for member in members.members() {
                                            let dbuser = match member.member_id() {
                                                Some(MessageSender::User(sender)) => {
                                                    log::info!("MessageSender:User: {:?}", sender);
                                                    Some(User {
                                                        user_id: sender.user_id(),
                                                        chat_id: g.chat_id,
                                                        user_name: String::default(),
                                                        chat_name: String::default(),
                                                        timestamp: Utc::now().timestamp(),
                                                    })
                                                }
                                                Some(_) => {
                                                    log::warn!(
                                                    "chatMembers: This shouldn't have happened!"
                                                );
                                                    None
                                                }
                                                None => Some(User {
                                                    user_id: member.user_id().unwrap(),
                                                    chat_id: g.chat_id,
                                                    user_name: String::default(),
                                                    chat_name: String::default(),
                                                    timestamp: Utc::now().timestamp(),
                                                }),
                                            };

                                            match dbuser {
                                                None => (),
                                                Some(user) => {
                                                    if db.chat_dbuser_exists(
                                                        user.user_id,
                                                        user.chat_id,
                                                    ) {
                                                        log::info!("chatMembers: exists {:?}", user)
                                                    } else {
                                                        log::info!(
                                                            "chatMembers: inserting {:?}",
                                                            user
                                                        );
                                                        db.insert_dbuser(user);
                                                    }
                                                }
                                            }
                                        }

                                        let new_offset = g.offset + LIMIT;
                                        if new_offset < total_count {
                                            let members_request = serde_json::json!({
                                            "@type": "getSupergroupMembers",
                                            "supergroup_id": g.supergroup_id,
                                            "offset": new_offset,
                                            "limit": LIMIT
                                            });

                                            let group = Group {
                                                supergroup_id: g.supergroup_id,
                                                chat_id: g.chat_id,
                                                offset: new_offset,
                                                timestamp: Utc::now().timestamp()
                                            };
                                            channel.push_back(group);
                                            tdlib.send(members_request.to_string().as_str());
                                            sleep(Duration::from_millis(2000)).await;
                                        }
                                    },
                                    None => log::error!("Request received but no group on queue")
                                },
                                Err(e) => log::error!("Error deserializing ChatMembers: {}", e),
                            }
                        }
                    }
                    Err(e) => log::error!("Error: {}", e),
                }
            }
            None => (),
        }
    }
}
