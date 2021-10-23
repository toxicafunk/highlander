use rtdlib::types::{
    Chat, ChatMembers, ChatType, MessageContent, TextEntityType,
    UpdateDeleteMessages, UpdateNewMessage,
};
use rtdlib::Tdlib;

use chrono::offset::Utc;
use lazy_static::lazy_static;
use regex::Regex;

use std::collections::VecDeque;
use std::sync::Arc;

use super::duplicates::extract_last250;
use super::models::User;
use super::repository::Repository;
use super::rocksdb::RocksDBRepo;

const LIMIT: i64 = 200;

pub async fn tgram_listener(tdlib: Arc<Tdlib>, db: RocksDBRepo) -> () {
    let mut offset: i64 = 0;
    let mut supergroup_id: i64 = 0;
    let mut chat_ids: VecDeque<i64> = VecDeque::new();
    loop {
        match tdlib.receive(5.0) {
            Some(response) => {
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
                            supergroup_id = match chat.type_() {
                                ChatType::BasicGroup(basic) => basic.basic_group_id(),
                                ChatType::Supergroup(group) => group.supergroup_id(),
                                ChatType::Private(private) => private.user_id(),
                                ChatType::Secret(secret) => secret.user_id(),
                                _ => 0,
                            };

                            chat_ids.push_back(supergroup_id);
                            let members_request = serde_json::json!({
                                "@type": "getSupergroupMembers",
                                "supergroup_id": supergroup_id,
                                "offset": offset,
                                "limit": 200
                            });
                            tdlib.send(members_request.to_string().as_str());
                        }

                        if v["@type"] == "chatMembers" {
                            let members_json = v.clone();
                            log::info!("chatMembers: {}", members_json);
                            match serde_json::from_value::<ChatMembers>(members_json) {
                                Ok(members) => {
                                    let total_count = members.total_count();
                                    let chat_id = chat_ids.pop_front().unwrap();
                                    log::info!(
                                        "chatMembers: total {} chat_id {}",
                                        total_count,
                                        chat_id
                                    );
                                    for member in members.members() {
                                        let dbuser = User {
                                            user_id: member.user_id(),
                                            chat_id,
                                            user_name: String::default(),
                                            chat_name: String::default(),
                                            timestamp: Utc::now().timestamp(),
                                        };
                                        if db.chat_dbuser_exists(dbuser.user_id, dbuser.chat_id) {
                                            log::info!("chatMembers: exists {:?}", dbuser)
                                        } else {
                                            log::info!("chatMembers: inserting {:?}", dbuser);
                                            db.insert_dbuser(dbuser);
                                        }
                                    }

                                    if (offset + LIMIT) < total_count {
                                        offset += LIMIT;
                                        let members_request = serde_json::json!({
                                        "@type": "getSupergroupMembers",
                                        "supergroup_id": supergroup_id,
                                        "offset": offset,
                                        "limit": LIMIT
                                        });
                                        tdlib.send(members_request.to_string().as_str());
                                    }
                                }
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
