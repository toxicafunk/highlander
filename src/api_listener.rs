use rtdlib::types::{MessageContent, TextEntityType, UpdateDeleteMessages, UpdateNewMessage};
use rtdlib::Tdlib;

use sqlite::Connection;

use lazy_static::lazy_static;
use regex::Regex;

use super::models::create_connection;
use super::duplicates::extract_last250;

const INSERT: &str = "INSERT INTO mappings (api_id, chat_id, unique_id) VALUES (?, ?, ?)";

fn insert_mapping(connection: &Connection, id: i64, chat_id: i64, unique_id: &str) {
    let mut insert_stmt = ok!(connection.prepare(INSERT));

    ok!(insert_stmt.bind(1, id));
    ok!(insert_stmt.bind(2, chat_id));
    ok!(insert_stmt.bind(3, unique_id));

    let mut cursor = insert_stmt.cursor();
    match cursor.next() {
        Err(e) => log::error!("{}", e),
        Ok(r) => match r {
            None => (),
            Some(e) => log::error!("{:?}", e)
        }
    }
}

pub async fn tgram_listener(tdlib: Tdlib) -> () {
    let connection: Connection = create_connection();
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
                            let new_messages: UpdateNewMessage = ok!(serde_json::from_value(update_new_message));
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
                                                    insert_mapping(&connection, id, chat_id, unique_id)
                                                });
                                            },
                                            _ => ()

                                        }
                                    })
                                },
                                MessageContent::MessageAudio(message_audio) => {
                                    let unique_id = message_audio.audio().audio().remote().unique_id();
                                    insert_mapping(&connection, id, chat_id, unique_id)
                                },
                                MessageContent::MessageDocument(message_document) => {
                                    let unique_id = message_document.document().document().remote().unique_id();
                                    insert_mapping(&connection, id, chat_id, unique_id)
                                },
                                MessageContent::MessagePhoto(message_photo) => message_photo.photo().sizes().iter().for_each(|size| {
                                    let unique_id = size.photo().remote().unique_id().as_str();
                                    insert_mapping(&connection, id, chat_id, unique_id)
                                }),
                                MessageContent::MessageVideo(message_video) => {
                                    let unique_id = message_video.video().video().remote().unique_id();
                                    insert_mapping(&connection, id, chat_id, unique_id)
                                },
                                MessageContent::MessageVideoNote(message_video_note) => {
                                    let unique_id = message_video_note.video_note().video().remote().unique_id();
                                    insert_mapping(&connection, id, chat_id, unique_id)
                                },
                                MessageContent::MessageVoiceNote(message_voice_note) => {
                                    let unique_id = message_voice_note.voice_note().voice().remote().unique_id();
                                    insert_mapping(&connection, id, chat_id, unique_id)
                                },
                                _ => ()
                            }
                        }

                        if v["@type"] == "updateDeleteMessages" {
                            log::info!("Delete Listener Value: {}", v);
                            let update_delete_message = v.clone();
                            let deleted_messages: UpdateDeleteMessages = ok!(serde_json::from_value(update_delete_message));
                            let chat_id = deleted_messages.chat_id();
                            for msg_id in deleted_messages.message_ids() {
                                let delete_media = "DELETE FROM media WHERE unique_id = (SELECT unique_id FROM mappings WHERE api_id = ? and chat_id = ?)";
                                let mut delete_media_stmt = ok!(connection.prepare(delete_media));
                                ok!(delete_media_stmt.bind(1, *msg_id));
                                ok!(delete_media_stmt.bind(2, chat_id));
                                let mut delete_media_cursor = delete_media_stmt.cursor();
                                match delete_media_cursor.next() {
                                    Err(e) => log::error!("{}", e),
                                    Ok(r) => match r {
                                        None => (),
                                        Some(_) => log::info!("Delete media message {} on chat {}", msg_id, chat_id)
                                    }
                                }

                                let delete_urls = "DELETE FROM urls WHERE unique_id = (SELECT unique_id FROM mappings WHERE api_id = ? and chat_id = ?)";
                                let mut delete_urls_stmt = ok!(connection.prepare(delete_urls));
                                ok!(delete_urls_stmt.bind(1, *msg_id));
                                ok!(delete_urls_stmt.bind(2, chat_id));
                                let mut delete_urls_cursor = delete_urls_stmt.cursor();
                                match delete_urls_cursor.next() {
                                    Err(e) => log::error!("{}", e),
                                    Ok(r) => match r {
                                        None => (),
                                        Some(_) => log::info!("Delete url message {} on chat {}", msg_id, chat_id)
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => log::error!("Error: {}", e),
                }
            }
            None => ()
        }
    }
}
