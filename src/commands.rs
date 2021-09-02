use teloxide::RequestError;
use teloxide::types::{InputMedia, InputMediaVideo, InputMediaAnimation, InputMediaPhoto, InputMediaAudio, InputMediaDocument, InputFile};
use teloxide::utils::command::BotCommand;

use sqlite::Connection;

//use futures::executor::block_on;

use std::env;

use rtdlib::Tdlib;
use rtdlib::types::*;

use super::models::HResponse;

#[derive(BotCommand)]
#[command(rename = "lowercase", description = "These commands are supported:")]
pub enum Command {
    #[command(description = "display this text.")]
    Help,
    #[command(description = "find users present in multiple groups")]
    FindInterUsers,
    #[command(description = "retrieves the last n stored media")]
    LastMediaStored(u8),
    #[command(description = "retrieves the last n stored urls")]
    LastUrlStored(u8),
    #[command(description = "retrieves the last n duplicate URLs found")]
    LastDuplicateUrls(u8),
    #[command(description = "retrieves the last n duplicate media found")]
    LastDuplicateMedia(u8),
    #[command(description = "list a user's groups")]
    ListUserGroups(i64),
    #[command(description = "find all users on multiple groups")]
    GetChatParticipants,
}

fn prepare_input_media(ftype: &str, file_id: Option<&str>, unique_id: Option<&str>) -> InputMedia {
    match ftype {
        "photo" => InputMedia::Photo(InputMediaPhoto { media: InputFile::FileId(ok!(file_id).into()), caption: Some(format!("Part of media {}", ok!(unique_id))), caption_entities: None, parse_mode: None }),
        "video" => InputMedia::Video(InputMediaVideo { media: InputFile::FileId(ok!(file_id).into()), caption: Some(format!("Part of media {}", ok!(unique_id))), caption_entities: None, parse_mode: None, thumb: None, width: None, height: None, duration: None, supports_streaming: None }),
        "audio" => InputMedia::Audio(InputMediaAudio { media: InputFile::FileId(ok!(file_id).into()), caption: Some(format!("Part of media {}", ok!(unique_id))), caption_entities: None, parse_mode: None, thumb: None, performer: None, title: None, duration: None }),
        "animation" => InputMedia::Animation(InputMediaAnimation { media: InputFile::FileId(ok!(file_id).into()), caption: Some(format!("Part of media {}", ok!(unique_id))), caption_entities: None, parse_mode: None, width: None, height: None, duration: None, thumb: None }),
        "document" => InputMedia::Document(InputMediaDocument { media: InputFile::FileId(ok!(file_id).into()), caption: Some(format!("Part of media {}", ok!(unique_id))), caption_entities: None, parse_mode: None, thumb: None, disable_content_type_detection: None }),
        _ => InputMedia::Photo(InputMediaPhoto { media: InputFile::FileId(ok!(file_id).into()), caption: Some(format!("Part of media {}", ok!(unique_id))), caption_entities: None, parse_mode: None }),
    }
}

pub fn handle_command(
    connection: &Connection,
    command: Command,
    chat_id: i64,
) -> Result<HResponse, RequestError> {
    let get_participants_reply = String::from("Comando ejecutado, ahora puede ejecutar /findinterusers");
    let r = match command {
        Command::Help => HResponse::URL(vec![Command::descriptions()]),
        Command::LastMediaStored(num) => {
            let select = format!("SELECT * FROM media  WHERE chat_id = {} GROUP BY msg_id ORDER BY timestamp DESC limit {};", chat_id, num);
            let mut vec = Vec::new();
            ok!(connection.iterate(select, |dbmedia| {
                let (_, file_type) = dbmedia[2];
                let (_, unique_id) = dbmedia[3];
                let (_, file_id) = dbmedia[4];
                let ftype = ok!(file_type);
                let im: InputMedia = prepare_input_media(ftype, file_id, unique_id);
                vec.push(im);
                true
            }));
            HResponse::Media(vec)
        },
        Command::LastUrlStored(num) => {
            let select = format!("SELECT * FROM urls WHERE chat_id = {} ORDER BY timestamp DESC limit {};", chat_id, num);
            let mut vec = Vec::new();
            ok!(connection.iterate(select, |dbmedia| {
                let (_, unique_id) = dbmedia[1];
                let url: String = ok!(unique_id).into();
                vec.push(format!("* {}", url));
                true
            }));
            HResponse::URL(vec)
        },
        Command::LastDuplicateMedia(num) => {
            let select = format!("SELECT * FROM duplicates  WHERE chat_id = {} and file_type != 'url' ORDER BY timestamp DESC limit {};", chat_id, num);
            let mut vec = Vec::new();
            ok!(connection.iterate(select, |dbmedia| {
                let (_, unique_id) = dbmedia[1];
                let (_, file_type) = dbmedia[2];
                let (_, file_id) = dbmedia[3];
                let ftype = ok!(file_type);
                let im: InputMedia =  prepare_input_media(ftype, file_id, unique_id);
                vec.push(im);
                true
            }));
            HResponse::Media(vec)
        },
        Command::LastDuplicateUrls(num) => {
            let select = format!("SELECT unique_id FROM duplicates  WHERE chat_id = {} and file_type = 'url' ORDER BY timestamp DESC limit {};", chat_id, num);
            let mut vec = Vec::new();
            ok!(connection.iterate(select, |dbmedia| {
                let (_, unique_id) = dbmedia[0];
                let url: String = ok!(unique_id).into();
                vec.push(format!("* {}", url));
                true
            }));
            HResponse::URL(vec)
        },
        Command::FindInterUsers => {
            let select = "SELECT *, COUNT(*) as cnt FROM users GROUP BY user_id HAVING cnt > 1;";
            let mut vec = Vec::new();
            ok!(connection.iterate(select, |dbmedia| {
                let (_, user_id) = dbmedia[0];
                let (_, chat_id) = dbmedia[1];
                let (_, user_name) = dbmedia[2];
                let (_, chat_name) = dbmedia[3];
                let (_, cnt) = dbmedia[4];
                let hit = format!("UserId: {}, GroupId: {}, UserName: {}, GroupName: {}, found in {} groups", ok!(user_id), ok!(chat_id), ok!(user_name), ok!(chat_name), ok!(cnt));
                vec.push(hit);
                true
            }));
            HResponse::URL(vec)
        },
        Command::ListUserGroups(id) => {
            let select = format!("SELECT chat_id, chat_name FROM users WHERE user_id = {};", id);
            let mut vec = Vec::new();
            ok!(connection.iterate(select, |dbmedia| {
                let (_, chat_id) = dbmedia[0];
                let (_, chat_name) = dbmedia[1];
                let hit = format!("GroupId: {}, GroupName: {}", ok!(chat_id), ok!(chat_name));
                vec.push(hit);
                true
            }));
            HResponse::URL(vec)
        },
        Command::GetChatParticipants => {
            log::info!("Connecting to Telegram...");
            let chat_ids = get_chat_ids(connection);
            log::info!("chats: {:?}", chat_ids);
            get_participants(connection, chat_ids);

            HResponse::Text(get_participants_reply)
        }
    };
    Ok(r)
}

fn get_participants(connection: &Connection, chat_ids: Vec<i64>) {
    let api_id = match env::var("TG_ID") {
        Ok(s) => s.parse::<i32>().unwrap(),
        Err(_) => 0,
    };
    let api_hash = ok!(env::var("TG_HASH"));
    let token = ok!(env::var("TELOXIDE_TOKEN"));

    let tdlib: Tdlib = Tdlib::new();
    ok!(Tdlib::set_log_verbosity_level(3));

    loop {
        match tdlib.receive(2.0) {
            Some(event) => {
                log::info!("Event: {:?}", event);
                match serde_json::from_str::<UpdateAuthorizationState>(&event[..]) {
                    Ok(state) => {
                        if state.authorization_state().is_closed() {
                            log::info!("Authorization closed!");
                            break;
                        }
                        if state.authorization_state().is_ready() {
                            log::info!("Authorization ready!");
                            break;
                        }
                        if state.authorization_state().is_wait_encryption_key() {
                            tdlib.send(r#"{"@type": "checkDatabaseEncryptionKey", "encryption_key": ""}"#);
                            let bot_auth = format!("{{ \"@type\":\"checkAuthenticationBotToken\",\"token\":\"{}\" }}", token);
                            tdlib.send(bot_auth.as_str());
                        }
                        if state.authorization_state().is_wait_tdlib_parameters() {
                            let set_parameters = format!("{{ \"@type\":\"setTdlibParameters\",\"parameters\": {{\
                                    \"api_id\":\"{}\",\
                                    \"api_hash\":\"{}\",\
                                    \"bot_auth_token\":\"{}\",\
                                    \"database_directory\":\"tdlib\",\
                                    \"system_language_code\":\"en\",\
                                    \"device_model\":\"Desktop\",\
                                    \"application_version\":\"1.0.0\"\
                                    }} }}", api_id, api_hash, token);
                            log::info!("{}", set_parameters);
                            tdlib.send(set_parameters.as_str());
                        }
                        if state.authorization_state().is_wait_phone_number() {
                            log::info!("Wait phone number");
                        }
                        if state.authorization_state().is_wait_password() {
                            log::info!("Wait password");
                        }
                        if state.authorization_state().is_wait_code() {
                            log::info!("Wait code");
                        }
                        if state.authorization_state().is_wait_registration() {
                            log::info!("Wait registration");
                        }
                    }
                    Err(_) => ()
                }
            }
            None => ()
        }
    }

    for id in chat_ids {
        //block_on(get_participants(id));
        log::info!("chat_id: {}", id);
        let chat_request = format!("{{\"@type\":\"getChat\",\"chat_id\":\"{}\" }}", id);
        tdlib.send(chat_request.as_str());
        process_chat(connection, &tdlib,id);
    }

    log::info!("No more updates");
}

const LIMIT: i64 = 200;

fn process_chat(connection: &Connection, tdlib: &Tdlib, chat_id: i64) {
    let mut i: i64 = 0;
    let mut offset: i64 = 0;
    let mut supergroup_id: i64 = 0;
    let mut chat_name = String::new();
    let unknown = "Unknown";

    loop {
        match tdlib.receive(5.0) {
            Some(response) => {
                log::info!("Response: {}", response);
                match serde_json::from_str::<serde_json::Value>(&response[..]) {
                    Ok(v) => {
                        if v["@type"] == "chat" {
                            let chat: Chat = ok!(serde_json::from_value(v.clone()));
                            let chat = chat.clone();
                            if chat_id == chat.id() {
                                chat_name = chat.title().clone();
                                let supergroup = ok!(chat.type_().as_supergroup());
                                supergroup_id = supergroup.supergroup_id();
                                //let members_request = format!("{{ \"@type\":\"getSupergroupMembers\",\"supergroup_id\":\"{}\",\"offset\":\"{}\",\"limit\":\"200\" }}", supergroup.supergroup_id(), offset);
                                let members_request = serde_json::json!({
                                    "@type": "getSupergroupMembers",
                                    "supergroup_id": supergroup_id,
                                    "offset": offset,
                                    "limit": 200
                                });
                                tdlib.send(members_request.to_string().as_str());
                            }
                        }
                        if v["@type"] == "chatMembers" {
                            match  serde_json::from_value::<ChatMembers>(v.clone()) {
                                Ok(members) => {
                                    let total_count = members.total_count();
                                    for member in members.members() {
                                        match member.bot_info() {
                                            Some(_) => (),
                                            None => {
                                                log::info!("Member: {}", member.user_id());
                                                let insert = "INSERT INTO users (user_id, chat_id, user_name, chat_name) VALUES (?, ?, ?, ?)";
                                                let mut insert_stmt = ok!(connection.prepare(insert));
                                                ok!(insert_stmt.bind(1, member.user_id()));
                                                ok!(insert_stmt.bind(2, chat_id));
                                                ok!(insert_stmt.bind(3, unknown));
                                                ok!(insert_stmt.bind(4, chat_name.as_str()));

                                                let mut cursor = insert_stmt.cursor();
                                                match cursor.next() {
                                                    Ok(_) => (),
                                                    Err(e) => log::warn!("Expected error: {}", e)
                                                }
                                            }
                                        }
                                    }
                                    //let members_request = format!("{{ \"@type\":\"getSupergroupMembers\",\"supergroup_id\":\"{}\",\"offset\":\"{}\",\"limit\":\"200\" }}", supergroup.supergroup_id(), offset);
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
                                },
                                Err(e) => log::error!("Error deserializing ChatMembers: {}", e)
                            }
                        }
                    },
                    Err(e) => log::error!("Error: {}", e)
                }
            },
            None => if i <= 3 {
                i += 1;
                log::info!("{} timeout", i)
            } else { break }
        }
    }
}

fn get_chat_ids(connection: &Connection) -> Vec<i64> {
    let select = "SELECT DISTINCT chat_id from users;";
    let mut vec = Vec::new();

    ok!(connection.iterate(select, |dbmedia| {
        log::info!("{:?}", dbmedia);
        let (_, chatid) = dbmedia[0];
        /*let chatid = chatid.unwrap();
        let chatid = if chatid.starts_with("-") { chatid.strip_prefix("-100") } else { chatid.strip_prefix("100") };*/
        let chat_id = ok!(chatid.unwrap().parse::<i64>());
        vec.push(chat_id);
        true
    }));
    vec
}
