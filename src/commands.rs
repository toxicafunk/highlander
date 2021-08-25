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
        }
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
        }
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
        }
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
        }
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
        }
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
        }
        Command::GetChatParticipants => {
            log::info!("Connecting to Telegram...");
            let chat_ids = get_chat_ids(connection);
            log::info!("chats: {:?}", chat_ids);
            get_participants(chat_ids);

            HResponse::URL(Vec::new())
        }
    };
    Ok(r)
}

fn get_participants(chat_ids: Vec<i64>) {
    let api_id = match env::var("TG_ID") {
        Ok(s) => s.parse::<i32>().unwrap(),
        Err(_) => 0,
    };
    let api_hash = ok!(env::var("TG_HASH"));
    let token = ok!(env::var("TELOXIDE_TOKEN"));

    let tdlib = Tdlib::new();
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
        let chat_request = format!("{{ \"@type\":\"getSupergroupMembers\",\"supergroup_id\":\"{}\",\"offset\":\"0\",\"limit\":\"10\" }}", id);
        tdlib.send(chat_request.as_str());
    }

    loop {
        match tdlib.receive(5.0) {
            /*Some(response) => match serde_json::from_str::<ChatMembers>(&response[..]) {
                Ok(members) => {
                    log::info!("ChatMembers: {:?}", members)
                },
                Err(e) => log::error!("Error: {}", e)
            },*/
            Some(response) => log::info!("Response: {}", response),
            None => break
        }
    }
    log::info!("No more updates");
}

fn get_chat_ids(connection: &Connection) -> Vec<i64> {
    let select = "SELECT DISTINCT chat_id from users;";
    let mut vec = Vec::new();
    ok!(connection.iterate(select, |dbmedia| {
        log::info!("{:?}", dbmedia);
        let (_, chatid) = dbmedia[0];
        let chatid = chatid.unwrap();
        let chatid = if chatid.starts_with("-") { chatid.strip_prefix("-100") } else { chatid.strip_prefix("100") };
        let chat_id = ok!(chatid.unwrap().parse::<i64>());
        vec.push(chat_id);
        true
    }));
    vec
}
