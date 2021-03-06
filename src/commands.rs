use teloxide::types::{
    InputFile, InputMedia, InputMediaAnimation, InputMediaAudio, InputMediaDocument,
    InputMediaPhoto, InputMediaVideo,
};
use teloxide::utils::command::BotCommand;
use teloxide::RequestError;
use rtdlib::Tdlib;

use chrono::offset::{TimeZone, Utc};

use std::sync::Arc;

use super::models::HResponse;
use super::repository::Repository;
use super::rocksdb::RocksDBRepo;

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
    #[command(description = "retrieves the last n duplicate media found")]
    LastDuplicateMedia(u8),
    #[command(description = "retrieves the last n duplicate URLs found")]
    LastDuplicateUrls(u8),
    #[command(description = "list a user's groups")]
    ListUserGroups(i64),
    #[command(description = "find all users on multiple groups")]
    GetChatParticipants,
    #[command(description = "find all users who've remained inactive over n days")]
    FindInactiveUsers(i64),
    #[command(description = "ban all users who've remained inactive over n days")]
    BanInactiveUsers(i64),
    #[command(description = "find all users on multiple groups")]
    ListMedia(u8),
    #[command(description = "find all users on multiple groups")]
    ListUsers(u8),
    #[command(description = "find all users on multiple groups")]
    ListDuplicates(u8),
    #[command(description = "Get the Ids of all chats managed by highlander")]
    GetChatIds,
}

fn prepare_input_media(ftype: &str, file_id: Option<&str>, unique_id: Option<&str>) -> InputMedia {
    match ftype {
        "photo" => InputMedia::Photo(InputMediaPhoto {
            media: InputFile::FileId(ok!(file_id).into()),
            caption: Some(format!("Part of media {}", ok!(unique_id))),
            caption_entities: None,
            parse_mode: None,
        }),
        "video" => InputMedia::Video(InputMediaVideo {
            media: InputFile::FileId(ok!(file_id).into()),
            caption: Some(format!("Part of media {}", ok!(unique_id))),
            caption_entities: None,
            parse_mode: None,
            thumb: None,
            width: None,
            height: None,
            duration: None,
            supports_streaming: None,
        }),
        "audio" => InputMedia::Audio(InputMediaAudio {
            media: InputFile::FileId(ok!(file_id).into()),
            caption: Some(format!("Part of media {}", ok!(unique_id))),
            caption_entities: None,
            parse_mode: None,
            thumb: None,
            performer: None,
            title: None,
            duration: None,
        }),
        "animation" => InputMedia::Animation(InputMediaAnimation {
            media: InputFile::FileId(ok!(file_id).into()),
            caption: Some(format!("Part of media {}", ok!(unique_id))),
            caption_entities: None,
            parse_mode: None,
            width: None,
            height: None,
            duration: None,
            thumb: None,
        }),
        "document" => InputMedia::Document(InputMediaDocument {
            media: InputFile::FileId(ok!(file_id).into()),
            caption: Some(format!("Part of media {}", ok!(unique_id))),
            caption_entities: None,
            parse_mode: None,
            thumb: None,
            disable_content_type_detection: None,
        }),
        _ => InputMedia::Photo(InputMediaPhoto {
            media: InputFile::FileId(ok!(file_id).into()),
            caption: Some(format!("Part of media {}", ok!(unique_id))),
            caption_entities: None,
            parse_mode: None,
        }),
    }
}

fn str_to_option(str: &String) -> Option<&String> {
    if str == "" {
        None
    } else {
        Some(str)
    }
}

pub fn handle_command(
    db: RocksDBRepo,
    tdlib: Arc<Tdlib>,
    command: Command,
    chat_id: i64,
) -> Result<HResponse, RequestError> {
    let get_participants_reply =
        String::from("Comando ejecutado, ahora puede ejecutar /findinterusers");
    let response_too_long =
        String::from("Respuesta demasiado larga para mostart en Telegram, ver logs.");
    let r = match command {
        Command::Help => HResponse::URL(vec![Command::descriptions()]),
        Command::LastMediaStored(num) => {
            let media_vec = db.last_media_stored(chat_id, num.into(), false);
            let vec = media_vec
                .iter()
                .map(|media| {
                    let file_id = str_to_option(&media.file_id).map(|s| s.as_str());
                    let unique_id = str_to_option(&media.unique_id).map(|s| s.as_str());
                    prepare_input_media(media.file_type.as_str(), file_id, unique_id)
                })
                .collect();
            HResponse::Media(vec)
        }
        Command::LastUrlStored(num) => {
            let media_vec = db.last_media_stored(chat_id, num.into(), true);
            let vec = media_vec
                .iter()
                .map(|media| media.unique_id.to_owned())
                .collect();
            HResponse::URL(vec)
        }
        Command::LastDuplicateMedia(num) => {
            let media_vec = db.last_media_duplicated(chat_id, num.into(), false);
            let vec = media_vec
                .iter()
                .map(|media| {
                    let file_id = str_to_option(&media.file_id).map(|s| s.as_str());
                    let unique_id = str_to_option(&media.unique_id).map(|s| s.as_str());
                    prepare_input_media(media.file_type.as_str(), file_id, unique_id)
                })
                .collect();
            HResponse::Media(vec)
        }
        Command::LastDuplicateUrls(num) => {
            let media_vec = db.last_media_duplicated(chat_id, num.into(), true);
            let vec = media_vec
                .iter()
                .map(|media| media.unique_id.to_owned())
                .collect();
            HResponse::URL(vec)
        }
        Command::FindInterUsers => {
            let exclude_list: Vec<&str> = vec![
                "1733079574",
                "162726413",
                "1575436070",
                "1042885111",
                "785731637",
                "208056682",
            ];

            let vec = db
                .get_users_chat_count()
                .iter()
                .filter(|tup| {
                    let user_id = tup.0.user_id.to_string();
                    !exclude_list.contains(&user_id.as_str())
                })
                .map(|tup| {
                    let user = tup.0.clone();
                    let count = tup.1;
                    format!(
                        "UserId: {}, GroupId: {}, UserName: {} found in {} groups",
                        user.user_id, user.chat_id, user.user_name, count
                    )
                })
                .collect::<Vec<_>>();

            HResponse::URL(vec)
        }
        Command::ListUserGroups(id) => {
            let users_vec = db.list_user_groups(chat_id, id);
            let vec = users_vec
                .iter()
                .map(|user| format!("GroupId: {}, GroupName: {}", user.chat_id, user.chat_name))
                .collect();
            HResponse::URL(vec)
        }
        Command::GetChatParticipants => {
            log::info!("Connecting to Telegram...");
            let chat_ids = db.get_chat_ids();
            log::info!("chats: {:?}", chat_ids);
            get_participants(tdlib, chat_ids);
            HResponse::Text(get_participants_reply)
        }
        Command::FindInactiveUsers(ndays) => {
            let vec = db
                .inactive_users_before(ndays)
                .iter()
                .map(|user| {
                    format!(
                        "UserId: {}, UserName: {}, Last Update: {}",
                        user.user_id, user.user_name, Utc.timestamp(user.timestamp, 0)
                    )
                })
                .collect::<Vec<_>>();
            HResponse::URL(vec)
        }
        Command::BanInactiveUsers(ndays) => {
            let vec = db.inactive_users_before(ndays);
            HResponse::Ban(vec)
        }
        Command::ListMedia(num) => {
            let media_vec = db.list_media(num.into());
            let vec = media_vec
                .iter()
                .map(|media| format!("{:?}", media))
                .collect::<Vec<_>>();
            log::info!("ListMedia: {}", vec.join("\n"));
            HResponse::Text(response_too_long)
        }

        Command::ListUsers(num) => {
            let users_vec = db.list_users(num.into());
            let vec = users_vec
                .iter()
                .map(|user| format!("{:?}", user))
                .collect::<Vec<_>>();
            log::info!("ListUsers: {}", vec.join("\n"));
            HResponse::Text(response_too_long)
        }

        Command::ListDuplicates(num) => {
            let media_vec = db.list_duplicates(num.into());
            let vec = media_vec
                .iter()
                .map(|media| format!("{:?}", media))
                .collect::<Vec<_>>();
            log::info!("ListDuplicates: {}", vec.join("\n"));
            HResponse::Text(response_too_long)
        }
        Command::GetChatIds => {
            let vec = db
                .get_chat_ids()
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>();
            HResponse::Text(vec.join("\n"))
        }
    };
    Ok(r)
}

fn get_participants(tdlib: Arc<Tdlib>, chat_ids: Vec<i64>) {
    for id in chat_ids {
        let tdlib = tdlib.clone();
        //block_on(get_participants(id));
        log::info!("chat_id: {}", id);
        let chat_request = serde_json::json!({
            "@type": "getChat",
            "chat_id": id
        });
        tdlib.send(chat_request.to_string().as_str());
    }

    log::info!("No more updates");
}
