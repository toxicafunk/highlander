use teloxide::RequestError;
use teloxide::types::{InputMedia, InputMediaVideo, InputMediaAnimation, InputMediaPhoto, InputMediaAudio, InputMediaDocument, InputFile};
use teloxide::utils::command::BotCommand;
use sqlite::Connection;

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
        }
        Command::ListUserGroups(id) => {
            let select = format!("SELECT chat_id, chat_name FROM users WHERE user_id = {};", id);
            let mut vec = Vec::new();
            ok!(connection.iterate(select, |dbmedia| {
                log::info!("{:?}", dbmedia);
                let (_, chat_id) = dbmedia[0];
                let (_, chat_name) = dbmedia[1];
                let hit = format!("GroupId: {}, GroupName: {}", ok!(chat_id), ok!(chat_name));
                vec.push(hit);
                true
            }));
            HResponse::URL(vec)
        }
    };
    Ok(r)
}