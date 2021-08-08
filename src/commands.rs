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
    #[command(description = "retrieves the last n stored media")]
    LastMediaStored(u8),
    #[command(description = "retrieves the last n stored urls")]
    LastUrlStored(u8),
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
    };
    Ok(r)
}