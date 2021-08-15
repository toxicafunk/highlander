#[macro_use] mod macros;

use teloxide::prelude::*;
use teloxide::utils::command::BotCommand;
use teloxide::types::{ChatMember, ChatMemberStatus};

use sqlite::Connection;

use std::env;

use std::io::Write;
use chrono::Local;
use pretty_env_logger::env_logger::Builder;
use log::LevelFilter;

use tokio_stream::wrappers::UnboundedReceiverStream;

use highlander::models::HResponse;
use highlander::commands::*;
use highlander::duplicates::detect_duplicates;

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

                    match message.update.from() {
                        Some(user) => {
                            // Handle normal messages
                            let member: ChatMember = ok!(message.requester.get_chat_member(message.update.chat.id, user.id).await);
                            let r = detect_duplicates(&connection, &message, user);
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

                            // Handle commands
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
