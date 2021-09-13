#[macro_use]
mod macros;

use teloxide::prelude::*;
use teloxide::utils::command::BotCommand;
use teloxide::types::{ChatMember, ChatMemberStatus};

use std::io::Write;

use chrono::Local;
use pretty_env_logger::env_logger::Builder;
use log::LevelFilter;

use tokio_stream::wrappers::UnboundedReceiverStream;

use highlander::models::{HResponse, create_connection};
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
                    let connection = create_connection();

                    match message.update.from() {
                        Some(user) => {
                            // Handle normal messages
                            let member: ChatMember = ok!(message.requester.get_chat_member(message.update.chat.id, user.id).await);
                            let is_admin = match member.status() {
                                ChatMemberStatus::Administrator => true,
                                ChatMemberStatus::Creator => true,
                                _ => false
                            };

                            let r = detect_duplicates(&connection, &message, user);
                            if !is_admin {
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
                            }

                            // Handle commands
                            let txt_opt = message.update.text();
                            let bot_name = "highlander";
                            //let bot_name = "ramirez";

                            match txt_opt {
                                Some(txt) => match Command::parse(txt, bot_name) {
                                    Ok(command) => {
                                        if is_admin {
                                            let cr = handle_command(&connection, command, message.update.chat_id());
                                            match cr {
                                                Ok(hr) => match hr {
                                                    HResponse::URL(urls) => {
                                                        let ans: String = urls.join("\n");
                                                        if ans.is_empty() {
                                                            ok!(message.answer("No results found").await);
                                                        } else {
                                                            match message.answer(ans.as_str()).await {
                                                                Ok(_) => (),
                                                                Err(e) => {
                                                                    log::error!("Error {}", e);
                                                                    log::info!("Tried to send {}", ans)
                                                                }
                                                            }
                                                        }
                                                    }
                                                    HResponse::Media(vec) => {
                                                        match message.answer_media_group(vec).await {
                                                            Ok(_) => (),
                                                            Err(e) => log::error!("Error: {:?}", e)
                                                        }
                                                    }
                                                    HResponse::Text(msg) => {
                                                        match message.answer(msg).await {
                                                            Ok(_) => (),
                                                            Err(e) => log::error!("Error: {:?}", e)
                                                        }
                                                    },
                                                },
                                                Err(e) => log::error!("Error: {:?}", e)
                                            }
                                        } else {
                                            ok!(message.answer("lamentablemente, este comando es solo para usuarios Admin").await);
                                        }
                                    }
                                    Err(_) => ()
                                },
                                None => ()
                            }
                        }
                        None => ()
                    }
                },
            )
        })
        .dispatch()
        .await;
}