#[macro_use]
mod macros;

use teloxide::prelude::*;
use teloxide::types::{ChatMember, ChatMemberStatus, True};
use teloxide::utils::command::BotCommand;
use teloxide::RequestError;

use tokio::spawn;
use tokio_stream::wrappers::UnboundedReceiverStream;

//use std::convert::Infallible;
use std::env;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use chrono::Local;
use lazy_static::lazy_static;
use log::LevelFilter;
use pretty_env_logger::env_logger::Builder;

use rtdlib::types::UpdateAuthorizationState;
use rtdlib::Tdlib;

use highlander::api_listener::tgram_listener;
use highlander::commands::*;
use highlander::duplicates::detect_duplicates;
use highlander::models::HResponse;
use highlander::models::User as DBUser;
use highlander::repository::Repository;
use highlander::rocksdb::RocksDBRepo;

static INIT_FLAG: AtomicBool = AtomicBool::new(true);

lazy_static! {
    static ref DB: RocksDBRepo = Repository::init();
    static ref TDLIB: Arc<Tdlib> = Arc::new(Tdlib::new());
}

fn init_tgram() -> () {
    log::info!("Initializing API");

    let api_id = match env::var("TG_ID") {
        Ok(s) => s.parse::<i32>().unwrap(),
        Err(_) => 0,
    };
    let api_hash = ok!(env::var("TG_HASH"));
    let token = ok!(env::var("TELOXIDE_TOKEN"));

    ok!(Tdlib::set_log_verbosity_level(1));

    loop {
        match TDLIB.receive(2.0) {
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
                            TDLIB.send(
                                r#"{"@type": "checkDatabaseEncryptionKey", "encryption_key": ""}"#,
                            );
                            let bot_auth = format!(
                                "{{ \"@type\":\"checkAuthenticationBotToken\",\"token\":\"{}\" }}",
                                token
                            );
                            TDLIB.send(bot_auth.as_str());
                        }
                        if state.authorization_state().is_wait_tdlib_parameters() {
                            let set_parameters = format!(
                                "{{ \"@type\":\"setTdlibParameters\",\"parameters\": {{\
                                    \"api_id\":\"{}\",\
                                    \"api_hash\":\"{}\",\
                                    \"bot_auth_token\":\"{}\",\
                                    \"database_directory\":\"tdlib\",\
                                    \"system_language_code\":\"en\",\
                                    \"device_model\":\"Desktop\",\
                                    \"application_version\":\"1.0.0\"\
                                    }} }}",
                                api_id, api_hash, token
                            );
                            log::info!("{}", set_parameters);
                            TDLIB.send(set_parameters.as_str());
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
                    Err(_) => (),
                }
            }
            None => (),
        }
    }
}

#[tokio::main]
async fn main() {
    run().await;
}

async fn run() {
    Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
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
            UnboundedReceiverStream::new(rx).for_each_concurrent(None, |cx| async move {
                let is_test_mode: bool = match env::var("HIGHLANDER_TEST_MODE") {
                    Ok(mode) => {
                        if mode == "true" {
                            true
                        } else {
                            false
                        }
                    }
                    Err(_) => false,
                };

                if INIT_FLAG.load(Ordering::Relaxed) {
                    init_tgram();
                    spawn(tgram_listener(TDLIB.clone(), DB.clone()));
                    INIT_FLAG.store(false, Ordering::Relaxed);
                }

                let message: &Message = &cx.update;
                match message.from() {
                    Some(user) => {
                        // Handle normal messages
                        let member: ChatMember =
                            ok!(cx.requester.get_chat_member(message.chat.id, user.id).await);
                        let is_admin = match member.status() {
                            ChatMemberStatus::Administrator => true,
                            ChatMemberStatus::Owner => true,
                            _ => false,
                        };

                        let status = detect_duplicates(DB.clone(), &message, user);
                        if is_test_mode || !is_admin {
                            let success = if status.action {
                                let mr = cx.delete_message().await;
                                match mr {
                                    Ok(m) => {
                                        log::info!("Deleted message: {:?}", m);
                                        true
                                    },
                                    Err(e) => {
                                        log::error!("Error: {:?}", e);
                                        false
                                    }
                                }
                            } else {
                                true
                            };

                            if status.respond && success {
                                let mr = cx.answer(status.text).await;
                                match mr {
                                    Ok(m) => log::info!("Responded: {:?}", m),
                                    Err(e) => log::error!("Error: {:?}", e),
                                }
                            }
                        }

                        // Handle commands
                        let txt_opt = message.text();
                        let bot_name = "highlander";
                        //let bot_name = "ramirez";

                        match txt_opt {
                            Some(txt) => match Command::parse(txt, bot_name) {
                                Ok(command) => {
                                    if is_admin {
                                        let cr = handle_command(DB.clone(), TDLIB.clone(), command, message.chat_id());
                                        match cr {
                                            Ok(hr) => match hr {
                                                HResponse::URL(urls) => {
                                                    let ans: String = urls.join("\n");
                                                    if ans.is_empty() {
                                                        ok!(cx.answer("No results found").await);
                                                    } else {
                                                        match cx.answer(ans.as_str()).await {
                                                            Ok(_) => (),
                                                            Err(e) => {
                                                                log::error!("Error {}", e);
                                                                log::info!("Tried to send {}", ans)
                                                            }
                                                        }
                                                    }
                                                }
                                                HResponse::Media(vec) => {
                                                    match cx.answer_media_group(vec).await {
                                                        Ok(_) => (),
                                                        Err(e) => log::error!("Error: {:?}", e)
                                                    }
                                                }
                                                HResponse::Text(msg) => {
                                                    match cx.answer(msg).await {
                                                        Ok(_) => (),
                                                        Err(e) => log::error!("Error: {:?}", e)
                                                    }
                                                }
                                                HResponse::Ban(users) => {
                                                    let b = users.iter()
                                                                 .map(|user| ban_user(&cx, message.chat_id(), user))
                                                                 .collect::<Vec<_>>();
                                                    log::info!("Banned {} users", b.len())
                                                }
                                            },
                                            Err(e) => log::error!("Error: {:?}", e)
                                        }
                                    } else {
                                        ok!(cx.answer("Lamentablemente, este comando es solo para usuarios Admin").await);
                                    }
                                }
                                Err(_) => ()
                            },
                            None => ()
                        }
                    }
                    None => (),
                }
            })
        })
        .dispatch()
        .await;
}

type Cx = UpdateWithCx<AutoSend<Bot>, Message>;

async fn ban_user(cx: &Cx, chat_id: i64, user: &DBUser) -> Result<True, RequestError> {
    cx.requester
        .ban_chat_member(chat_id, user.user_id)
        .until_date(0)
        .await
}
