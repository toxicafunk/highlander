#[macro_use]
mod macros;

use teloxide::prelude::*;
use teloxide::types::{ChatMemberStatus, True};
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

use rtdlib::types::RObject;
use rtdlib::types::UpdateAuthorizationState;
use rtdlib::Tdlib;

use tokio::time::{sleep, Duration};

use highlander::api_listener::tgram_listener;
use highlander::commands::*;
use highlander::duplicates::{build_message, chat_id_for_link, detect_duplicates};
use highlander::models::{HResponse, Local as HLocal};
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

async fn notify_staff(chat_id: i64, msg_id: i32) {
    let chat_id_link = chat_id_for_link(chat_id);
    let link = format!(
        "Se requiere intervencion de un `admin`: https://t.me/c/{}/{}",
        chat_id_link, msg_id
    );
    let send_message = build_message(link, -1001193436037);
    match send_message.to_json() {
        Err(e) => log::error!(
            "Failed to convert send_message to json for {} {}\n{}",
            chat_id,
            msg_id,
            e
        ),
        Ok(msg) => {
            log::info!("Sending: {}", msg);
            TDLIB.send(msg.as_str());
            sleep(Duration::from_millis(1000)).await;
            log::info!("Notification sent!")
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
                        let is_admin = match cx.requester.get_chat_member(message.chat.id, user.id).await {
                            Ok(member) => match member.status() {
                                ChatMemberStatus::Administrator => true,
                                ChatMemberStatus::Owner => true,
                                _ => false,
                            },
                            Err(_) => false
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
                                let mr = match status.reply_markup {
                                    None => cx.answer(status.text).await,
                                    Some(mrkup) => cx.answer(status.text).reply_markup(mrkup).send().await
                                };
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
                            Some(txt) => {
                                if let Some(_) = txt.find("@admin") {
                                    log::info!("Notificando a admin");
                                    notify_staff(message.chat_id(), message.id).await;
                                }
                                match Command::parse(txt, bot_name) {
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
                            }
                        },
                            None => ()
                        }
                    }
                    None => (),
                }
            })
        })
        .callback_queries_handler(handle_callback_query)
        .dispatch()
        .await;
}

async fn handle_callback_query(rx: DispatcherHandlerRx<AutoSend<Bot>, CallbackQuery>) {
    UnboundedReceiverStream::new(rx)
        .for_each_concurrent(None, |cx| async move { handle_callback(cx).await })
        .await;
}

async fn handle_callback(cx: UpdateWithCx<AutoSend<Bot>, CallbackQuery>) {
    let query = &cx.update;
    log::info!("Callback: {:?}", query);
    let query_id = &query.id;
    let user_id = query.from.id;
    let message_id = query.message.as_ref().unwrap().id;
    let data = match &query.data {
        None => String::from("Error No data"),
        Some(d) => d.to_string(),
    };

    let parts: Vec<&str> = data.split(":").collect();
    let coords: Vec<&str> = parts[0].split("_").collect();
    let vote: u16 = parts[1].parse().unwrap();
    let locals: Vec<HLocal> = DB.find_local_by_coords(coords[0].parse::<f64>().unwrap(), coords[1].parse::<f64>().unwrap());
    let yay = if vote == 1 { 1 } else { 0 };
    let nay = if vote == 0 { 1 } else { 0 };
    let local = HLocal::new(&locals[0], yay, nay);
    let success = DB.insert_local(local);

    match cx
        .requester
        .answer_callback_query(query_id)
        .text(format!("Voto contabilizado: {}", success))
        .send()
        .await
    {
        Err(e) => log::error!("Error handle_message {}\n{}", &data, e),
        _ => log::info!("{}", data),
    }

    match cx.requester
        .edit_message_text(user_id, message_id, format!("Voto contabilizado!"))
        .send()
        .await {
            Err(e) => log::error!("Error edit_message {}", e),
            _ => log::info!("Great!")
        }
}

type Cx = UpdateWithCx<AutoSend<Bot>, Message>;

async fn ban_user(cx: &Cx, chat_id: i64, user: &DBUser) -> Result<True, RequestError> {
    cx.requester
        .ban_chat_member(chat_id, user.user_id)
        .until_date(0)
        .await
}
