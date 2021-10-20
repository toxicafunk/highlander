use std::env;
use std::sync::Arc;

use rtdlib::types::UpdateDeleteMessages;
use teloxide::types::{Chat, ChatKind, User};

use bincode;
use chrono::offset::Utc;
use rocksdb::{ColumnFamilyDescriptor, CompactionDecision, Options, SliceTransform, DB};

use super::models::User as DBUser;
use super::models::{Mapping, Media, SDO};
use super::repository::*;

const FOUR_DAYS_SECS: i64 = 345600;

#[allow(unused_variables)]
fn ttl_cf_filter(level: u32, key: &[u8], value: &[u8]) -> CompactionDecision {
    use self::CompactionDecision::*;
    let media: Media = bincode::deserialize(value).unwrap();
    let now = Utc::now().timestamp();
    if now - media.timestamp > FOUR_DAYS_SECS {
        Remove
    } else {
        Keep
    }
}

fn key(k: &[u8]) -> Box<[u8]> {
    k.to_vec().into_boxed_slice()
}

fn user_to_db(user: &User, chat: Arc<Chat>) -> DBUser {
    let unknown = String::from("Unknown");
    let chat_name = match &chat.kind {
        ChatKind::Public(public) => public.title.as_ref().unwrap_or(&unknown) as &str,
        ChatKind::Private(_) => unknown.as_str(),
    };

    let user_name = user.username.as_ref().unwrap_or(&user.first_name);
    DBUser {
        user_id: user.id,
        chat_id: chat.id,
        user_name: user_name.to_string(),
        chat_name: chat_name.into(),
        timestamp: Utc::now().timestamp(),
    }
}

fn sdo_to_media(sdo: SDO) -> Media {
    Media {
        unique_id: sdo.unique_id,
        chat_id: sdo.chat.id,
        msg_id: sdo.msg_id,
        file_type: sdo.file_type,
        file_id: sdo.file_id.unwrap_or("".into()),
        timestamp: Utc::now().timestamp(),
    }
}

#[derive(Clone)]
pub struct RocksDBRepo {
    db: Arc<DB>,
}

impl Repository<Media> for RocksDBRepo {
    fn init() -> Self {
        let db_path = match env::var("HIGHLANDER_DB_PATH") {
            Ok(path) => path,
            Err(_) => String::from("."),
        };

        let prefix_extractor = SliceTransform::create_fixed_prefix(14); // length of chat_id

        let mut cfopts = Options::default();
        cfopts.set_compaction_filter("ttl_cf", ttl_cf_filter);
        let media_descriptor = ColumnFamilyDescriptor::new("media", cfopts);
        cfopts = Options::default();
        cfopts.set_compaction_filter("ttl_cf", ttl_cf_filter);
        let users_descriptor = ColumnFamilyDescriptor::new("users", cfopts);
        cfopts = Options::default();
        cfopts.set_compaction_filter("ttl_cf", ttl_cf_filter);
        let mappings_descriptor = ColumnFamilyDescriptor::new("mappings", cfopts);
        cfopts = Options::default();
        cfopts.set_compaction_filter("ttl_cf", ttl_cf_filter);
        let duplicates_descriptor = ColumnFamilyDescriptor::new("duplicates", cfopts);

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_prefix_extractor(prefix_extractor);

        let cfs = vec![
            media_descriptor,
            users_descriptor,
            mappings_descriptor,
            duplicates_descriptor,
        ];

        match DB::open_cf_descriptors(&opts, &format!("{}/.rocksdb", db_path), cfs) {
            Err(e) => panic!("{}", e),
            Ok(db) => RocksDBRepo { db: Arc::new(db) },
        }
    }

    fn chat_user_exists(&self, user: &User, chat: Arc<Chat>) -> bool {
        let users_handle = self.db.cf_handle("users").unwrap();
        let chat_id = bincode::serialize(&chat.id).unwrap();
        let mut users_it = self.db.prefix_iterator_cf(users_handle, chat_id);
        match users_it.find(|(k, _)| {
            let key = String::from_utf8(k.to_vec()).unwrap();
            match key.get(15..) {
                None => false,
                Some(id) => match id.parse::<i64>() {
                    Ok(i) => i == user.id,
                    Err(_) => false,
                },
            }
        }) {
            None => false,
            Some(_) => true,
        }
    }

    fn update_user_timestamp(&self, user: &User, chat: Arc<Chat>) -> bool {
        let users_handle = self.db.cf_handle("users").unwrap();
        let dbuser = user_to_db(user, chat.clone());
        let k = format!("{}_{}", chat.id, user.id);
        log::info!("Update key: {}", k);
        match bincode::serialize(&dbuser) {
            Err(e) => {
                log::error!("update_user_timestamp: {}", e);
                false
            }
            Ok(v) => match self.db.put_cf(users_handle, key(k.as_bytes()), v) {
                Err(e) => {
                    log::error!("update_user_timestamp: {}", e);
                    false
                }
                Ok(_) => true,
            },
        }
    }

    fn insert_user(&self, user: &User, chat: Arc<Chat>) -> bool {
        self.update_user_timestamp(user, chat)
    }

    #[allow(unused_variables)]
    fn item_exists(&self, sdo: SDO, is_media: bool) -> Option<Media> {
        let media_handle = self.db.cf_handle("media").unwrap();
        let chat_id = bincode::serialize(&sdo.chat.id).unwrap();
        let mut media_it = self.db.prefix_iterator_cf(media_handle, chat_id);
        match media_it.find(|(k, _)| {
            let key = String::from_utf8(k.to_vec()).unwrap();
            match key.get(15..) {
                None => false,
                Some(id) => id == sdo.unique_id,
            }
        }) {
            None => None,
            Some(media_ser) => {
                let media: Media = bincode::deserialize(&media_ser.1).unwrap();
                Some(media)
            }
        }
    }

    fn insert_item(&self, sdo: SDO, _is_media: bool) -> bool {
        let media_handle = self.db.cf_handle("media").unwrap();
        let chat_id = sdo.chat.id;
        let media = sdo_to_media(sdo);
        match bincode::serialize(&media) {
            Err(e) => {
                log::error!("insert_item: {}", e);
                false
            }
            Ok(media_ser) => {
                let k = format!("{}_{}", chat_id, media.unique_id);
                match self.db.put_cf(media_handle, key(k.as_bytes()), media_ser) {
                    Err(e) => {
                        log::error!("insert_item: {}", e);
                        false
                    }
                    Ok(_) => true,
                }
            }
        }
    }

    fn insert_duplicate(&self, sdo: SDO) -> bool {
        let duplicates_handle = self.db.cf_handle("duplicates").unwrap();
        let chat_id = sdo.chat.id;
        let media = sdo_to_media(sdo);
        match bincode::serialize(&media) {
            Err(e) => {
                log::error!("insert_duplicate: {}", e);
                false
            }
            Ok(media_ser) => {
                let k = format!("{}_{}", chat_id, media.unique_id);
                match self
                    .db
                    .put_cf(duplicates_handle, key(k.as_bytes()), media_ser)
                {
                    Err(e) => {
                        log::error!("insert_duplicate: {}", e);
                        false
                    }
                    Ok(_) => true,
                }
            }
        }
    }

    fn delete_item(&self, deleted_messages: UpdateDeleteMessages) -> () {
        let media_handle = self.db.cf_handle("media").unwrap();
        let chat_id = deleted_messages.chat_id();
        for msg_id in deleted_messages.message_ids() {
            let k = format!("{}_{}", chat_id, msg_id);
            match self.db.delete_cf(media_handle, k) {
                Err(e) => log::error!("delete_item: {}", e),
                Ok(_) => (),
            }
        }
    }

    fn insert_mapping(&self, id: i64, chat_id: i64, unique_id: &str) -> bool {
        let mappings_handle = self.db.cf_handle("mappings").unwrap();
        let mapping = Mapping {
            unique_id: unique_id.into(),
            chat_id,
            api_id: id,
            timestamp: Utc::now().timestamp(),
        };
        match bincode::serialize(&mapping) {
            Err(e) => {
                log::error!("insert_mapping: {}", e);
                false
            }
            Ok(mapping_ser) => {
                let k = format!("{}_{}", chat_id, unique_id);
                match self
                    .db
                    .put_cf(mappings_handle, key(k.as_bytes()), mapping_ser)
                {
                    Err(e) => {
                        log::error!("insert_mapping: {}", e);
                        false
                    }
                    Ok(_) => true,
                }
            }
        }
    }
}
