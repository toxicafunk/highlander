use std::env;
use std::sync::Arc;

use rtdlib::types::UpdateDeleteMessages;
use teloxide::types::{Chat, ChatKind, User};

use bincode;
use chrono::offset::Utc;
use chrono::Duration;

use rocksdb::{
    ColumnFamilyDescriptor, CompactionDecision, IteratorMode, Options, SliceTransform, DB,
};

use itertools::Itertools;

use super::models::User as DBUser;
use super::models::{Mapping, Media, SDO};
use super::repository::*;

const FOUR_DAYS_SECS: i64 = 345600;

#[allow(unused_variables)]
fn media_ttl_filter(level: u32, key: &[u8], value: &[u8]) -> CompactionDecision {
    use self::CompactionDecision::*;
    let media: Media = bincode::deserialize(value).unwrap();
    let now = Utc::now().timestamp();
    if now - media.timestamp > FOUR_DAYS_SECS {
        Remove
    } else {
        Keep
    }
}

#[allow(unused_variables)]
fn users_ttl_filter(level: u32, key: &[u8], value: &[u8]) -> CompactionDecision {
    use self::CompactionDecision::*;
    let user: DBUser = bincode::deserialize(value).unwrap();
    let now = Utc::now().timestamp();
    if now - user.timestamp > FOUR_DAYS_SECS {
        Remove
    } else {
        Keep
    }
}

#[allow(unused_variables)]
fn mappings_ttl_filter(level: u32, key: &[u8], value: &[u8]) -> CompactionDecision {
    use self::CompactionDecision::*;
    let mapping: Mapping = bincode::deserialize(value).unwrap();
    let now = Utc::now().timestamp();
    if now - mapping.timestamp > FOUR_DAYS_SECS {
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

        let mut media_opts = Options::default();
        media_opts.set_compaction_filter("ttl_media", media_ttl_filter);
        let media_descriptor = ColumnFamilyDescriptor::new("media", media_opts);
        let mut user_opts = Options::default();
        user_opts.set_compaction_filter("ttl_user", users_ttl_filter);
        let users_descriptor = ColumnFamilyDescriptor::new("users", user_opts);
        let mut mappings_opts = Options::default();
        mappings_opts.set_compaction_filter("ttl_mappings", mappings_ttl_filter);
        let mappings_descriptor = ColumnFamilyDescriptor::new("mappings", mappings_opts);
        let mut duplicates_opts = Options::default();
        duplicates_opts.set_compaction_filter("ttl_duplicates", media_ttl_filter);
        let duplicates_descriptor = ColumnFamilyDescriptor::new("duplicates", duplicates_opts);
        let groups_opts = Options::default();
        let groups_descriptor = ColumnFamilyDescriptor::new("groups", groups_opts);


        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_prefix_extractor(prefix_extractor);

        let cfs = vec![
            media_descriptor,
            users_descriptor,
            mappings_descriptor,
            duplicates_descriptor,
            groups_descriptor
        ];

        match DB::open_cf_descriptors(&opts, &format!("{}/.rocksdb", db_path), cfs) {
            Err(e) => panic!("{}", e),
            Ok(db) => RocksDBRepo { db: Arc::new(db) },
        }
    }

    fn chat_user_exists(&self, user: &User, chat: Arc<Chat>) -> bool {
        self.chat_dbuser_exists(user.id, chat.id)
    }

    fn chat_dbuser_exists(&self, user_id: i64, chat_id: i64) -> bool {
        let users_handle = self.db.cf_handle("users").unwrap();
        let chat_id = chat_id.to_string();
        let mut users_it = self.db.prefix_iterator_cf(users_handle, chat_id.as_bytes());
        match users_it.find(|(k, _)| {
            let key = String::from_utf8(k.to_vec()).unwrap();
            let ids: Vec<&str> = key.split("_").collect();
            if ids.is_empty() {
                false
            } else {
                match ids[1].parse::<i64>() {
                    Ok(i) => i == user_id && ids[0] == chat_id,
                    Err(_) => false,
                }
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
        log::info!("Update user key: {}", k);
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
        log::info!("insert_user: {} on chat {}", user.id, chat.id);
        self.update_user_timestamp(user, chat)
    }

    #[allow(unused_variables)]
    fn item_exists(&self, sdo: SDO, is_media: bool) -> Option<Media> {
        let media_handle = self.db.cf_handle("media").unwrap();
        let chat_id = sdo.chat.id.to_string();
        let mut media_it = self.db.prefix_iterator_cf(media_handle, chat_id.as_bytes());
        match media_it.find(|(k, _)| {
            let key = String::from_utf8(k.to_vec()).unwrap();
            let prefix = key.get(..14);
            let id = key.get(15..);
            match (prefix, id) {
                (Some(p), Some(k)) => p == chat_id && k == sdo.unique_id,
                _ => false,
            }
        }) {
            None => {
                log::info!(
                    "item_exists: not found key {}_{}",
                    sdo.chat.id,
                    sdo.unique_id
                );
                None
            }
            Some(media_ser) => {
                let media: Media = bincode::deserialize(&media_ser.1).unwrap();
                log::info!("item_exists: found media {:?}", media);
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
                    Ok(_) => {
                        log::info!("insert_item: {}", k);
                        true
                    }
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
        for api_id in deleted_messages.message_ids() {
            match self.find_mapping(*api_id, chat_id) {
                None => {
                    log::error!("Mapping {}_{} not found", chat_id, api_id);
                }
                Some(mapping) => {
                    let unique_id = mapping.unique_id;
                    let k = format!("{}_{}", chat_id, unique_id);
                    match self.db.delete_cf(media_handle, key(k.as_bytes())) {
                        Err(e) => log::error!("delete_item: {}", e),
                        Ok(_) => log::info!("Deleted {}_{}", chat_id, unique_id),
                    }
                }
            }
        }
    }

    fn find_mapping(&self, api_id: i64, chat_id: i64) -> Option<Mapping> {
        let mappings_handle = self.db.cf_handle("mappings").unwrap();
        let chat_id = chat_id.to_string();
        let mut mappings_it = self
            .db
            .prefix_iterator_cf(mappings_handle, chat_id.as_bytes());
        match mappings_it.find(|(k, _)| {
            let key = String::from_utf8(k.to_vec()).unwrap();
            let ids: Vec<&str> = key.split("_").collect();
            if ids.is_empty() {
                false
            } else {
                ids[1].parse::<i64>().unwrap_or(0) == api_id && ids[0] == chat_id
            }
        }) {
            None => {
                log::info!("find_mapping: not found {}_{}", chat_id, api_id);
                None
            }
            Some(mapping_ser) => {
                let mapping: Mapping = bincode::deserialize(&mapping_ser.1).unwrap();
                log::info!("find_mapping: found {:?}", mapping);
                Some(mapping)
            }
        }
    }

    fn insert_mapping(&self, api_id: i64, chat_id: i64, unique_id: &str) -> bool {
        let mappings_handle = self.db.cf_handle("mappings").unwrap();
        let mapping = Mapping {
            unique_id: unique_id.into(),
            chat_id,
            api_id,
            timestamp: Utc::now().timestamp(),
        };
        match bincode::serialize(&mapping) {
            Err(e) => {
                log::error!("insert_mapping: {}", e);
                false
            }
            Ok(mapping_ser) => {
                let k = format!("{}_{}", chat_id, api_id);
                match self
                    .db
                    .put_cf(mappings_handle, key(k.as_bytes()), mapping_ser)
                {
                    Err(e) => {
                        log::error!("insert_mapping: {}", e);
                        false
                    }
                    Ok(_) => {
                        log::info!("insert_mapping: {:?}", mapping);
                        true
                    }
                }
            }
        }
    }

    fn last_media_stored(&self, chat_id: i64, limit: usize, is_url: bool) -> Vec<Media> {
        let media_handle = self.db.cf_handle("media").unwrap();
        let chat_id_str = chat_id.to_string();
        let media_it = self
            .db
            .prefix_iterator_cf(media_handle, chat_id_str.as_bytes());
        let mut media_vec = media_it
            .filter(|(k, _)| {
                let key = String::from_utf8(k.to_vec()).unwrap();
                match key.get(..14) {
                    Some(prefix) => prefix == chat_id_str,
                    None => false,
                }
            })
            .map(|(_, v_ser)| {
                let media: Media = bincode::deserialize(&v_ser).unwrap();
                (media.msg_id, media)
            })
            .filter(|tup| (tup.1.file_type == "url") == is_url)
            .into_group_map()
            .into_iter()
            .map(|(_, g)| g.first().unwrap().clone())
            .collect::<Vec<_>>();
        media_vec.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        media_vec.truncate(limit);
        media_vec
    }

    fn last_media_duplicated(&self, chat_id: i64, limit: usize, is_url: bool) -> Vec<Media> {
        let duplicates_handle = self.db.cf_handle("duplicates").unwrap();
        let chat_id_str = chat_id.to_string();
        let duplicates_it = self
            .db
            .prefix_iterator_cf(duplicates_handle, chat_id_str.as_bytes());
        let mut duplicates_vec = duplicates_it
            .filter(|(k, _)| {
                let key = String::from_utf8(k.to_vec()).unwrap();
                match key.get(..14) {
                    Some(prefix) => prefix == chat_id_str,
                    None => false,
                }
            })
            .map(|(_, v_ser)| {
                let duplicates: Media = bincode::deserialize(&v_ser).unwrap();
                (duplicates.msg_id, duplicates)
            })
            .filter(|tup| (tup.1.file_type == "url") == is_url)
            .into_group_map()
            .into_iter()
            .map(|(_, g)| g.first().unwrap().clone())
            .collect::<Vec<_>>();
        duplicates_vec.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        duplicates_vec.truncate(limit);
        duplicates_vec
    }

    fn list_user_groups(&self, chat_id: i64, user_id: i64) -> Vec<DBUser> {
        let users_handle = self.db.cf_handle("users").unwrap();
        let chat_id_str = chat_id.to_string();
        let users_it = self
            .db
            .prefix_iterator_cf(users_handle, chat_id_str.as_bytes());
        let users_vec = users_it
            .filter(|(k, _)| {
                let key = String::from_utf8(k.to_vec()).unwrap();
                match key.get(15..) {
                    None => false,
                    Some(id) => match id.parse::<i64>() {
                        Ok(i) => i == user_id,
                        Err(_) => false,
                    },
                }
            })
            .map(|(_, v_ser)| {
                let user: DBUser = bincode::deserialize(&v_ser).unwrap();
                user
            })
            .collect::<Vec<_>>();
        users_vec
    }

    fn get_chat_ids(&self) -> Vec<i64> {
        let users_handle = self.db.cf_handle("users").unwrap();
        let users_it = self.db.iterator_cf(users_handle, IteratorMode::Start);
        let users_vec = users_it
            .dedup_by(|(k1, _), (k2, _)| {
                let key1 = String::from_utf8(k1.to_vec()).unwrap();
                let key2 = String::from_utf8(k2.to_vec()).unwrap();
                match (key1.get(..14), key2.get(..14)) {
                    (Some(id1), Some(id2)) => id1 == id2,
                    _ => false,
                }
            })
            .map(|(k, _)| {
                let key = String::from_utf8(k.to_vec()).unwrap();
                match key.get(..14) {
                    None => 0,
                    Some(id) => match id.parse::<i64>() {
                        Ok(i) => i,
                        Err(_) => 0,
                    },
                }
            })
            .collect::<Vec<_>>();
        users_vec
    }

    fn insert_dbuser(&self, user: DBUser) -> bool {
        let users_handle = self.db.cf_handle("users").unwrap();
        let k = format!("{}_{}", user.chat_id, user.user_id);
        log::info!("Insert DBUser key: {}", k);
        match bincode::serialize(&user) {
            Err(e) => {
                log::error!("insert_dbuser: {}", e);
                false
            }
            Ok(v) => match self.db.put_cf(users_handle, key(k.as_bytes()), v) {
                Err(e) => {
                    log::error!("insert_dbuser: {}", e);
                    false
                }
                Ok(_) => true,
            },
        }
    }

    fn list_media(&self, limit: usize) -> Vec<Media> {
        let media_handle = self.db.cf_handle("media").unwrap();
        let media_it = self.db.iterator_cf(media_handle, IteratorMode::Start);
        let mut media_vec = media_it
            .map(|(_, v_ser)| {
                let media: Media = bincode::deserialize(&v_ser).unwrap();
                media
            })
            .collect::<Vec<_>>();
        if limit > 0 {
            media_vec.truncate(limit);
        }
        media_vec
    }

    fn list_users(&self, limit: usize) -> Vec<DBUser> {
        let users_handle = self.db.cf_handle("users").unwrap();
        let users_it = self.db.iterator_cf(users_handle, IteratorMode::Start);
        let mut users_vec = users_it
            .map(|(_, v_ser)| {
                let user: DBUser = bincode::deserialize(&v_ser).unwrap();
                user
            })
            .collect::<Vec<_>>();
        if limit > 0 {
            users_vec.truncate(limit);
        }
        users_vec
    }

    fn list_duplicates(&self, limit: usize) -> Vec<Media> {
        let media_handle = self.db.cf_handle("duplicates").unwrap();
        let media_it = self.db.iterator_cf(media_handle, IteratorMode::Start);
        let mut media_vec = media_it
            .map(|(_, v_ser)| {
                let media: Media = bincode::deserialize(&v_ser).unwrap();
                media
            })
            .collect::<Vec<_>>();
        if limit > 0 {
            media_vec.truncate(limit);
        }
        media_vec
    }

    fn get_users_chat_count(&self, chat_id: i64, num_groups: usize) -> Vec<(DBUser, usize)> {
        let users_handle = self.db.cf_handle("users").unwrap();
        let users_it = self.db.iterator_cf(users_handle, IteratorMode::Start);
        let users_vec = users_it
            .map(|(_, v_ser)| {
                let user: DBUser = bincode::deserialize(&v_ser).unwrap();
                (user.user_id, user)
            })
            .into_group_map()
            .into_iter()
            .map(|(_, g)| {
                let count = g.len();
                let user = g.first().unwrap().clone();
                let is_in_chat = g.iter().any(|user| user.chat_id == chat_id);
                (user, count, is_in_chat)
            })
            .filter(|tup| {
                tup.1 >= num_groups && tup.2
            })
            .map(|tup| (tup.0, tup.1))
            .collect::<Vec<_>>();
        users_vec
    }

    fn inactive_users_before(&self, ndays: i64) -> Vec<DBUser> {
        let users_handle = self.db.cf_handle("users").unwrap();
        let users_it = self.db.iterator_cf(users_handle, IteratorMode::Start);
        let users_vec = users_it
            .map(|(_, v_ser)| {
                let user: DBUser = bincode::deserialize(&v_ser).unwrap();
                user
            })
            .filter(|user| {
                let offset_day = Utc::now() - Duration::days(ndays);
                user.timestamp < offset_day.timestamp()
            })
            .collect::<Vec<_>>();
        users_vec
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_split_str() {
        let key = "-1001445478423_1072037897";
        let ids: Vec<&str> = key.split("_").collect();
        println!("{} | {}", ids[0], ids[1]);
        assert_eq!(ids[0], "-1001445478423");
        assert_eq!(ids[1], "1072037897");
    }

    #[test]
    fn test_bool() {
        assert_eq!((("url" == "url") == true), true);
        assert_eq!((("url" == "url") == false), false);
        assert_eq!((("photo" == "url") == true), false);
        assert_eq!((("video" == "url") == false), true);
    }
}
