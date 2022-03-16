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
use super::models::{Config, Group, Local, Mapping, Media, Vote, SDO};
use super::repository::*;

const FOUR_DAYS_SECS: i64 = 345600;
const DEFAULT_CONFIG: Config = Config {
    allow_forwards: true,
    block_non_latin: false,
    days_blocked: 5,
    allow_duplicate_media: false,
    allow_duplicate_links: false,
};

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
        file_id: sdo.file_id.unwrap_or_else(|| "".into()),
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
        let configs_opts = Options::default();
        let configs_descriptor = ColumnFamilyDescriptor::new("configs", configs_opts);
        let locals_opts = Options::default();
        let locals_descriptor = ColumnFamilyDescriptor::new("locals", locals_opts);
        let votes_opts = Options::default();
        let votes_descriptor = ColumnFamilyDescriptor::new("votes", votes_opts);

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_prefix_extractor(prefix_extractor);

        let cfs = vec![
            media_descriptor,
            users_descriptor,
            mappings_descriptor,
            duplicates_descriptor,
            groups_descriptor,
            configs_descriptor,
            locals_descriptor,
            votes_descriptor,
        ];

        match DB::open_cf_descriptors(&opts, &format!("{}/.rocksdb", db_path), cfs) {
            Err(e) => panic!("{}", e),
            Ok(db) => RocksDBRepo { db: Arc::new(db) },
        }
    }

    fn chat_user_exists(&self, user: &User, chat: Arc<Chat>) -> bool {
        match self.get_group(chat.id.to_string()) {
            Some(_) => (),
            None => match chat.kind.clone() {
                ChatKind::Public(chat_public) => {
                    let name = chat_public
                        .title
                        .unwrap_or_else(|| chat.title().unwrap().to_string());
                    let group = Group {
                        name: name.clone(),
                        supergroup_id: 0_i64,
                        chat_id: chat.id,
                        offset: 0,
                        timestamp: Utc::now().timestamp(),
                    };
                    log::info!("Inserted group: {}, {}", name, self.insert_group(group));
                }
                ChatKind::Private(_) => (),
            },
        }
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

    fn update_config(&self, config: Config, chat: i64) -> bool {
        let configs_handle = self.db.cf_handle("configs").unwrap();
        let k = format!("{}", chat);
        log::info!("Update config key: {}", k);
        match bincode::serialize(&config) {
            Err(e) => {
                log::error!("update_config: {}", e);
                false
            }
            Ok(v) => match self.db.put_cf(configs_handle, key(k.as_bytes()), v) {
                Err(e) => {
                    log::error!("update_config: {}", e);
                    false
                }
                Ok(_) => true,
            },
        }
    }

    fn get_config(&self, chat: i64) -> Config {
        let configs_handle = self.db.cf_handle("configs").unwrap();
        let chat_id = chat.to_string();
        match self.db.get_cf(configs_handle, chat_id.as_bytes()) {
            Ok(Some(config_ser)) => match bincode::deserialize(&config_ser) {
                Err(e) => {
                    log::error!("Config for {} failed to deserialize: {}", chat_id, e);
                    DEFAULT_CONFIG
                }
                Ok(config) => config,
            },
            Ok(None) => {
                log::warn!(
                    "Config for {} not found, using default config {:?}",
                    chat_id,
                    DEFAULT_CONFIG
                );
                DEFAULT_CONFIG
            }
            Err(e) => {
                log::error!("Config for {} could not be retrieved: {}", chat_id, e);
                DEFAULT_CONFIG
            }
        }
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

    fn delete_item(&self, deleted_messages: UpdateDeleteMessages) {
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

    fn list_user_groups_unpacked(&self, users: Vec<DBUser>) -> Vec<(i64, i64)> {
        let chat_ids = self.list_groups(0);
        let mut keys = Vec::new();
        for c in chat_ids {
            for u in &users {
                keys.push(format!("{}_{}", c.chat_id, u.user_id))
            }
        }

        self.list_users(0).iter()
            .filter(|dbuser| keys.contains(&format!("{}_{}", dbuser.chat_id, dbuser.user_id)))
            .map(|dbuser| (dbuser.chat_id, dbuser.user_id))
            .collect::<Vec<_>>()
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

    fn insert_group(&self, group: Group) -> bool {
        let groups_handle = self.db.cf_handle("groups").unwrap();
        match bincode::serialize(&group) {
            Err(e) => {
                log::error!("insert_group: {}", e);
                false
            }
            Ok(group_ser) => {
                let k = group.chat_id.to_string();
                match self.db.put_cf(groups_handle, key(k.as_bytes()), group_ser) {
                    Err(e) => {
                        log::error!("insert_group: {}", e);
                        false
                    }
                    Ok(_) => {
                        log::info!("insert_group: {:?}", group);
                        true
                    }
                }
            }
        }
    }

    fn get_group(&self, id: String) -> Option<Group> {
        let groups_handle = self.db.cf_handle("groups").unwrap();
        match self.db.get_cf(groups_handle, id.clone()) {
            Ok(Some(g_ser)) => match bincode::deserialize::<Group>(&g_ser) {
                Ok(group) => Some(group),
                Err(e) => {
                    log::error!("Error deserializing group {}\n{}", id, e);
                    None
                }
            },
            Ok(_) => {
                log::error!("Retrieved empty group {}", id);
                None
            }
            Err(e) => {
                log::error!("Error retrieving group {}\n{}", id, e);
                None
            }
        }
    }

    fn list_groups(&self, limit: usize) -> Vec<Group> {
        let groups_handle = self.db.cf_handle("groups").unwrap();
        let groups_it = self.db.iterator_cf(groups_handle, IteratorMode::Start);
        let mut groups_vec = groups_it
            .map(|(_, v_ser)| {
                let group: Group = bincode::deserialize(&v_ser).unwrap();
                group
            })
            .collect::<Vec<_>>();
        if limit > 0 {
            groups_vec.truncate(limit);
        }
        groups_vec
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
            .filter(|tup| tup.1 >= num_groups && tup.2)
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

    fn insert_vote(&self, vote: Vote) -> bool {
        let votes_handle = self.db.cf_handle("votes").unwrap();
        match bincode::serialize(&vote) {
            Err(e) => {
                log::error!("insert_vote: {}", e);
                false
            }
            Ok(vote_ser) => {
                let k = format!("{}_{}", vote.local_id, vote.user_id);
                match self.db.put_cf(votes_handle, key(k.as_bytes()), vote_ser) {
                    Err(e) => {
                        log::error!("insert_vote: {}", e);
                        false
                    }
                    Ok(_) => {
                        log::info!("insert_vote: {}", k);
                        true
                    }
                }
            }
        }
    }

    fn get_vote(&self, vote_id: String) -> Option<Vote> {
        let votes_handle = self.db.cf_handle("votes").unwrap();
        match self.db.get_cf(votes_handle, vote_id.clone()) {
            Ok(Some(v_ser)) => match bincode::deserialize::<Vote>(&v_ser) {
                Ok(vote) => Some(vote),
                Err(e) => {
                    log::error!("Error deserializing vote {}\n{}", vote_id, e);
                    None
                }
            },
            Ok(_) => {
                log::error!("Retrieved empty vote {}", vote_id);
                None
            }
            Err(e) => {
                log::error!("Error retrieving vote {}\n{}", vote_id, e);
                None
            }
        }
    }

    fn delete_vote(&self, vote_id: String) -> bool {
        let votes_handle = self.db.cf_handle("votes").unwrap();
        match self.db.delete_cf(votes_handle, vote_id.as_bytes()) {
            Ok(_) => true,
            Err(e) => {
                log::error!("Error deleting vote {}\n{}", vote_id, e);
                false
            }
        }
    }

    fn delete_local_votes(&self, local_id: String) -> bool {
        let votes_handle = self.db.cf_handle("votes").unwrap();
        let votes_it = self.db.iterator_cf(votes_handle, IteratorMode::Start);
        votes_it
            .filter(|(k_ser, _)| {
                let key = String::from_utf8(k_ser.to_vec()).unwrap();
                let ids: Vec<&str> = key.split("_").collect();
                log::info!("votes by local id: {} | {}", ids[0], ids[1]);
                ids[0] == local_id.as_str()
            })
            .fold(true, |acc, tup| {
                let key = String::from_utf8(tup.0.to_vec()).unwrap();
                acc && self.delete_vote(key)
            })
    }

    fn find_votes_by_localid(&self, local_id: String) -> Vote {
        let votes_handle = self.db.cf_handle("votes").unwrap();
        let votes_it = self.db.iterator_cf(votes_handle, IteratorMode::Start);
        let votes_agg = votes_it
            .filter(|(k_ser, _)| {
                let key = String::from_utf8(k_ser.to_vec()).unwrap();
                let ids: Vec<&str> = key.split("_").collect();
                log::info!("votes by local id: {} | {}", ids[0], ids[1]);
                ids[0] == local_id.as_str()
            })
            .map(|(_, v_ser)| {
                let vote: Vote = bincode::deserialize(&v_ser).unwrap();
                (vote.pass, vote.nopass, vote.awake)
            })
            .fold((0_u16, 0_u16, 0_u16), |acc, c| {
                (acc.0 + c.0, acc.1 + c.1, acc.2 + c.2)
            });
        Vote {
            local_id,
            user_id: 0,
            pass: votes_agg.0,
            nopass: votes_agg.1,
            awake: votes_agg.2,
        }
    }

    fn get_local(&self, local_id: String) -> Option<(Local, Vote)> {
        let locals_handle = self.db.cf_handle("locals").unwrap();
        match self.db.get_cf(&locals_handle, local_id.clone()) {
            Ok(Some(l_ser)) => match bincode::deserialize::<Local>(&l_ser) {
                Ok(local) => {
                    let vote = self.find_votes_by_localid(local_id);
                    Some((local, vote))
                }
                Err(e) => {
                    log::error!("Error deserializing local {}\n{}", local_id, e);
                    None
                }
            },
            Ok(_) => {
                log::error!("Retrieved empty local {}", local_id);
                None
            }
            Err(e) => {
                log::error!("Error retrieving local {}\n{}", local_id, e);
                None
            }
        }
    }

    fn insert_local(&self, local: Local) -> bool {
        let locals_handle = self.db.cf_handle("locals").unwrap();
        match bincode::serialize(&local) {
            Err(e) => {
                log::error!("insert_local: {}", e);
                false
            }
            Ok(local_ser) => {
                let k = local.id;
                match self.db.put_cf(locals_handle, key(k.as_bytes()), local_ser) {
                    Err(e) => {
                        log::error!("insert_local: {}", e);
                        false
                    }
                    Ok(_) => {
                        log::info!("insert_local: {}", k);
                        true
                    }
                }
            }
        }
    }

    fn delete_local(&self, local_id: String) -> bool {
        let locals_handle = self.db.cf_handle("locals").unwrap();
        if self.delete_local_votes(local_id.clone()) {
            match self.db.delete_cf(locals_handle, &local_id.as_bytes()) {
                Ok(_) => true,
                Err(e) => {
                    log::error!("Error deleting vote {}\n{}", local_id, e);
                    false
                }
            }
        } else {
            log::error!("Failed to delete votes for local {}", local_id);
            false
        }
    }
    fn find_local_by_coords(&self, latitude: f64, longitude: f64) -> Vec<(Local, Vote)> {
        self.find_nearby_by_coords(latitude, longitude, 1_f64)
    }

    fn find_nearby_by_coords(
        &self,
        latitude: f64,
        longitude: f64,
        offset: f64,
    ) -> Vec<(Local, Vote)> {
        let locals_handle = self.db.cf_handle("locals").unwrap();
        self.db
            .iterator_cf(locals_handle, IteratorMode::Start)
            .map(|(_, v_ser)| bincode::deserialize::<Local>(&v_ser).unwrap())
            .filter(|local| {
                is_within_meters(local.latitude, local.longitude, latitude, longitude, offset)
            })
            .map(|local| {
                let vote = self.find_votes_by_localid(local.id.clone());
                (local, vote)
            })
            .collect::<Vec<_>>()
    }

    fn find_local_by_name(&self, name: String) -> Vec<(Local, Vote)> {
        let locals_handle = self.db.cf_handle("locals").unwrap();
        self.db
            .iterator_cf(locals_handle, IteratorMode::Start)
            .map(|(_, v_ser)| bincode::deserialize::<Local>(&v_ser).unwrap())
            .filter(|local| local.name.to_lowercase().contains(&name.to_lowercase()))
            .map(|local| {
                let vote = self.find_votes_by_localid(local.id.clone());
                (local, vote)
            })
            .collect::<Vec<_>>()
    }

    fn find_local_by_address(&self, address: String) -> Vec<(Local, Vote)> {
        let locals_handle = self.db.cf_handle("locals").unwrap();
        self.db
            .iterator_cf(locals_handle, IteratorMode::Start)
            .map(|(_, v_ser)| bincode::deserialize::<Local>(&v_ser).unwrap())
            .filter(|local| {
                local
                    .address
                    .to_lowercase()
                    .contains(&address.to_lowercase())
            })
            .map(|local| {
                let vote = self.find_votes_by_localid(local.id.clone());
                (local, vote)
            })
            .collect::<Vec<_>>()
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
