use std::env;
use std::sync::Arc;

use rtdlib::types::UpdateDeleteMessages;
use teloxide::types::{Chat, ChatKind, User};

use bincode;
use chrono::offset::Utc;
use chrono::Duration;
use rocksdb::{ColumnFamilyDescriptor, CompactionDecision, Options, DB, SliceTransform};

use super::models::*;
use super::repository::*;

#[derive(Clone)]
pub struct RocksDBRepo {
    db: Arc<DB>
}

/*impl Repository for RocksDBRepo {
    let path = "/home/enrs/src/rust/highlander/.rocksdb_prefix";
    let four_days_secs = 345600;

    let mut cfopts = Options::default();
    cfopts.set_compaction_filter("ttl_cf", ttl_cf_filter);
    let media_descriptor = ColumnFamilyDescriptor::new("media", cfopts);
    let users_descriptor = ColumnFamilyDescriptor::new("users", cfopts);
    let mappings_descriptor = ColumnFamilyDescriptor::new("mappings", cfopts);

    #[allow(unused_variables)]
    fn ttl_cf_filter(level: u32, key: &[u8], value: &[u8]) -> CompactionDecision {
        use self::CompactionDecision::*;
        let media: Media = bincode::deserialize(value).unwrap();
        let now = Utc::now().timestamp();
        if now - media.timestamp > four_days_secs {
            Remove
        } else {
            Keep
        }
    }

    fn key(k: &[u8]) -> Box<[u8]> {
        k.to_vec().into_boxed_slice()
    }

    fn init() -> Self {
        let prefix_extractor = SliceTransform::create_fixed_prefix(14); // length of chat_id
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_prefix_extractor(prefix_extractor);

        let cfs = vec![media_descriptor, users_descriptor, mappings_descriptor];
        let db_result = DB::open_cf_descriptors(&opts, &path, cfs);

        match db_result {
            Error(e) => panic!("{}", e),
            Ok(db) => RocksDBRepo { db: Arc::new(db) }
        }
    }

    fn chat_user_exists(&self, user: &User, chat: Arc<Chat>) -> bool {
        let users_handle = self.db.cf_handle("users").unwrap();
        let users_it = self.db.prefix_iterator_cf(users_handle, chat.chat_id);
        users_it.find(|&&i| i.user_id == user.user_id)
    }
}*/
