use std::sync::Arc;

use rtdlib::types::UpdateDeleteMessages;
use teloxide::types::{Chat, User};

use super::models::{Group, Mapping, Media, SDO};
use super::models::{User as DBUser};

pub trait Repository<T> {
    fn init() -> Self;
    fn chat_dbuser_exists(&self, user_id: i64, chat_id: i64) -> bool;
    fn chat_user_exists(&self, user: &User, chat: Arc<Chat>) -> bool;
    fn update_user_timestamp(&self, user: &User, chat: Arc<Chat>) -> bool;
    fn insert_user(&self, user: &User, chat: Arc<Chat>) -> bool;
    fn item_exists(&self, sdo: SDO, is_media: bool) -> Option<T>;
    fn insert_item(&self, sdo: SDO, is_media: bool) -> bool;
    fn insert_duplicate(&self, sdo: SDO) -> bool;
    fn delete_item(&self, deleted_messages: UpdateDeleteMessages) -> ();
    fn insert_mapping(&self, api_id: i64, chat_id: i64, unique_id: &str) -> bool;
    fn find_mapping(&self, api_id: i64, chat_id: i64) -> Option<Mapping>;
    fn last_media_stored(&self, chat_id: i64, limit: usize, is_url: bool) -> Vec<Media>;
    fn last_media_duplicated(&self, chat_id: i64, limit: usize, is_url: bool) -> Vec<Media>;
    fn list_user_groups(&self, chat_id: i64, user_id: i64) -> Vec<DBUser>;
    fn get_chat_ids(&self) -> Vec<i64>;
    fn insert_dbuser(&self, user: DBUser) -> bool;
    fn list_media(&self, limit: usize) -> Vec<Media>;
    fn list_users(&self, limit: usize) -> Vec<DBUser>;
    fn list_duplicates(&self, limit: usize) -> Vec<Media>;
    fn get_users_chat_count(&self) -> Vec<(DBUser, usize)>;
    fn inactive_users_before(&self, ndays: i64) -> Vec<DBUser>;
    fn insert_group(&self, group: Group) -> bool;
    fn get_group(&self, supergroup_id: i64) -> Option<Group>;
}

#[cfg(test)]
mod tests {
    use crate::models::*;
    use crate::time::GetTime;
    use bincode;
    use chrono::offset::Utc;
    use chrono::Duration;
    use rocksdb::{ColumnFamilyDescriptor, CompactionDecision, Options, DB, SliceTransform};

    fn create_media(unique_id: &str, chat_id: i64, days_offset: i64) -> Media {
        let tmspt = Utc::now() - Duration::days(days_offset);
        let id = unique_id.get(15..).unwrap();
        println!("{:?}", id);
        Media {
                unique_id: id.to_string(),
                chat_id: chat_id,
                msg_id: 416,
                file_type: "photo".to_string(),
                file_id: "AgACAgUAAx0CXu_xoAACAaBhaViSMsDSnO2Txq5zNDGt3i1fTQACiq4xG--XSVcKeLs5IodbMgEAAwIAA3MAAyEE".to_string(),
                timestamp: tmspt.timestamp()
            }
    }

    #[test]
    fn test_db() {
        let path = "/home/enrs/src/rust/highlander/.rocksdb_def";
        let mut opts = Options::default();
        opts.create_if_missing(true);
        //{
        let db = DB::open(&opts, path).unwrap();
        db.put(b"key1", b"my value").unwrap();
        match db.get(b"key1") {
            Ok(Some(value)) => {
                let my_val = String::from_utf8(value).unwrap();
                println!("retrieved value {}", my_val);
                assert_eq!(my_val, "my value")
            }
            Ok(None) => println!("value not found"),
            Err(e) => println!("operational problem encountered: {}", e),
        }

        let unique_id = "-1001592783264_AQADiq4xG--XSVd4";
        let media = create_media(unique_id, -1001592783264, 0);
        let media_ser = bincode::serialize(&media).unwrap();
        db.put(unique_id, media_ser).unwrap();
        match db.get(unique_id) {
            Ok(Some(value)) => {
                let my_val: Media = bincode::deserialize(&value).unwrap();
                println!("Serialized value {:?}", my_val)
            }
            Ok(None) => println!("value not found"),
            Err(e) => println!("operational problem encountered: {}", e),
        }
        db.delete(b"my key").unwrap();
        match db.delete(unique_id) {
            Err(e) => println!("Test Delete: {}", e),
            Ok(_) => println!("Test Delete successful"),
        }
        //}
        let _ = DB::destroy(&Options::default(), path);
    }

    #[allow(unused_variables)]
    fn test_filter(level: u32, key: &[u8], value: &[u8]) -> CompactionDecision {
        use self::CompactionDecision::*;
        let media_cf: ColFam = bincode::deserialize(value).unwrap();
        let now = Utc::now().timestamp();
        let cur = media_cf.get_timestamp();
        if now - cur > 345600 {
            Remove
        } else {
            Keep
        }
    }

    #[test]
    fn test_compfilter() {
        let path = "/home/enrs/src/rust/highlander/.rocksdb_cf";
        //let cf_descriptor = ColumnFamilyDescriptor::new("cf1", Options::default());

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_compaction_filter("test", test_filter);

        //let cfs = vec![cf_descriptor];
        let db = DB::open(&opts, &path).unwrap();

        {
            let unique_id = "-1001592783264_AQADiq4xG--XSVd4";
            let unique_id1 = "-1001592783264_AQADiq4xG--XSQr3";
            let media = create_media(unique_id, -1001592783264, 5);
            let media1 = create_media(unique_id1, -1001592783264, 0);
            let cf1 = ColFam::MediaCF(media);
            let cf2 = ColFam::MediaCF(media1);
            let media_ser = bincode::serialize(&cf1).unwrap();
            let media_ser1 = bincode::serialize(&cf2).unwrap();
            println!("{} - {}", cf1.get_timestamp(), cf2.get_timestamp());
            db.put(unique_id, media_ser).unwrap();
            db.put(unique_id1, &media_ser1).unwrap();
            db.compact_range(None::<&[u8]>, None::<&[u8]>);
            assert_eq!(db.get(unique_id).unwrap(), None);
            let r = db.get(unique_id1).unwrap().unwrap();
            assert_eq!(r.len(), media_ser1.len());
        }
        let _ = DB::destroy(&Options::default(), path);
    }

    #[allow(unused_variables)]
    fn test_cf_filter(level: u32, key: &[u8], value: &[u8]) -> CompactionDecision {
        use self::CompactionDecision::*;
        let media: Media = bincode::deserialize(value).unwrap();
        let now = Utc::now().timestamp();
        let cur = media.timestamp;
        if now - cur > 345600 {
            Remove
        } else {
            Keep
        }
    }

    #[test]
    fn test_cf_compfilter() {
        let path = "/home/enrs/src/rust/highlander/.rocksdb_cf_cf";
        let mut cfopts = Options::default();
        cfopts.set_compaction_filter("test", test_cf_filter);
        let cf_descriptor = ColumnFamilyDescriptor::new("cf1", cfopts);

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cfs = vec![cf_descriptor];
        let db = DB::open_cf_descriptors(&opts, &path, cfs).unwrap();

        {
            let unique_id = "-1001592783264_AQADiq4xG--XSVd4";
            let unique_id1 = "-1001592783264_AQADiq4xG--XSQr3";
            let media = create_media(unique_id, -1001592783264, 5);
            let media1 = create_media(unique_id1, -1001592783264, 0);
            let media_ser = bincode::serialize(&media).unwrap();
            let media_ser1 = bincode::serialize(&media1).unwrap();

            let cf1 = db.cf_handle("cf1").unwrap();
            db.put_cf(&cf1, unique_id, media_ser).unwrap();
            db.put_cf(&cf1, unique_id1, &media_ser1).unwrap();
            db.compact_range_cf(&cf1, None::<&[u8]>, None::<&[u8]>);
            assert_eq!(db.get_cf(&cf1, unique_id).unwrap(), None);
            let r = db.get_cf(&cf1, unique_id1).unwrap().unwrap();
            assert_eq!(r.len(), media_ser1.len());
        }
        let _ = DB::destroy(&Options::default(), path);
    }

    fn key(k: &[u8]) -> Box<[u8]> {
        k.to_vec().into_boxed_slice()
    }

    #[test]
    fn test_prefix() {
        let path = "/home/enrs/src/rust/highlander/.rocksdb_prefix";

        let prefix_extractor = SliceTransform::create_fixed_prefix(14);
        let mut cfopts = Options::default();
        cfopts.set_compaction_filter("test", test_cf_filter);
        let cf_descriptor = ColumnFamilyDescriptor::new("cf1", cfopts);

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_prefix_extractor(prefix_extractor);

        let cfs = vec![cf_descriptor];
        let db = DB::open_cf_descriptors(&opts, &path, cfs).unwrap();

        {
            let unique_id1 = "-1001592783264_AQADiq4xG--XSVd4";
            let unique_id2 = "-1001592783264_AQADiq4xG--XSQr3";
            let unique_id3 = "-1001192585346_AQADiq4xG--XSAc9";
            let media1 = create_media(unique_id1, -1001592783264, 3);
            let media2 = create_media(unique_id2, -1001592783264, 0);
            let media3 = create_media(unique_id3, -1001192585346, 0);
            let media_ser1 = bincode::serialize(&media1).unwrap();
            let media_ser2 = bincode::serialize(&media2).unwrap();
            let media_ser3 = bincode::serialize(&media3).unwrap();

            let cf1 = db.cf_handle("cf1").unwrap();
            let key1: Box<[u8]> = key(unique_id1.as_bytes());
            let key2: Box<[u8]> = key(unique_id2.as_bytes());
            let key3: Box<[u8]> = key(unique_id3.as_bytes());
            db.put_cf(&cf1, key1, &media_ser1).unwrap();
            db.put_cf(&cf1, key2, &media_ser2).unwrap();
            db.put_cf(&cf1, key3, &media_ser3).unwrap();
            db.compact_range_cf(&cf1, None::<&[u8]>, None::<&[u8]>);

            let mut chat2_it = db.prefix_iterator_cf(cf1, b"-1001592783264");
            let r = chat2_it.find(|(k,_)| {
                let key = String::from_utf8(k.to_vec()).unwrap();
                key.get(15..).unwrap() == "AQADiq4xG--XSQr3"
            });
            let m: Media = bincode::deserialize(&r.unwrap().1).unwrap();
            println!("Find: {:?}", m);
            assert_eq!(m.msg_id, 416);
            //assert_eq!(chat2_it.collect::<Vec<_>>().len(), 2);
            let chat1_it = db.prefix_iterator_cf(cf1, b"-1001192585346");
            assert_eq!(chat1_it.collect::<Vec<_>>().len(), 3);
        }
        let _ = DB::destroy(&Options::default(), path);
    }

}
