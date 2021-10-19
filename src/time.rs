use crate::models::*;


pub trait GetTime {
    fn get_timestamp(&self) -> i64;
}

impl GetTime for ColFam {
    fn get_timestamp(&self) -> i64 {
        match self {
            ColFam::MediaCF(media) => media.timestamp,
            ColFam::UserCF(user) => user.timestamp,
            ColFam::MappingCF(mapping) => mapping.timestamp
        }
    }
}
