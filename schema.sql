create table media(
    chat_id sqlite3_int64,
    msg_id sqlite3_int32,
    file_type varchar(9) not null,
    unique_id varchar(16) not null,
    file_id varchar(90) null,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (chat_id, unique_id)
);

create table urls(
    chat_id sqlite3_int64,
    unique_id varchar(250) not null,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (chat_id, unique_id)
);

create table users(
    user_id sqlite3_int64,
    chat_id sqlite3_int64,
    user_name varchar(250),
    chat_name varchar(250),
    PRIMARY KEY (user_id, chat_id)
);

create table duplicates(
    chat_id sqlite3_int64,
    unique_id varchar(16) not null,
    file_type varchar(9) not null,
    file_id varchar(90) null,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (chat_id, unique_id)
);

select * from media where timestamp <= date('now', '-4 day');
select * from urls where timestamp <= date('now', '-4 day');

SELECT * FROM media WHERE chat_id = -1001592783264 GROUP BY msg_id ORDER BY timestamp DESC limit 5;
SELECT * FROM urls WHERE chat_id = -1001592783264 ORDER BY timestamp DESC limit 5;