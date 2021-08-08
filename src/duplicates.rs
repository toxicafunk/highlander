use sqlite::Connection;

use super::models::*;

pub fn handle_message(connection: &Connection, acc: Status, sdo: SDO, table: &str) -> Status {
    let select = format!("SELECT unique_id FROM {} WHERE chat_id = ? AND unique_id = ?", table);
    let mut select_stmt = ok!(connection.prepare(select));
    ok!(select_stmt.bind(1, sdo.chat_id));
    ok!(select_stmt.bind(2, sdo.unique_id.as_str()));
    let mut select_cursor = select_stmt.cursor();
    let row = ok!(select_cursor.next());

    let is_media = table == "media";
    let insert = if is_media {
        format!("INSERT INTO {} (chat_id, msg_id, file_type, unique_id, file_id) VALUES (?, ?, ?, ?, ?)", table)
    } else {
        format!("INSERT INTO {} (chat_id, unique_id) VALUES (?, ?)", table)
    };

    log::info!("table: {}, SDO: {:?}", table, sdo);
    match row {
        None => {
            let mut insert_stmt = ok!(connection.prepare(insert));
            if is_media {
                ok!(insert_stmt.bind(1, sdo.chat_id));
                ok!(insert_stmt.bind(2, f64::from(sdo.msg_id)));
                ok!(insert_stmt.bind(3, sdo.file_type.as_str()));
                ok!(insert_stmt.bind(4, sdo.unique_id.as_str()));
                ok!(insert_stmt.bind(5, ok!(sdo.file_id).as_str()));
            } else {
                ok!(insert_stmt.bind(1, sdo.chat_id));
                ok!(insert_stmt.bind(2, sdo.unique_id.as_str()));
            };
            let mut cursor = insert_stmt.cursor();
            ok!(cursor.next());
            log::info!("Stored {} - {} - {} - {}", sdo.chat_id, sdo.msg_id, sdo.unique_id, acc.text);
            acc
        },
        Some(_) => {
            log::info!("Duplicate: {} - {} - {}", sdo.chat_id, sdo.unique_id, acc.text);
            Status { action: true, respond: true, text: "Mensaje Duplicado: El archivo o url ya se ha compartido en los ultimos 5 dias.".to_string() }
        }
    }
}

#[cfg(test)]
mod tests {
    use regex::Regex;
    use crate::extract_last250;

    #[test]
    fn url_regex() {
        let t1 = "hola https://twitter.com/plaforscience/status/1379526168513277960";
        let t2 = "hola https://twitter.com/plaforscience/status/1379526168513277960 y ademas https://youtu.be/GCI0NMgVfPk";
        let t3 = "https://drive.google.com/file/d/1t3_HeKZDIMEJl5_Y_l7uuIt4IeebCN7e/view?usp=sharing";

        let re: Regex = ok!(Regex::new("(http|ftp|https)://([\\w_-]+(?:(?:\\.[\\w_-]+)+))([\\w.,@?^=%&:/~+#-]*[\\w@?^=%&/~+#-])?"));

        let caps = re.captures_iter(t1);
        //println!("Found: {}", caps.count());
        for i in caps {
            let url = &i[0];
            println!("{}", url);
            println!("{}", extract_last250(url))
        }

        for i in re.captures_iter(t2) {
            let url = &i[0];
            println!("{}", url);
            println!("{}", extract_last250(url))
        }

        for i in re.captures_iter(t3) {
            let url = &i[0];
            println!("{}", url);
            println!("{}", extract_last250(url))
        }

        assert_eq!(2 + 2, 4);
    }
}