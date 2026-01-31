use std::collections::HashMap;
use rusqlite::fallible_streaming_iterator::FallibleStreamingIterator;
use rusqlite::{params, Row, Error as RusqliteError};
use serde::Serialize;
use tauri::ipc::Channel;
use crate::backend::db;
use crate::util::error;






/// Creates a new table.
pub fn create(name: String) -> Result<i64, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;
    trans.execute("INSERT INTO METADATA_TABLE_COLUMN_TYPE (MODE) VALUES (3);", [])?;
    let table_oid: i64 = trans.last_insert_rowid();
    trans.execute(
        "INSERT INTO METADATA_TABLE (OID, NAME) VALUES (?1, ?2);",
        params![table_oid, &name]
    )?;
    let create_table_cmd: String = format!("
    CREATE TABLE TABLE{table_oid} (
        OID INTEGER PRIMARY KEY, 
        TRASH BOOLEAN NOT NULL DEFAULT 0
    ) STRICT;");
    trans.execute(&create_table_cmd, [])?;
    let create_view_cmd = format!("CREATE VIEW TABLE{table_oid}_SURROGATE (OID, DISPLAY_VALUE) AS SELECT OID, CASE WHEN TRASH = 0 THEN '— NO PRIMARY KEY —' ELSE '— DELETED —' END AS DISPLAY_VALUE FROM TABLE{table_oid};");
    trans.execute(&create_view_cmd, [])?;
    return Ok(table_oid);
}

/// Flags a table as trash.
pub fn move_trash(table_oid: i64) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Flag the table as trash
    trans.execute("UPDATE METADATA_TABLE SET TRASH = 1 WHERE OID = ?1;", params![table_oid])?;

    // Commit and return
    trans.commit()?;
    return Ok(());
}

/// Unflags a table as trash.
pub fn unmove_trash(table_oid: i64) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Flag the table as trash
    trans.execute("UPDATE METADATA_TABLE SET TRASH = 0 WHERE OID = ?1;", params![table_oid])?;

    // Commit and return
    trans.commit()?;
    return Ok(());
}

/// Deletes the table with the given OID and all associated local columns.
/// Generally, this function should only be called after the table has been flagged as trash for reasonably long enough that the user could undo it if they wanted to.
pub fn delete(table_oid: i64) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Drop data from the table
    let drop_cmd: String = format!("DROP TABLE IF EXISTS TABLE{table_oid};");
    trans.execute(&drop_cmd, [])?;

    // Drop tables associated locally with the table
    // TODO

    // Finally, drop the table's metadata
    trans.execute("DELETE FROM METADATA_TABLE_COLUMN_TYPE WHERE OID = ?1;", params![table_oid])?;
    return Ok(());
}



#[derive(Serialize)]
/// The most bare-bones version of table metadata, used solely for populating the list of tables
pub struct BasicMetadata {
    oid: i64,
    name: String
}

/// Sends a list of tables through the provided channel.
pub fn send_metadata_list(table_channel: Channel<BasicMetadata>) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    db::query_iterate(&trans, 
        "SELECT 
            OID, 
            NAME 
        FROM METADATA_TABLE 
        WHERE TRASH = 0 
        ORDER BY NAME ASC;", [], 
        &mut |row| {
            table_channel.send(BasicMetadata {  
                oid: row.get::<_, i64>(0)?,
                name: row.get::<_, String>(1)?,
            })?;
            return Ok(());
        }
    )?;
    return Ok(());
}



#[derive(Serialize)]
struct Table {
    oid: i64,
    parent_table_oid: Option<i64>,
    name: String,
    //data: HashMap<i64, (TableColumn, Vec<Serialize>)>,
    surrogate_key_column_oid: Option<i64>
}