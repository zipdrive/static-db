use std::sync::{Mutex};
use rusqlite::{params, Connection, Transaction, TransactionBehavior, DropBehavior, Result};

/// Initializes a new database at the given path.
fn initialize_new_db<P: AsRef<Path>>(path: P) -> Result<()> {
    let conn: Connection = Connection::open(path);
    conn.execute("PRAGMA foreign_keys = ON;");
    conn.execute("PRAGMA journal_mode = WAL;");
    conn.execute_batch("
    BEGIN;

    -- __METADATA_TYPE stores all pre-defined and user-defined data types
    CREATE TABLE __METADATA_TABLE_COLUMN_TYPE (
        ID INTEGER PRIMARY KEY,
        MODE INTEGER NOT NULL DEFAULT 0 
            -- Modes are:
            -- 0 = primitive
            -- 1 = adhoc single-select dropdown
            -- 2 = adhoc multi-select dropdown
            -- 3 = reference to independent table
            -- 4 = child object
            -- 5 = child table
    );
    INSERT INTO __METADATA_TABLE_COLUMN_TYPE (ID, MODE) VALUES (0, 0); -- Always null
    INSERT INTO __METADATA_TABLE_COLUMN_TYPE (ID, MODE) VALUES (1, 0); -- Boolean
    INSERT INTO __METADATA_TABLE_COLUMN_TYPE (ID, MODE) VALUES (2, 0); -- Integer
    INSERT INTO __METADATA_TABLE_COLUMN_TYPE (ID, MODE) VALUES (3, 0); -- Number
    INSERT INTO __METADATA_TABLE_COLUMN_TYPE (ID, MODE) VALUES (4, 0); -- Date
    INSERT INTO __METADATA_TABLE_COLUMN_TYPE (ID, MODE) VALUES (5, 0); -- Timestamp
    INSERT INTO __METADATA_TABLE_COLUMN_TYPE (ID, MODE) VALUES (6, 0); -- BLOB
    INSERT INTO __METADATA_TABLE_COLUMN_TYPE (ID, MODE) VALUES (7, 0); -- BLOB (displayed as image thumbnail)
    INSERT INTO __METADATA_TABLE_COLUMN_TYPE (ID, MODE) VALUES (8, 0); -- Text
    INSERT INTO __METADATA_TABLE_COLUMN_TYPE (ID, MODE) VALUES (9, 0); -- Text (JSON)

    -- __METADATA_TABLE stores all user-defined tables and data types
    CREATE TABLE __METADATA_TABLE (
        ID INTEGER PRIMARY KEY REFERENCES __METADATA_TABLE_COLUMN_TYPE(ID),
        NAME TEXT NOT NULL,
        FOREIGN KEY (ID) REFERENCES __METADATA_TABLE_COLUMN_TYPE (ID) 
            ON UPDATE CASCADE
            ON DELETE CASCADE
    );

    -- __METADATA_TABLE_COLUMN stores all columns of user-defined tables and data types
    CREATE TABLE __METADATA_TABLE_COLUMN (
        ID INTEGER PRIMARY KEY,
        TABLE_ID INTEGER NOT NULL,
        NAME TEXT NOT NULL,
        TYPE_ID INTEGER NOT NULL DEFAULT 8,
        COLUMN_WIDTH INTEGER NOT NULL DEFAULT 100,
            -- Column width, as measured in pixels
        IS_NULLABLE BIT NOT NULL DEFAULT 1,
        IS_UNIQUE BIT NOT NULL DEFAULT 0,
        IS_PRIMARY_KEY BIT NOT NULL DEFAULT 0,
        DEFAULT_VALUE ANY,
        FOREIGN KEY (TABLE_ID) REFERENCES __METADATA_TABLE (ID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        FOREIGN KEY (TYPE_ID) REFERENCES __METADATA_TABLE_COLUMN_TYPE (ID)
            ON UPDATE CASCADE
            ON DELETE SET DEFAULT
    );

    COMMIT;
    ")?;

    Ok(());
}


static current_db_connection: Mutex<Connection> = Mutex::new();
static current_db_transaction: Mutex<Transaction> = Mutex::new();
static current_db_transaction_last_savepoint_id: Mutex<u32> = Mutex::new(0);

/// Closes any previous database connection, and opens a new one.
pub fn init<P: AsRef<Path>>(path: P) -> Result<()> {
    // Initialize the database if it did not already exist
    if !path.exists() {
        initialize_new_db(path);
    }

    // Open a connection to the database
    let mut conn = current_db_connection.lock().unwrap();
    *conn = Connection::open(path);
    *conn.execute("PRAGMA foreign_keys = ON;")?;
    *conn.execute("PRAGMA journal_mode = WAL;")?;

    // Start the transaction, configure it to update database immediately and to commit if the connection is dropped
    let mut tx = current_db_transaction.lock().unwrap();
    *tx = *conn.transaction_with_behavior(TransactionBehavior::IMMEDIATE)?;
    *tx.set_drop_behavior(DropBehavior::COMMIT)?;

    Ok(());
}

fn create_savepoint() -> Result<()> {
    // Create a savepoint
    let mut tx = current_db_transaction.lock().unwrap();
    let mut savepoint_id = current_db_transaction_last_savepoint_id.lock().unwrap();
    *savepoint_id = *savepoint_id + 1;
    *tx.execute(
        "SAVEPOINT ?1;",
        params![String::from("save") + *savepoint_id.to_string()]
    );
}

/// Undoes the last action performed.
pub fn undo() -> Result<()> {
    let mut savepoint_id = current_db_transaction_last_savepoint_id.lock().unwrap();
    if *savepoint_id > 0 {
        // Rollback to the last savepoint
        let mut tx = current_db_transaction.lock().unwrap();
        *tx.execute(
            "ROLLBACK TO SAVEPOINT ?1;",
            params![String::from("save") + *savepoint_id.to_string()]
        );
        *savepoint_id = *savepoint_id - 1;
    }
    // If savepoint_id = 0, do nothing because the edit stack is empty
}

/// Creates a new table.
pub fn create_table(name: &str) -> Result<()> {
}