use std::collections::{HashMap, HashSet, LinkedList};

use rusqlite::{params, Row, Error as RusqliteError, OptionalExtension};
use serde::Serialize;
use tauri::ipc::Channel;
use crate::backend::column::MetadataColumnType;
use crate::backend::{column, db, table};
use crate::util::error;


#[derive(Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase", untagged)]
pub enum Cell {
    RowStart {
        row_oid: i64
    },
    ColumnValue {
        column_oid: i64,
        column_type: column::MetadataColumnType,
        display_value: Option<String>,
        failed_validations: Vec<error::FailedValidation>
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase", untagged)]
pub enum RowCell {
    RowExists {
        row_exists: bool
    },
    ColumnValue {
        column_oid: i64,
        column_type: column::MetadataColumnType,
        display_value: Option<String>,
        failed_validations: Vec<error::FailedValidation>
    }
}

/// Insert a row into the data such that the OID places it before any existing rows with that OID.
pub fn insert(table_oid: i64, row_oid: i64) -> Result<i64, error::Error> {
    let action = db::begin_db_action()?;

    // If OID is already in database, shift every row with OID >= row_oid up by 1
    let select_cmd = format!("SELECT OID FROM TABLE{table_oid} WHERE OID = ?1;");
    let existing_row_oid = action.trans.query_one(&select_cmd, params![row_oid], 
        |row| {
            return Ok(row.get::<_, i64>(0)?);
        }
    ).optional()?;

    match existing_row_oid {
        None => {
            // Insert with OID = row_oid
            let insert_cmd = format!("INSERT INTO TABLE{table_oid} (OID) VALUES (?1);");
            action.trans.execute(&insert_cmd, params![row_oid])?;
            let row_oid = action.trans.last_insert_rowid();

            // Return the row_oid
            return Ok(row_oid);
        },
        Some(_) => {
            let existing_prev_row_oid = action.trans.query_one(&select_cmd, params![row_oid - 1], 
                |row| {
                    return Ok(row.get::<_, i64>(0)?);
                }
            ).optional()?;
            
            match existing_prev_row_oid {
                None => {
                    // Insert with OID = row_oid - 1
                    let insert_cmd = format!("INSERT INTO TABLE{table_oid} (OID) VALUES (?1);");
                    action.trans.execute(&insert_cmd, params![row_oid - 1])?;
                    let row_oid = action.trans.last_insert_rowid();

                    // Return the row_oid
                    return Ok(row_oid);
                },
                Some(_) => {
                    // Increment every OID >= row_oid up by 1 to make room for the new row
                    let select_all_cmd = format!("SELECT OID FROM TABLE{table_oid} WHERE OID >= ?1 ORDER BY OID DESC;");
                    action.query_iterate(&select_all_cmd, params![row_oid], 
                        &mut |row| {
                            let update_cmd = format!("UPDATE TABLE{table_oid} SET OID = OID + 1 WHERE OID = ?1;");
                            action.trans.execute(&update_cmd, params![row.get::<_, i64>(0)?])?;
                            return Ok(());
                        }
                    )?;

                    // Insert the row
                    let insert_cmd = format!("INSERT INTO TABLE{table_oid} (OID) VALUES (?1);");
                    action.trans.execute(&insert_cmd, params![row_oid])?;
                    let row_oid = action.trans.last_insert_rowid();

                    // Return the row_oid
                    return Ok(row_oid);
                }
            }
        }
    }
}

/// Push a row into the table with a default OID.
pub fn push(table_oid: i64) -> Result<i64, error::Error> {
    let action = db::begin_db_action()?;

    // Insert the row
    let insert_cmd = format!("INSERT INTO TABLE{table_oid} DEFAULT VALUES;");
    action.trans.execute(&insert_cmd, [])?;
    let row_oid = action.trans.last_insert_rowid();

    // Return the row OID
    return Ok(row_oid);
}

/// Delete the row with the given OID.
pub fn delete(table_oid: i64, row_oid: i64) -> Result<(), error::Error> {
    let action = db::begin_db_action()?;

    // Delete the row
    let delete_cmd = format!("DELETE FROM TABLE{table_oid} WHERE OID = ?1;");
    action.trans.execute(&delete_cmd, params![row_oid])?;

    // Return the row OID
    return Ok(());
}

/// Attempts to update a primitive value in a table.
pub fn try_update_primitive_value(table_oid: i64, row_oid: i64, column_oid: i64, new_value: String) -> Result<(), error::Error> {
    let action = db::begin_db_action()?;

    // Update the value
    let update_cmd = format!("UPDATE TABLE{table_oid} SET COLUMN{column_oid} = ?1 WHERE OID = ?2;");
    action.trans.execute(
        &update_cmd,
        params![new_value, row_oid]
    )?;

    // Return OK
    return Ok(());
}


struct Column {
    ord: usize,
    column_oid: i64,
    column_name: String,
    column_type: MetadataColumnType,
    is_nullable: bool,
    invalid_nonunique_oid: HashSet<i64>,
    is_primary_key: bool
}

/// Construct a SELECT query to get data from a table
fn construct_data_query(action: &db::DbAction<'_>, table_oid: i64) -> Result<(String, LinkedList<Column>), error::Error> {
    // Build the SELECT query
    let mut select_cmd_cols: String = String::from("SELECT t.OID");
    let mut select_cmd_tables: String = format!("FROM TABLE{table_oid} t");
    let mut ord: usize = 1;
    let mut table_num: i64 = 1;
    let mut columns = LinkedList::<Column>::new();
    action.query_iterate(
        "SELECT 
            c.OID,
            c.TYPE_OID,
            t.MODE,
            c.IS_NULLABLE,
            c.IS_UNIQUE,
            c.IS_PRIMARY_KEY,
            c.NAME
        FROM METADATA_TABLE_COLUMN c
        INNER JOIN METADATA_TABLE_COLUMN_TYPE t ON t.OID = c.TYPE_OID
        WHERE c.TABLE_OID = ?1
        ORDER BY c.COLUMN_ORDERING;",
        params![table_oid], 
        &mut |row| {
            let column_oid: i64 = row.get(0)?;
            let column_type: MetadataColumnType = MetadataColumnType::from_database(row.get(1)?, row.get(2)?);
            
            let enforce_uniqueness: bool = row.get(4)?;
            let mut invalid_nonunique_oid: HashSet<i64> = HashSet::<i64>::new();

            match column_type {
                MetadataColumnType::Primitive(_)
                | MetadataColumnType::SingleSelectDropdown(_) => {
                    // Primitive type
                    // Single-select dropdown (i.e. *-to-1 join with table of values)
                    select_cmd_cols = format!("{select_cmd_cols}, t.COLUMN{column_oid}");
                    
                    // Check for invalid nonunique rows
                    if enforce_uniqueness {
                        let check_nonunique_cmd = format!("
                            SELECT t.OID FROM TABLE{table_oid} t
                            INNER JOIN (
                                SELECT COLUMN{column_oid}, COUNT(OID) AS ROW_COUNT
                                FROM TABLE{table_oid} 
                                GROUP BY COLUMN{column_oid} 
                                HAVING COUNT(OID) > 1
                            ) a ON a.COLUMN{column_oid} = t.COLUMN{column_oid}
                        ");
                        action.query_iterate(&check_nonunique_cmd, [], 
                            &mut |row| {
                                invalid_nonunique_oid.insert(row.get(0)?);
                                return Ok(());
                            }
                        )?;
                    }
                },
                MetadataColumnType::MultiSelectDropdown(column_type_oid) => {
                    // Multi-select dropdown (i.e. *-to-* join with table of values)
                    select_cmd_cols = format!("{select_cmd_cols}, (SELECT GROUP_CONCAT(b.VALUE) FROM TABLE{column_type_oid}_MULTISELECT b WHERE b.OID = t.OID GROUP BY b.OID) AS COLUMN{column_oid}");
                    
                    // Check for invalid nonunique rows
                    if enforce_uniqueness {
                        let check_nonunique_cmd = format!("
                            WITH TABLE_SURROGATE AS (
                                SELECT 
                                    OID,
                                    GROUP_CONCAT(VALUE) AS COLUMN{column_oid}
                                FROM TABLE{column_type_oid}_MULTISELECT 
                                GROUP BY OID
                            )
                            SELECT t.OID FROM TABLE_SURROGATE t
                            INNER JOIN (
                                SELECT COLUMN{column_oid}, COUNT(OID) AS ROW_COUNT
                                FROM TABLE_SURROGATE
                                GROUP BY COLUMN{column_oid} 
                                HAVING COUNT(OID) > 1
                            ) a ON a.COLUMN{column_oid} = t.COLUMN{column_oid}
                        ");
                        action.query_iterate(&check_nonunique_cmd, [], 
                            &mut |row| {
                                invalid_nonunique_oid.insert(row.get(0)?);
                                return Ok(());
                            }
                        )?;
                    }
                },
                MetadataColumnType::Reference(column_type_oid)
                | MetadataColumnType::ChildObject(column_type_oid) => {
                    // Reference to row in other table
                    // Pull display value from TABLE0_SURROGATE view
                    select_cmd_cols = format!("{select_cmd_cols}, t{table_num}.DISPLAY_VALUE");
                    select_cmd_tables = format!("{select_cmd_tables} LEFT JOIN TABLE{column_type_oid}_SURROGATE t{table_num} ON t{table_num}.OID = t.COLUMN{column_oid}");
                    table_num += 1;
                    
                    // Check for invalid nonunique rows
                    if enforce_uniqueness {
                        let check_nonunique_cmd = format!("
                            SELECT t.OID FROM TABLE{table_oid} t
                            INNER JOIN (
                                SELECT COLUMN{column_oid}, COUNT(OID) AS ROW_COUNT
                                FROM TABLE{table_oid} 
                                GROUP BY COLUMN{column_oid} 
                                HAVING COUNT(OID) > 1
                            ) a ON a.COLUMN{column_oid} = t.COLUMN{column_oid}
                        ");
                        action.query_iterate(&check_nonunique_cmd, [], 
                            &mut |row| {
                                invalid_nonunique_oid.insert(row.get(0)?);
                                return Ok(());
                            }
                        )?;
                    }
                },
                MetadataColumnType::ChildTable(column_type_oid) => {
                    // Child table
                    // Pull display values for items from TABLE0_SURROGATE view
                    select_cmd_cols = format!("{select_cmd_cols}, (SELECT GROUP_CONCAT(b.DISPLAY_VALUE) FROM TABLE{column_type_oid} a INNER JOIN TABLE{column_type_oid}_SURROGATE b ON b.OID = a.OID WHERE a.PARENT_OID = t.OID) AS COLUMN{column_oid}");
                },
                _ => {
                    return Err(error::Error::AdhocError("Unknown column type mode."));
                }
            }

            // Push the column information
            columns.push_back(Column {
                ord: ord, 
                column_oid: column_oid,
                column_name: row.get(6)?,
                column_type: column_type,
                is_nullable: row.get(3)?,
                invalid_nonunique_oid: invalid_nonunique_oid,
                is_primary_key: row.get(5)?
            });
            ord += 1;
            return Ok(());
        }
    )?;
    return Ok((format!("{select_cmd_cols} {select_cmd_tables}"), columns));
}

/// Sends all cells for the table through a channel.
pub fn send_table_data(table_oid: i64, cell_channel: Channel<Cell>) -> Result<(), error::Error> {
    let action = db::begin_readonly_db_action()?;
    let (table_select_cmd, columns) = construct_data_query(&action, table_oid)?;
    
    // Iterate over the results, sending each cell to the frontend
    action.query_iterate(
        &table_select_cmd, 
        [], 
        &mut |row| {
            // Start by sending the OID, which is the first ordinal
            let row_oid: i64 = row.get(0)?;
            cell_channel.send(Cell::RowStart { row_oid: row.get(0)? })?;

            let invalid_key: bool = false; // TODO

            // Iterate over the columns, sending over the displayed value of that cell in the current row for each
            for column in columns.iter() {
                let display_value: Option<String> = row.get(column.ord)?;
                let mut failed_validations: Vec<error::FailedValidation> = Vec::<error::FailedValidation>::new();

                // Nullability validation
                if !column.is_nullable && display_value == None {
                    failed_validations.push(error::FailedValidation {
                        description: format!("{} cannot be NULL!", column.column_name)
                    });
                }

                // Uniqueness validation
                if column.invalid_nonunique_oid.contains(&row_oid) {
                    failed_validations.push(error::FailedValidation {
                        description: format!("{} value is not unique!", column.column_name)
                    });
                }

                // Primary key validation
                if column.is_primary_key && invalid_key {
                    failed_validations.push(error::FailedValidation {
                        description: format!("Primary key for this row is not unique!")
                    });
                }

                // Send the cell value to frontend
                cell_channel.send(Cell::ColumnValue {
                    column_oid: column.column_oid, 
                    column_type: column.column_type.clone(), 
                    display_value: display_value,
                    failed_validations: failed_validations
                })?;
            }

            // Conclude the row's iteration
            return Ok(());
        }
    )?;
    return Ok(());
}

/// Sends all cells for a row in the table through a channel.
pub fn send_table_row(table_oid: i64, row_oid: i64, cell_channel: Channel<RowCell>) -> Result<(), error::Error> {
    let action = db::begin_readonly_db_action()?;
    let (table_select_all_cmd, columns) = construct_data_query(&action, table_oid)?;
    let table_select_cmd = format!("{table_select_all_cmd} WHERE t.OID = ?1;");

    // Query for the specified row
    match action.trans.query_row_and_then(
        &table_select_cmd, 
        params![row_oid], 
        |row| -> Result<(), error::Error> {
            // Start by sending message that confirms the row exists
            cell_channel.send(RowCell::RowExists { row_exists: true })?;

            let invalid_key = false;

            // Iterate over the columns, sending over the displayed value of that cell in the current row for each
            for column in columns.iter() {
                let display_value: Option<String> = row.get(column.ord)?;
                let mut failed_validations: Vec<error::FailedValidation> = Vec::<error::FailedValidation>::new();

                // Nullability validation
                if !column.is_nullable && display_value == None {
                    failed_validations.push(error::FailedValidation {
                        description: format!("{} cannot be NULL!", column.column_name)
                    });
                }

                // Uniqueness validation
                if column.invalid_nonunique_oid.contains(&row_oid) {
                    failed_validations.push(error::FailedValidation {
                        description: format!("{} value is not unique!", column.column_name)
                    });
                }

                // Primary key validation
                if column.is_primary_key && invalid_key {
                    failed_validations.push(error::FailedValidation {
                        description: format!("Primary key for this row is not unique!")
                    });
                }

                // Send the cell value to frontend
                cell_channel.send(RowCell::ColumnValue {
                    column_oid: column.column_oid, 
                    column_type: column.column_type.clone(), 
                    display_value: display_value,
                    failed_validations: failed_validations
                })?;
            }

            // 
            return Ok(());
        }
    ) {
        Err(error::Error::RusqliteError(e)) => {
            match e {
                RusqliteError::QueryReturnedNoRows => {
                    cell_channel.send(RowCell::RowExists { row_exists: false })?;
                    return Ok(());
                },
                _ => {
                    return Err(error::Error::from(e));
                }
            }
        },
        Err(e) => {
            return Err(e);
        }
        Ok(_) => {
            return Ok(());
        }
    }
}