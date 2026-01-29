mod db;
mod table;
mod column;
mod data;
use tauri::menu::{ContextMenu, Menu, MenuItem, MenuBuilder};
use tauri::{AppHandle, WebviewWindowBuilder, WebviewUrl, Emitter, Size, PhysicalSize, Manager};
use tauri_plugin_dialog::{DialogExt, MessageDialogKind};
use tauri::ipc::Channel;
use crate::util::error;

#[tauri::command]
/// Initialize a connection to a StaticDB database file.
pub fn init(path: String) -> Result<(), error::Error> {
    return db::init(path);
}

/// Shuts down the connection to the StaticDB database file.
pub fn close() -> Result<(), error::Error> {
    return db::close();
}

/// Sends a message to the frontend that the list of tables needs to be updated.
fn msg_update_table_list(app: &AppHandle) {
    app.emit("update-table-list", ()).unwrap();
}

/// Sends a message to the frontend that the currently-displayed table needs to be refreshed.
fn msg_update_table_data(app: &AppHandle, table_oid: i64) {
    app.emit("update-table-data", table_oid).unwrap();
}

/// Sends a message to the frontend that a row in the currently-displayed table needs to be refreshed.
fn msg_update_table_row(app: &AppHandle, table_oid: i64, row_oid: i64) {
    app.emit("update-table-row", (table_oid, row_oid)).unwrap();
}

#[tauri::command]
/// Closes the current dialog window.
pub fn dialog_close(window: tauri::Window) -> Result<(), error::Error> {
    match window.close() {
        Ok(_) => { return Ok(()); },
        Err(e) => { return Err(error::Error::TauriError(e)); }
    }
}

#[tauri::command]
/// Pull up a dialog window for creating a new table.
pub async fn dialog_create_table(app: AppHandle) -> Result<(), error::Error> {
    let window_idx = app.webview_windows().len();
    WebviewWindowBuilder::new(
        &app,
        format!("createTableWindow-{window_idx}"),
        WebviewUrl::App("/src/frontend/dialogTable.html".into()),
    )
    .title("Create New Table")
    .inner_size(400.0, 150.0)
    .maximizable(false)
    .build()?;
    return Ok(());
}

#[tauri::command]
/// Create a table.
pub fn create_table(app: AppHandle, name: String) -> Result<i64, error::Error> {
    let table_oid: i64 = table::create(name)?;
    msg_update_table_list(&app);
    return Ok(table_oid);
}

#[tauri::command]
pub fn get_table_list(table_channel: Channel<table::BasicMetadata>) -> Result<(), error::Error> {
    // Use channel to send BasicMetadata objects
    table::send_metadata_list(table_channel)?;
    return Ok(());
}

#[tauri::command]
/// Pull up a dialog window for creating a new table.
pub async fn dialog_create_table_column(app: AppHandle, table_oid: i64, column_ordering: i64) -> Result<(), error::Error> {    
    let window_idx = app.webview_windows().len();
    WebviewWindowBuilder::new(
        &app,
        format!("tableColumnWindow-{window_idx}"),
        WebviewUrl::App(format!("/src/frontend/dialogTableColumn.html?table_oid={table_oid}&column_ordering={column_ordering}").into()),
    )
    .title("Add New Column")
    .inner_size(400.0, 200.0)
    .resizable(false)
    .maximizable(false)
    .build()?;
    return Ok(());
}

#[tauri::command]
/// Create a new column in a table.
pub fn create_table_column(app: AppHandle, table_oid: i64, column_name: String, column_type: column::MetadataColumnType, column_ordering: i64, column_style: &str, is_nullable: bool, is_unique: bool, is_primary_key: bool) -> Result<i64, error::Error> {
    // Wrapper for column::create
    let column_oid = column::create(table_oid, &column_name, column_type, column_ordering, column_style, is_nullable, is_unique, is_primary_key)?;
    msg_update_table_data(&app, table_oid);
    return Ok(column_oid);
}

#[tauri::command]
/// Pull up a dialog window for editing a table column.
pub async fn dialog_edit_table_column(app: AppHandle, table_oid: i64, column_oid: i64) -> Result<(), error::Error> {    
    let window_idx = app.webview_windows().len();
    WebviewWindowBuilder::new(
        &app,
        format!("tableColumnWindow-{window_idx}"),
        WebviewUrl::App(format!("/src/frontend/dialogTableColumn.html?table_oid={table_oid}&column_oid={column_oid}").into()),
    )
    .title("Edit Column")
    .inner_size(400.0, 200.0)
    .resizable(false)
    .maximizable(false)
    .build()?;
    return Ok(());
}

#[tauri::command]
/// Edit a column in a table.
pub fn edit_table_column(app: AppHandle, table_oid: i64, column_oid: i64, column_name: &str, column_type: column::MetadataColumnType, column_style: &str, is_nullable: bool, is_unique: bool, is_primary_key: bool) -> Result<(), error::Error> {
    column::edit(column_oid, column_name, column_type, column_style, is_nullable, is_unique, is_primary_key)?;
    msg_update_table_data(&app, table_oid);
    return Ok(());
}

#[tauri::command]
/// Get the metadata for a particular column in a table.
pub fn get_table_column(column_oid: i64) -> Result<Option<column::Metadata>, error::Error> {
    return column::get_metadata(column_oid);
}

#[tauri::command]
pub fn get_table_column_list(table_oid: i64, column_channel: Channel<column::Metadata>) -> Result<(), error::Error> {
    // Use channel to send BasicMetadata objects
    column::send_metadata_list(table_oid, column_channel)?;
    return Ok(());
}

#[tauri::command]
/// Insert a blank row with default OID into data table.
pub fn push_row(app: AppHandle, table_oid: i64) -> Result<i64, error::Error> {
    let row_oid = data::push(table_oid)?;
    msg_update_table_row(&app, table_oid, row_oid);
    return Ok(row_oid);
}

#[tauri::command]
/// Insert a blank row and update OIDs such that the inserted row appears before the row with the given OID, but after any existing row with OID less than it.
pub fn insert_row(app: AppHandle, table_oid: i64, row_oid: i64) -> Result<i64, error::Error> {
    let row_oid = data::insert(table_oid, row_oid)?;
    msg_update_table_data(&app, table_oid);
    return Ok(row_oid);
}

#[tauri::command]
/// Deletes the row with the given OID.
pub fn delete_row(app: AppHandle, table_oid: i64, row_oid: i64) -> Result<i64, error::Error> {
    data::delete(table_oid, row_oid)?;
    msg_update_table_row(&app, table_oid, row_oid);
    return Ok(row_oid);
}

#[tauri::command]
/// Attempts to update a column with type Primitive, SingleSelectDropdown, Reference.
pub fn try_update_primitive_value(app: AppHandle, table_oid: i64, row_oid: i64, column_oid: i64, new_primitive_value: String) -> Result<(), error::Error> {
    data::try_update_primitive_value(table_oid, row_oid, column_oid, new_primitive_value)?;
    msg_update_table_row(&app, table_oid, row_oid);
    return Ok(());
}

#[tauri::command]
pub fn get_table_data(table_oid: i64, cell_channel: Channel<data::Cell>) -> Result<(), error::Error> {
    data::send_table_data(table_oid, cell_channel)?;
    return Ok(());
}

#[tauri::command]
pub fn get_table_row(table_oid: i64, row_oid: i64, cell_channel: Channel<data::RowCell>) -> Result<(), error::Error> {
    data::send_table_row(table_oid, row_oid, cell_channel)?;
    return Ok(());
}