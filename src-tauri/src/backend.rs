mod db;
mod table;
use tauri::{AppHandle, WebviewWindowBuilder, WebviewUrl, Emitter};

use crate::util::error;

#[tauri::command]
/// Initialize a connection to a StaticDB database file.
pub fn init(path: String) -> Result<(), error::Error> {
    return db::init(path);
}

/// Sends a message to the frontend that the list of tables needs to be updated.
fn msg_update_table_list(app: &AppHandle) {
    app.emit("update-table-list", ()).unwrap();
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
pub fn dialog_create_table(app: AppHandle) -> Result<(), error::Error> {
    match WebviewWindowBuilder::new(
        &app,
        String::from("createTableWindow"),
        WebviewUrl::App("/src/dialogs/createTable.html".into()),
    ).build() {
        Ok(_) => {
            return Ok(());
        },
        Err(e) => {
            return Err(error::Error::TauriError(e));
        }
    }
}

#[tauri::command]
/// Create a table.
pub fn create_table(app: AppHandle, name: String) -> Result<(), error::Error> {
    table::Table::create(name)?;
    msg_update_table_list(&app);
    return Ok(());
}

#[tauri::command]
pub fn get_table_list() -> Result<(), error::Error> {
    // Use channel
}