import { closeDialogAsync, executeAsync } from "./backendutils";
import { message } from "@tauri-apps/plugin-dialog";

async function createTable() {
    let tableNameInput: HTMLInputElement = document.getElementById('tableName') as HTMLInputElement;
    let tableName = tableNameInput.value;
    
    if (!tableName || !tableName.trim()) {
        message("Unable to create a table with no name.", {
            title: "An error occurred while creating table.",
            kind: 'error'
        });
    } else {
        await executeAsync({
            createTable: {
                tableName: tableName
            }
        })
        .then(closeDialogAsync)
        .catch(async (e) => {
            await message(e, {
                title: "An error occurred while creating table.",
                kind: 'error'
            });
        });
    }
}

function cancel() {
    closeDialogAsync()
    .catch(async (e) => {
        await message(e, {
            title: "An error occurred while closing dialog box.",
            kind: 'error'
        });
    });
}

// Add initial listeners
window.addEventListener("DOMContentLoaded", () => {
    document.querySelector('#create-table-button')?.addEventListener("click", async (e) => {
        console.debug('createTable() called.');
        e.preventDefault();
        e.returnValue = false;
        await createTable();
    });
    document.querySelector('#cancel-create-table-button')?.addEventListener("click", async (e) => {
        e.preventDefault();
        e.returnValue = false;
        await cancel();
    });
});