import { invoke, Channel } from "@tauri-apps/api/core";
import { message } from "@tauri-apps/plugin-dialog";

export type TableBasicMetadata = {
    oid: number,
    name: string 
};

export type ColumnType = { primitive: 'Any' | 'Boolean' | 'Integer' | 'Number' | 'Date' | 'Timestamp' | 'Text' | 'JSON' | 'File' | 'Image' } 
    | { singleSelectDropdown: number }
    | { multiSelectDropdown: number }
    | { reference: number } 
    | { childObject: number } 
    | { childTable: number };

export type TableColumnMetadata = {
    oid: number, 
    name: string,
    columnStyle: string,
    columnType: ColumnType,
    isNullable: boolean,
    isUnique: boolean,
    isPrimaryKey: boolean,
};

export type DropdownValue = {
    trueValue: string | null,
    displayValue: string | null
};

export type TableColumnCell = {
    columnOid: number, 
    columnType: ColumnType, 
    trueValue: string | null,
    displayValue: string | null,
    failedValidations: { description: string }[]
};

export type TableCellChannelPacket = {
    rowOid: number
} | TableColumnCell;

export type TableRowCellChannelPacket = {
    rowExists: boolean
} | TableColumnCell;


export type Query = {
    invokeAction: 'get_table_list',
    invokeParams: {
        tableChannel: Channel<TableBasicMetadata>
    }
} | {
    invokeAction: 'get_table_column',
    invokeParams: {
        columnOid: number
    }
} | {
    invokeAction: 'get_table_column_dropdown_values',
    invokeParams: {
        columnOid: number,
        dropdownValueChannel: Channel<DropdownValue>
    }
} | {
    invokeAction: 'get_table_column_list',
    invokeParams: {
        tableOid: number,
        columnChannel: Channel<TableColumnMetadata>
    }
} | {
    invokeAction: 'get_table_data',
    invokeParams: {
        tableOid: number,
        cellChannel: Channel<TableCellChannelPacket>
    }
} | {
    invokeAction: 'get_table_row',
    invokeParams: {
        tableOid: number,
        rowOid: number,
        cellChannel: Channel<TableRowCellChannelPacket>
    }
};

export type Dialog = {
    invokeAction: 'dialog_create_table',
    invokeParams: {}
} | {
    invokeAction: 'dialog_create_table_column',
    invokeParams: {
        tableOid: number,
        columnOrdering: number
    }
} | {
    invokeAction: 'dialog_edit_table_column',
    invokeParams: {
        tableOid: number,
        columnOid: number
    }
} | {
    invokeAction: 'dialog_close',
    invokeParams: {}
};

export type Action = {
    createTable: {
        name: string
    }
} | {
    deleteTable: {
        tableOid: number
    }
} | {
    createTableColumn: {
        tableOid: number,
        columnOrdering: number,
        columnName: string,
        columnType: ColumnType,
        columnStyle: string,
        isNullable: boolean,
        isUnique: boolean,
        isPrimaryKey: boolean
    }
} | {
    editTableColumnMetadata: {
        tableOid: number,
        columnOid: number,
        columnName: string,
        columnType: ColumnType,
        columnStyle: string,
        isNullable: boolean,
        isUnique: boolean,
        isPrimaryKey: boolean
    }
} | {
    editTableColumnDropdownValues: {
        tableOid: number,
        columnOid: number,
        dropdownValues: DropdownValue[]
    }
} | {
    deleteTableColumn: {
        tableOid: number,
        columnOid: number
    }
} | {
    pushTableRow: {
        tableOid: number
    }
} | {
    insertTableRow: {
        tableOid: number,
        rowOid: number
    }
} | {
    deleteTableRow: {
        tableOid: number,
        rowOid: number
    }
} | {
    updateTableCellStoredAsPrimitiveValue: {
        tableOid: number,
        rowOid: number,
        columnOid: number,
        newPrimitiveValue: string | null
    }
};



/**
 * Runs a query and returns the result or passes the result through one or more channels.
 * @param query The query to run.
 * @returns The result of the query, if singular. Otherwise, returns void.
 */
export async function queryAsync(query: Query): Promise<any> {
    return await invoke(query.invokeAction, query.invokeParams)
    .catch(async (e) => {
        await message(e, {
            title: "An error occurred while reading database.",
            kind: 'error'
        });
    });
}

/**
 * Opens a dialog window.
 * @param dialog The dialog window to open.
 */
export async function openDialogAsync(dialog: Dialog): Promise<void> {
    await invoke(dialog.invokeAction, dialog.invokeParams)
    .catch(async (e) => {
        await message(e, {
            title: "An error occurred while opening dialog box.",
            kind: 'error'
        });
    });
}

/**
 * Closes the current dialog window.
 */
export async function closeDialogAsync(): Promise<void> {
    await invoke('dialog_close', {})
    .catch(async (e) => {
        await message(e, {
            title: "An error occurred while closing dialog box.",
            kind: 'error'
        });
    });
}



let historyStack: Action[] = [];
let redoStack: Action[] = [];

/**
 * Does an action with an impact on the state of the database.
 * @param action The action to perform.
 * @returns May return the OID of the object created. Usually returns void.
 */
export async function executeAsync(action: Action): Promise<void> {
    historyStack.push(action);
    redoStack = [];
    return await invoke('execute', action);
}

/**
 * Undoes the last action performed.
 */
export async function undoAsync() {
    const lastActionPerformed = historyStack.pop();
    if (lastActionPerformed) {
        redoStack.push();
        await invoke('undo', {});
    }
}

/**
 * Redoes the last action that was undone.
 */
export async function redoAsync() {
    const lastActionUndone = redoStack.pop();
    if (lastActionUndone) {
        await executeAsync(lastActionUndone);
    }
}