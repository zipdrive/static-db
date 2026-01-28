import { Menu } from "@tauri-apps/api/menu";
import { Channel, invoke } from "@tauri-apps/api/core";
import { listen } from '@tauri-apps/api/event';
import { message } from "@tauri-apps/plugin-dialog";

async function updateTableListAsync() {
  // Remove the tables in the sidebar that were present before
  document.querySelectorAll('.table-sidebar-button').forEach(element => {
    element.remove();
  });
  let addTableButtonWrapper: HTMLElement | null = document.querySelector('#add-new-table-button-wrapper');

  // Set up a channel
  const onReceiveUpdatedTable = new Channel<{ oid: number, name: string }>();
  onReceiveUpdatedTable.onmessage = (table) => {
    // Load in each table and create a button for that table
    addTableButtonWrapper?.insertAdjacentHTML('beforebegin', 
      `<button class="table-sidebar-button" id="table-sidebar-button-${table.oid}"></button>`
    );

    // Add functionality when clicked
    let tableSidebarButton: HTMLInputElement | null = document.querySelector(`#table-sidebar-button-${table.oid}`);
    if (tableSidebarButton != null) {
      tableSidebarButton.innerText = table.name;
      tableSidebarButton?.addEventListener("click", _ => {
        // Set every other table as inactive
        document.querySelectorAll('.table-sidebar-button').forEach(element => {
          element.classList.remove("active");
        });
        // Set this table as active
        tableSidebarButton?.classList.add("active");
        // Display the table
        displayTable(table.oid);
      });
    }
  };

  // Send a command to Rust to get the list of tables from the database
  await invoke("get_table_list", { tableChannel: onReceiveUpdatedTable });
}

/**
 * Opens the dialog to create a new table.
 */
export async function createTable() {
  await invoke("dialog_create_table", {})
    .catch(async e => {
      await message(e, {
        title: 'Error while opening dialog box to create table.',
        kind: 'error'
      });
    });
}

/**
 * Displays the data for a table.
 * @param tableOid The OID of the table.
 */
export async function displayTable(tableOid: number) {
  type TableColumn = {
    oid: number, 
    name: string,
    width: number,
    columnType: { primitive: string } 
      | { singleSelectDropdown: number }
      | { multiSelectDropdown: number }
      | { reference: number } 
      | { childObject: number } 
      | { childTable: number },
    isNullable: boolean,
    isUnique: boolean,
    isPrimaryKey: boolean,
  };

  type TableCell = {
    rowOid: number
  } | {
    columnOid: number,
    displayValue: string | null
  };

  // Strip the former contents of the table
  let tableNode: HTMLTableElement | null = document.querySelector('#table-content');
  if (tableNode)
    tableNode.innerHTML = '<thead><tr><th></th></tr></thead><tbody></tbody><tfoot><tr></tr></tfoot>';
  let tableHeaderRowNode: HTMLTableRowElement | null = document.querySelector('#table-content > thead > tr');
  let tableBodyNode: HTMLElement | null = document.querySelector('#table-content > tbody');

  // Set up a channel to populate the list of user-defined columns
  let tableColumnList: TableColumn[] = []
  const onReceiveColumn = new Channel<TableColumn>();
  onReceiveColumn.onmessage = (column) => {
    // Add the column to the list of columns
    const columnOid = column.oid;
    tableColumnList.push(column);

    // Add a header for the column
    let tableHeaderNode: HTMLElement | null = document.createElement('th');
    if (tableHeaderNode != null) {
      tableHeaderNode.style.columnWidth = `${column.width}px`;
      tableHeaderNode.innerText = column.name;
      tableHeaderRowNode?.insertAdjacentElement('beforeend', tableHeaderNode);

      // Add listener to pull up context menu
      tableHeaderNode.addEventListener('contextmenu', async (e) => {
        e.preventDefault();
        e.returnValue = false;

        await invoke("contextmenu_table_column", { tableOid: tableOid, columnOid: columnOid })
          .catch(async e => {
            await message(e, {
              title: 'Error while displaying context menu for table column.',
              kind: 'error'
            });
          });
      });
    }
  };

  // Send a command to Rust to get the list of table columns from the database
  await invoke("get_table_column_list", { tableOid: tableOid, columnChannel: onReceiveColumn })
    .catch(async e => {
      await message(e, {
        title: 'Error while retrieving list of columns for table.',
        kind: 'error'
      });
    });

  // Add a final column header that is a button to add a new column
  const numColumns = tableColumnList.length;
  let tableAddColumnHeaderNode = document.createElement('th');
  if (tableAddColumnHeaderNode != null) {
    tableAddColumnHeaderNode.id = 'add-new-column-button';
    tableAddColumnHeaderNode.innerText = 'Add New Column';
    tableAddColumnHeaderNode.addEventListener('click', async (_) => {
      await invoke("dialog_create_table_column", {
        tableOid: tableOid,
        columnOrdering: numColumns
      }).catch(async e => {
          await message(e, {
            title: 'Error while opening dialog box to create table.',
            kind: 'error'
          });
        });
    });
    tableHeaderRowNode?.insertAdjacentElement('beforeend', tableAddColumnHeaderNode);
  }

  // Set the span of the footer
  let tableFooterRowNode: HTMLElement | null = document.querySelector('#table-content > tfoot > tr');
  let tableFooterCellNode = document.createElement('td');
  tableFooterCellNode.id = 'add-new-row-button';
  tableFooterCellNode.innerText = 'Add New Row';
  // Set the footer to span the entire row
  tableFooterCellNode.setAttribute('colspan', (tableColumnList.length + 2).toString());
  // Set what it should do on click
  tableFooterCellNode.addEventListener('click', (_) => {
    invoke('push_row', { tableOid: tableOid })
      .catch(async (e) => {
        await message(e, {
          title: 'Error while adding new row into table.',
          kind: 'error'
        });
      });
  });
  tableFooterRowNode?.insertAdjacentElement('beforeend', tableFooterCellNode);

  // Set up a channel to populate the rows of the table
  let rowOids: number[] = [];
  const onReceiveCell = new Channel<TableCell>();
  let currentRowNode: HTMLTableRowElement | null = null;
  onReceiveCell.onmessage = (cell) => {
    if ('rowOid' in cell) {
      // New row
      const rowOid = cell.rowOid;
      rowOids.push(rowOid);
      currentRowNode = document.createElement('tr');
      currentRowNode.insertAdjacentHTML('beforeend', `<td style="text-align: center;">${rowOid}</td>`);
      tableBodyNode?.insertAdjacentElement('beforeend', currentRowNode);

      // Add listener to pull up context menu
      currentRowNode.addEventListener('contextmenu', async (e) => {
        e.preventDefault();
        e.returnValue = false;

        await invoke("contextmenu_table_row", { tableOid: tableOid, rowOid: rowOid })
          .catch(async e => {
            await message(e, {
              title: 'Error while displaying context menu for table row.',
              kind: 'error'
            });
          });
      });
    } else {
      // Add cell to current row
      if (currentRowNode != null) {
        // Get current row and column OID
        const rowOid = rowOids[rowOids.length - 1];
        const columnOid = cell.columnOid;

        // Insert cell node
        let tableCellNode: HTMLElement = document.createElement('td');
        tableCellNode.innerText = cell.displayValue ?? '';
        currentRowNode.insertAdjacentElement('beforeend', tableCellNode);

        // Add listener to pull up context menu
        tableCellNode.addEventListener('contextmenu', async (e) => {
          e.preventDefault();
          e.returnValue = false;

          await invoke("contextmenu_table_cell", { tableOid: tableOid, columnOid: columnOid, rowOid: rowOid })
            .catch(async e => {
              await message(e, {
                title: 'Error while displaying context menu for table cell.',
                kind: 'error'
              });
            });
        });
      }
    }
  };

  // Send a command to Rust to get the list of rows from the database
  await invoke("get_table_data", { tableOid: tableOid, cellChannel: onReceiveCell })
    .catch(async e => {
      await message(e, {
        title: 'Error while retrieving rows of table.',
        kind: 'error'
      });
    });
}


// Add initial listeners
window.addEventListener("DOMContentLoaded", () => {
  document.querySelector('#add-new-table-button')?.addEventListener("click", createTable);

  updateTableListAsync();
});

listen<any>("update-table-list", updateTableListAsync);
listen<number>("update-table-data", (e) => displayTable(e.payload));