import { Menu, MenuItem } from "@tauri-apps/api/menu";
import { message } from "@tauri-apps/plugin-dialog";
import { TableColumnCell, executeAsync } from './backendutils';


/**
 * Adds a cell representing a table cell to the end of a row.
 * @param rowNode The row of the table to insert the cell into.
 * @param tableOid The OID of the table that the cell belongs to.
 * @param rowOid The OID of the row of the table that the cell belongs to.
 * @param cell Information about the cell itself.
 */
export function addTableColumnCellToRow(rowNode: HTMLTableRowElement, tableOid: number, rowOid: number, cell: TableColumnCell) {
  const columnOid = cell.columnOid;

  // Insert cell node
  let tableCellNode: HTMLTableCellElement = document.createElement('td');
  
  // Differentiate based on the column type
  if ('primitive' in cell.columnType) {
    switch (cell.columnType.primitive) {
      case 'Text':
      case 'JSON':
      case 'Number':
      case 'Integer':
      case 'Date':
      case 'Timestamp': {
        // Set cell to be editable
        let editableDivNode: HTMLDivElement = document.createElement('div');
        editableDivNode.contentEditable = 'true';
        if (cell.displayValue) {
          editableDivNode.innerText = cell.displayValue;
        } else {
          editableDivNode.setAttribute('placeholder', '— NULL —');
        }

        // Set up an event listener for when the value is changed
        editableDivNode.addEventListener('focusout', async (_) => {
          const newPrimitiveValue = editableDivNode.innerText.trimEnd();

          await executeAsync({
            invokeAction: 'try_update_primitive_value',
            invokeParams: {
              tableOid: tableOid,
              rowOid: rowOid,
              columnOid: columnOid,
              newPrimitiveValue: newPrimitiveValue == '' ? null : newPrimitiveValue
            }
          })
          .catch(async e => {
            await message(e, {
              title: "Unable to update value.",
              kind: 'warning'
            });
          });
        });

        // Add the div to the cell
        tableCellNode.insertAdjacentElement('beforeend', editableDivNode);
        break;
      }
      case 'Boolean': {
        let inputNode: HTMLInputElement = document.createElement('input');
        inputNode.type = 'checkbox';
        inputNode.checked = cell.displayValue == '1';
        tableCellNode.insertAdjacentElement('beforeend', inputNode);
        break;
      }
      case 'File': {
        // TODO
        break;
      }
      case 'Image': {
        // TODO
        // Like file, but display image as thumbnail
        break;
      }
    }
  } else if ('singleSelectDropdown' in cell.columnType) {
    // TODO
  } else if ('multiSelectDropdown' in cell.columnType) {
    // TODO
  } else if ('reference' in cell.columnType) {
    // TODO
  } else if ('childObject' in cell.columnType) {
    // TODO
  } else if ('childTable' in cell.columnType) {
    // TODO
  }

  // Add null class for CSS
  if (!cell.displayValue) {
    tableCellNode.classList.add('cell-null');
  }

  // Add validation errors
  if (cell.failedValidations.length > 0) {
    tableCellNode.classList.add('cell-error');

    let failureMsgTooltipNode = document.createElement('div');
    failureMsgTooltipNode.classList.add('cell-error-tooltip');
    failureMsgTooltipNode.innerText = cell.failedValidations.map((failure) => failure.description).join('\n');
    tableCellNode.insertAdjacentElement('beforeend', failureMsgTooltipNode);
  }

  // Add the cell to the row
  rowNode.insertAdjacentElement('beforeend', tableCellNode);

  // Add listener to pull up context menu
  tableCellNode.addEventListener('contextmenu', async (e) => {
    e.preventDefault();
    e.returnValue = false;

    const contextMenuItems = await Promise.all([
      MenuItem.new({
        text: 'Cut',
        action: async () => {
          
        }
      }),
      MenuItem.new({
        text: 'Copy',
        action: async () => {
          
        }
      }),
      MenuItem.new({
        text: 'Paste',
        action: async () => {
          
        }
      }),
      MenuItem.new({
        text: 'Edit Cell',
        action: async () => {
          
        }
      })
    ]);
    const contextMenu = await Menu.new({
      items: contextMenuItems
    });
    await contextMenu.popup()
      .catch(async e => {
        await message(e, {
          title: 'Error while displaying context menu for table column.',
          kind: 'error'
        });
      });
  });
}