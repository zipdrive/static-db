const urlParams = new URLSearchParams(window.location.search);
const urlParamTableOid = urlParams.get('table_oid');
const urlParamObjOid = urlParams.get('obj_oid');

if (urlParamTableOid && urlParamObjOid) {
  const tableOid: number = parseInt(urlParamTableOid);
  const objOid: number = parseInt(urlParamObjOid);

  async function refreshObjectAsync() {
    
    // Strip the former contents of the table
    let tableNode: HTMLTableElement | null = document.querySelector('#object-content');
    if (tableNode)
      tableNode.innerHTML = '<colgroup><col span="1" class="field-name-cell"><col span="1" class="field-input-cell"></colgroup><tbody></tbody>';
    let tableBodyNode: HTMLElement | null = document.querySelector('#table-content > tbody');

    
  }

}