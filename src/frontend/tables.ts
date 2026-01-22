import { invoke } from "@tauri-apps/api/core";
import { listen } from '@tauri-apps/api/event';

export function initListeners() {
  // Set up the tab listeners
  let addTableButton: HTMLInputElement | null = document.querySelector('#add-new-table-button');
  addTableButton?.addEventListener("click", (e) => {
    invoke("dialog_create_table", {});
  });
};

listen<>("update-table-list", e => {
  
});