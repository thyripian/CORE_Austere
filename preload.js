const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('electronAPI', {
    // Opens the native “Browse…” dialog (only .db), returns the chosen path
    selectDbFile: () => ipcRenderer.invoke('dialog:openFile'),

    // Called when you drop a file into the Settings drop‐zone
    loadDbFile: (filePath) => ipcRenderer.invoke('file-dropped', filePath),

    exportKml: (table, query, latCol, lonCol) =>
        ipcRenderer.invoke('export:kml', { table, query, latCol, lonCol }),

    // Allow renderer to read back the dynamically chosen port
    getApiPort: () => ipcRenderer.invoke('get-api-port'),

    // Existing quit
    quitApp: () => ipcRenderer.send('app:quit')
});
