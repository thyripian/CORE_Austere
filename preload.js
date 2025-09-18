const { contextBridge, ipcRenderer, shell } = require('electron');

contextBridge.exposeInMainWorld('electronAPI', {
    // Dialog operations
    dialog: {
        openFile: () => ipcRenderer.invoke('dialog:openFile'),
        selectFolder: () => ipcRenderer.invoke('dialog:selectFolder'),
    },

    // Database path management
    db: {
        setPath: (absPath) => ipcRenderer.invoke('db:setPath', absPath),
        getPath: () => ipcRenderer.invoke('db:getPath'),
    },

    // Backend management
    backend: {
        start: (options) => ipcRenderer.invoke('backend:start', options),
        isReady: () => ipcRenderer.invoke('backend:isReady'),
    },


    // Shell operations
    shell: {
        openPath: (path) => shell.openPath(path),
    },

    // Legacy API for backward compatibility
    selectDbFile: () => ipcRenderer.invoke('dialog:openFile'),
    selectFolder: () => ipcRenderer.invoke('dialog:selectFolder'),
    loadDbFile: (filePath) => ipcRenderer.invoke('file-dropped', filePath),
    exportKml: (table, query, latCol, lonCol) =>
        ipcRenderer.invoke('export:kml', { table, query, latCol, lonCol }),
    getApiPort: () => ipcRenderer.invoke('get-api-port'),
    quitApp: () => ipcRenderer.send('app:quit')
});
