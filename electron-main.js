const fs = require('fs');
const path = require('path');
const net = require('net');
const { app, BrowserWindow, ipcMain, dialog } = require('electron');
const spawn = require('cross-spawn');

// Set AppUserModelId on Windows for notifications/jump lists
app.setAppUserModelId('com.389MIB.corescout');

let pythonProcess = null;
let mainWindow = null;
let apiPort = null;

// Path to save last-used DB
const configFile = path.join(__dirname, 'config', 'settings_lite.json');

// Save the chosen DB path for next launch
function saveLastDb(dbPath) {
    try {
        fs.mkdirSync(path.dirname(configFile), { recursive: true });
        fs.writeFileSync(configFile, JSON.stringify({ dbPath }), 'utf-8');
    } catch (e) {
        console.error('Failed to save DB path:', e);
    }
}

// Load the saved DB path (if any)
function loadLastDb() {
    try {
        const data = fs.readFileSync(configFile, 'utf-8');
        return JSON.parse(data).dbPath;
    } catch {
        return null;
    }
}

// Ask the OS for a free ephemeral port
function getFreePort() {
    return new Promise((resolve, reject) => {
        const srv = net.createServer();
        srv.unref();
        srv.on('error', reject);
        srv.listen(0, () => {
            const port = srv.address().port;
            srv.close(() => resolve(port));
        });
    });
}

// Prefer venv python, else fallback to system
function getPythonExecutable() {
    const venvDir = path.join(__dirname, 'venv');
    if (process.platform === 'win32') {
        const exe = path.join(venvDir, 'Scripts', 'python.exe');
        return fs.existsSync(exe) ? exe : 'python';
    } else {
        const exe = path.join(venvDir, 'bin', 'python3');
        return fs.existsSync(exe) ? exe : 'python3';
    }
}

// Launch or relaunch the Python FastAPI backend on the given port
function startPythonBackend(dbPath) {
    if (pythonProcess) {
        pythonProcess.kill();
        pythonProcess = null;
    }
    const pythonExe = getPythonExecutable();
    const script = path.join(__dirname, 'run_app.py');
    const args = ['--db', dbPath, '--port', String(apiPort)];
    console.log(`[Spawn] ${pythonExe} ${script} ${args.join(' ')}`);

    pythonProcess = spawn(pythonExe, [script, ...args], {
        cwd: __dirname,
        stdio: 'inherit'
    });

    pythonProcess.on('error', err => console.error('Python error:', err));
    pythonProcess.on('exit', code => console.log(`Python exited ${code}`));
}

// Create the Electron BrowserWindow and load the React build
function createWindow() {
    mainWindow = new BrowserWindow({
        width: 1200,
        height: 800,
        title: 'Scout',
        icon: path.join(__dirname, 'assets', 'app-icon.ico'),
        webPreferences: {
            preload: path.join(__dirname, 'preload.js'),
            contextIsolation: true,
            nodeIntegration: false,
        },
    });
    mainWindow.webContents.openDevTools();
    mainWindow.loadFile(
        path.join(__dirname, 'user_interface', 'build', 'index.html')
    );
}

app.whenReady().then(async () => {
    // 1) Pick a free port
    apiPort = await getFreePort();
    console.log(`Chosen API port: ${apiPort}`);

    // 2) Auto-start last DB if present
    const lastDb = loadLastDb();
    if (lastDb) startPythonBackend(lastDb);

    // 3) IPC: Provide the chosen port to renderer
    ipcMain.handle('get-api-port', () => apiPort);

    // 4) IPC: Browse… dialog
    ipcMain.handle('dialog:openFile', async () => {
        console.log('[IPC] dialog:openFile triggered');
        const { canceled, filePaths } = await dialog.showOpenDialog({
            title: 'Select SQLite Database',
            properties: ['openFile'],
            filters: [{ name: 'SQLite Database', extensions: ['db'] }],
        });
        if (canceled || filePaths.length === 0) {
            console.log('[IPC] dialog:openFile canceled');
            return null;
        }
        const dbPath = filePaths[0];
        console.log('[IPC] dialog:openFile got DB path →', dbPath);
        startPythonBackend(dbPath);
        saveLastDb(dbPath);
        return dbPath;
    });

    // 5) IPC: Drag‐and‐drop
    ipcMain.handle('file-dropped', async (_e, filePath) => {
        console.log('[IPC] file-dropped triggered with →', filePath);
        if (filePath?.toLowerCase().endsWith('.db')) {
            startPythonBackend(filePath);
            saveLastDb(filePath);
            console.log('[IPC] file-dropped launching backend on →', filePath);
            return filePath;
        }
        console.log('[IPC] file-dropped invalid extension');
        return null;
    });

    // 6) IPC: Export KML/KMZ
    ipcMain.handle('export:kml', async (_e, { table, query, mgrs_col, limit }) => {
        const { canceled, filePath } = await dialog.showSaveDialog({
            title: 'Save KMZ',
            defaultPath: `${table}.kmz`,
            filters: [{ name: 'KMZ File', extensions: ['kmz'] }],
        });
        if (canceled || !filePath) return null;
        const resp = await fetch(
            `http://127.0.0.1:${apiPort}` +
            `/export/kml/${encodeURIComponent(table)}` +
            `?query=${encodeURIComponent(query)}` +
            `&mgrs_col=${encodeURIComponent(mgrs_col)}` +
            `&limit=${encodeURIComponent(limit)}`
        );
        const buffer = await resp.arrayBuffer();
        fs.writeFileSync(filePath, Buffer.from(buffer));
        return filePath;
    });

    // 7) IPC: Quit
    ipcMain.on('app:quit', () => {
        if (pythonProcess) pythonProcess.kill();
        app.quit();
    });

    // 8) Finally create the window
    createWindow();

    app.on('activate', () => {
        if (BrowserWindow.getAllWindows().length === 0) createWindow();
    });
});

// Clean shutdown on all windows closed
app.on('window-all-closed', () => {
    if (pythonProcess) pythonProcess.kill();
    if (process.platform !== 'darwin') app.quit();
});
