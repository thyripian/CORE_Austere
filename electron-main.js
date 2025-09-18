const fs = require('fs');
const path = require('path');
const net = require('net');
const { app, BrowserWindow, ipcMain, dialog } = require('electron');
const spawn = require('cross-spawn');
const http = require('http');

// Set AppUserModelId on Windows for notifications/jump lists
app.setAppUserModelId('com.389MIB.corescout');

let pythonProcess = null;
let mainWindow = null;
let apiPort = null;
let selectedDBPath = null; // Single source of truth for DB path
let backendReady = false; // Health gate flag
let logStream = null; // Log file stream
let isRestarting = false; // Flag to prevent double-restart
let killTimeout = null; // Store timeout ID for clearing
let isShuttingDown = false; // Flag to track app shutdown

// Path to save last-used DB
const isPackaged = app.isPackaged;
const baseDir = isPackaged ? path.join(process.resourcesPath, '..') : __dirname;
const configFile = path.join(baseDir, 'config', 'settings_lite.json');

// Logging setup
function setupLogging() {
    const logDir = path.join(app.getPath('userData'), 'logs');
    fs.mkdirSync(logDir, { recursive: true });
    const logFile = path.join(logDir, 'backend.log');
    logStream = fs.createWriteStream(logFile, { flags: 'a' });

    const timestamp = new Date().toISOString();
    logStream.write(`\n=== CORE Scout Backend Log - ${timestamp} ===\n`);
}

function logBackend(message, type = 'INFO') {
    const timestamp = new Date().toISOString();
    const logMessage = `[${timestamp}] [${type}] ${message}\n`;

    console.log(`[Backend] ${message}`);
    if (logStream) {
        logStream.write(logMessage);
    }
}

// Show backend error in UI
function showBackendError(title, message) {
    if (mainWindow && !mainWindow.isDestroyed()) {
        try {
            mainWindow.webContents.send('backend-error', {
                title: title,
                message: message,
                logPath: path.join(app.getPath('userData'), 'logs', 'backend.log')
            });
        } catch (err) {
            // Window might be destroyed during send, ignore the error
            console.log('[Backend] Could not send error to window (window destroyed)');
        }
    }
}

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
    const venvDir = path.join(baseDir, 'venv');
    if (process.platform === 'win32') {
        const exe = path.join(venvDir, 'Scripts', 'python.exe');
        return fs.existsSync(exe) ? exe : 'python';
    } else {
        const exe = path.join(venvDir, 'bin', 'python3');
        return fs.existsSync(exe) ? exe : 'python3';
    }
}

// Launch or relaunch the Python FastAPI backend on the given port
async function startPythonBackend(dbPath = null) {
    // Generate a fresh port for each restart to avoid conflicts
    apiPort = await getFreePort();
    logBackend(`Generated fresh API port: ${apiPort}`, 'INFO');

    if (pythonProcess) {
        logBackend(`Killing existing backend process (PID: ${pythonProcess.pid})`, 'INFO');

        // On Windows, use taskkill to force-kill the process
        if (process.platform === 'win32') {
            try {
                const { exec } = require('child_process');
                await new Promise((resolve) => {
                    exec(`taskkill /F /PID ${pythonProcess.pid}`, (error, stdout, stderr) => {
                        if (error) {
                            logBackend(`taskkill failed: ${error.message}`, 'WARN');
                        } else {
                            logBackend(`taskkill succeeded: ${stdout}`, 'INFO');
                        }
                        resolve(); // Always resolve, even on error
                    });
                });
            } catch (err) {
                logBackend(`taskkill error: ${err.message}`, 'ERROR');
                pythonProcess.kill('SIGTERM');
            }
        } else {
            pythonProcess.kill('SIGTERM');
        }

        // Wait for the process to actually die
        await new Promise((resolve) => {
            const checkInterval = setInterval(() => {
                if (!pythonProcess || pythonProcess.killed || pythonProcess.exitCode !== null) {
                    clearInterval(checkInterval);
                    if (killTimeout) clearTimeout(killTimeout);
                    resolve();
                }
            }, 50); // Check more frequently

            // Force kill after 2 seconds if it's still alive
            killTimeout = setTimeout(() => {
                if (pythonProcess && !pythonProcess.killed && pythonProcess.exitCode === null) {
                    logBackend(`Force killing backend process (PID: ${pythonProcess.pid})`, 'INFO');
                    pythonProcess.kill('SIGKILL');
                }
                clearInterval(checkInterval);
                resolve();
            }, 2000);
        });

        pythonProcess = null;
        backendReady = false;

        // Give the OS more time to release the port
        await new Promise(resolve => setTimeout(resolve, 1000));
    }

    // Start the new process after the old one is dead
    (() => {
        let backendExePath;
        let workingDir;

        if (isPackaged) {
            // For packaged app, resolve backend EXE path
            backendExePath = path.join(process.resourcesPath, 'backend', 'core-scout-backend.exe');
            workingDir = path.join(process.resourcesPath, 'backend');

            logBackend(`Running in packaged mode`, 'INFO');
            logBackend(`Resolved backend EXE path: ${backendExePath}`, 'INFO');
            logBackend(`Working directory: ${workingDir}`, 'INFO');
            logBackend(`File exists check: ${fs.existsSync(backendExePath)}`, 'INFO');

            if (!fs.existsSync(backendExePath)) {
                const errorMsg = `Backend EXE not found at: ${backendExePath}`;
                logBackend(errorMsg, 'ERROR');
                showBackendError('Backend executable not found', errorMsg);
                return;
            }
        } else {
            // For development mode, use the batch file approach
            const batchFile = path.join(baseDir, 'start_backend.bat');
            logBackend(`Running in development mode, using batch file: ${batchFile}`, 'INFO');
            logBackend(`Working directory: ${baseDir}`, 'INFO');

            if (!fs.existsSync(batchFile)) {
                logBackend(`Batch file not found at: ${batchFile}`, 'ERROR');
                return;
            }

            // Update the batch file with the correct database path and port
            const dbArg = dbPath ? `--db "${dbPath}"` : '';
            const batchContent = `@echo off
cd /d "%~dp0"
set API_PORT=${apiPort}
set DB_PATH=${dbPath || ''}
venv\\Scripts\\python.exe run_app_dynamic.py ${dbArg} --port ${apiPort}
pause`;

            try {
                fs.writeFileSync(batchFile, batchContent, 'utf-8');
                logBackend(`Updated batch file with DB: ${dbPath}, Port: ${apiPort}`, 'INFO');
            } catch (err) {
                logBackend(`Failed to update batch file: ${err.message}`, 'ERROR');
                return;
            }

            pythonProcess = spawn('cmd', ['/c', batchFile], {
                cwd: baseDir,
                stdio: 'pipe',
                shell: true,
                env: {
                    ...process.env,
                    API_PORT: apiPort.toString(),
                    DB_PATH: dbPath || ''
                }
            });
        }

        if (isPackaged) {
            // Spawn the packaged EXE with proper environment
            const env = {
                ...process.env,
                API_PORT: apiPort.toString(),
                DB_PATH: dbPath || ''
            };

            logBackend(`Spawning backend with environment:`, 'INFO');
            logBackend(`  API_PORT: ${apiPort}`, 'INFO');
            logBackend(`  DB_PATH: ${dbPath || 'none'}`, 'INFO');
            logBackend(`  Working directory: ${workingDir}`, 'INFO');

            try {
                // Pass API_PORT explicitly as a CLI arg to avoid any env/default mismatch
                const spawnArgs = ['--port', String(apiPort)];
                if (dbPath) spawnArgs.push('--db', dbPath);
                logBackend(`Spawning command: ${backendExePath} ${spawnArgs.join(' ')}`, 'INFO');
                pythonProcess = spawn(backendExePath, spawnArgs, {
                    cwd: workingDir,
                    stdio: 'pipe',
                    shell: false,
                    env: env
                });
            } catch (err) {
                const errorMsg = `Failed to spawn backend process: ${err.message}`;
                logBackend(errorMsg, 'ERROR');
                showBackendError('Failed to start backend', errorMsg);
                return;
            }
        }

        // Handle process events
        pythonProcess.on('error', err => {
            const errorMsg = `Backend process error: ${err.message}`;
            logBackend(errorMsg, 'ERROR');
            logBackend(`Failed to start backend with database: ${dbPath}`, 'ERROR');
            logBackend(`Working directory: ${isPackaged ? workingDir : baseDir}`, 'ERROR');
            backendReady = false;
            showBackendError('Backend process error', errorMsg);
        });

        pythonProcess.on('exit', (code, signal) => {
            const exitMsg = `Backend process exited with code ${code}${signal ? `, signal ${signal}` : ''}`;
            logBackend(exitMsg, code === 0 ? 'INFO' : 'ERROR');

            // Only show error if it's not a normal shutdown (SIGTERM/SIGKILL) and app is still running
            if (code !== 0 && code !== null && !isShuttingDown) {
                const errorMsg = `Backend failed to start properly - exit code: ${code}`;
                logBackend(errorMsg, 'ERROR');
                showBackendError('Backend startup failed', errorMsg);
            }
            backendReady = false;
            isRestarting = false; // Reset restart flag when process exits
        });

        pythonProcess.on('spawn', () => {
            logBackend(`Backend process spawned successfully (PID: ${pythonProcess.pid})`, 'INFO');
            isRestarting = false; // Reset restart flag when process actually spawns
            if (killTimeout) {
                clearTimeout(killTimeout); // Clear any pending kill timeout
                killTimeout = null;
            }
        });

        // Capture stdout and stderr
        if (pythonProcess.stdout) {
            pythonProcess.stdout.on('data', (data) => {
                const output = data.toString().trim();
                if (output) {
                    logBackend(`STDOUT: ${output}`, 'DEBUG');
                }
            });
        }

        if (pythonProcess.stderr) {
            pythonProcess.stderr.on('data', (data) => {
                const output = data.toString().trim();
                if (output) {
                    logBackend(`STDERR: ${output}`, 'ERROR');
                }
            });
        }

        // Start enhanced health check with exponential backoff
        checkBackendHealthWithRetry();
    })();
}

// Enhanced health check with exponential backoff
function checkBackendHealthWithRetry() {
    const maxAttempts = 20; // 20 attempts total
    const maxTimeout = 20000; // 20 seconds max
    let attempts = 0;
    let startTime = Date.now();

    const checkHealth = () => {
        attempts++;
        const elapsed = Date.now() - startTime;

        // Calculate exponential backoff delay (300ms to 2000ms)
        const baseDelay = 300;
        const maxDelay = 2000;
        const delay = Math.min(baseDelay * Math.pow(1.5, attempts - 1), maxDelay);

        logBackend(`Health check attempt ${attempts}/${maxAttempts} (elapsed: ${elapsed}ms)`, 'DEBUG');

        const options = {
            hostname: '127.0.0.1',
            port: apiPort,
            path: '/health',
            method: 'GET',
            timeout: 3000
        };

        const req = http.request(options, (res) => {
            if (res.statusCode === 200) {
                logBackend(`Backend is ready! Responding on port ${apiPort} after ${attempts} attempts`, 'INFO');
                backendReady = true;

                // Notify renderer that backend is ready
                if (mainWindow) {
                    mainWindow.webContents.send('backend-ready', {
                        port: apiPort,
                        dbPath: selectedDBPath
                    });
                }
            } else {
                logBackend(`Backend responded with status ${res.statusCode}, retrying in ${delay}ms...`, 'WARN');
                if (attempts < maxAttempts && elapsed < maxTimeout) {
                    setTimeout(checkHealth, delay);
                } else {
                    const errorMsg = `Backend health check failed after ${attempts} attempts (${elapsed}ms)`;
                    logBackend(errorMsg, 'ERROR');
                    backendReady = false;
                    showBackendError('Backend startup timeout', errorMsg);
                }
            }
        });

        req.on('error', (err) => {
            logBackend(`Health check error: ${err.message}, retrying in ${delay}ms...`, 'DEBUG');
            if (attempts < maxAttempts && elapsed < maxTimeout) {
                setTimeout(checkHealth, delay);
            } else {
                const errorMsg = `Backend health check failed after ${attempts} attempts (${elapsed}ms)`;
                logBackend(errorMsg, 'ERROR');
                backendReady = false;
                showBackendError('Backend connection failed', errorMsg);
            }
        });

        req.on('timeout', () => {
            logBackend(`Health check timed out, retrying in ${delay}ms...`, 'DEBUG');
            req.destroy();
            if (attempts < maxAttempts && elapsed < maxTimeout) {
                setTimeout(checkHealth, delay);
            } else {
                const errorMsg = `Backend health check failed after ${attempts} attempts (${elapsed}ms)`;
                logBackend(errorMsg, 'ERROR');
                backendReady = false;
                showBackendError('Backend startup timeout', errorMsg);
            }
        });

        req.end();
    };

    // Start checking after a short delay
    setTimeout(checkHealth, 1000);
}

// Legacy function for backward compatibility
function checkBackendHealth() {
    checkBackendHealthWithRetry();
}

// Legacy function for backward compatibility
async function verifyBackendHealth() {
    return new Promise((resolve) => {
        const options = {
            hostname: '127.0.0.1',
            port: apiPort,
            path: '/health',
            method: 'GET',
            timeout: 5000
        };

        const req = http.request(options, (res) => {
            logBackend(`Backend is responding on port ${apiPort} - status: ${res.statusCode}`, 'INFO');
            resolve(res.statusCode === 200);
        });

        req.on('error', (err) => {
            logBackend(`Backend not responding on port ${apiPort}: ${err.message}`, 'ERROR');
            resolve(false);
        });

        req.on('timeout', () => {
            logBackend(`Backend health check timed out on port ${apiPort}`, 'ERROR');
            req.destroy();
            resolve(false);
        });

        req.end();
    });
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
            devTools: !isPackaged, // Disable dev tools in production
        },
        show: false, // Don't show until ready
    });

    // Remove menu bar in production
    if (isPackaged) {
        mainWindow.setMenuBarVisibility(false);
        mainWindow.setMenu(null);
    }

    // Load the React frontend - different paths for dev vs packaged
    const frontendPath = isPackaged
        ? path.join(process.resourcesPath, 'app.asar', 'user_interface', 'build', 'index.html')
        : path.join(__dirname, 'user_interface', 'build', 'index.html');

    console.log('[Electron] Loading frontend from:', frontendPath);
    mainWindow.loadFile(frontendPath);

    // Show window when ready to prevent flash
    mainWindow.once('ready-to-show', () => {
        mainWindow.show();
    });

    // Disable dev tools shortcuts in production
    if (isPackaged) {
        mainWindow.webContents.on('before-input-event', (event, input) => {
            // Disable F12, Ctrl+Shift+I, Ctrl+Shift+J, Ctrl+U
            if (input.key === 'F12' ||
                (input.control && input.shift && input.key === 'I') ||
                (input.control && input.shift && input.key === 'J') ||
                (input.control && input.key === 'U')) {
                event.preventDefault();
            }
        });
    }
}

app.whenReady().then(async () => {
    // 1) Setup logging
    setupLogging();
    logBackend('CORE Scout starting up', 'INFO');

    // Port will be generated when backend starts

    // 3) Load saved database path
    selectedDBPath = loadLastDb();
    if (selectedDBPath) {
        logBackend(`Loaded saved DB path: ${selectedDBPath}`, 'INFO');
    }

    // 2) Set up IPC handlers BEFORE creating the window
    ipcMain.handle('get-api-port', () => {
        console.log(`[IPC] get-api-port called, returning port: ${apiPort}`);
        console.log(`[IPC] Current apiPort variable: ${apiPort}`);
        console.log(`[IPC] Type of apiPort: ${typeof apiPort}`);
        return apiPort;
    });

    // IPC: Dialog to open file
    ipcMain.handle('dialog:openFile', async () => {
        console.log('[IPC] dialog:openFile triggered');
        const { canceled, filePaths } = await dialog.showOpenDialog({
            title: 'Select SQLite Database',
            properties: ['openFile'],
            filters: [
                { name: 'SQLite Database', extensions: ['db', 'sqlite', 'sqlite3'] }
            ],
        });
        if (canceled || filePaths.length === 0) {
            console.log('[IPC] dialog:openFile canceled');
            return null;
        }
        const dbPath = filePaths[0];
        console.log('[IPC] dialog:openFile got DB path →', dbPath);
        return dbPath;
    });

    // IPC: Set database path (for drag & drop)
    ipcMain.handle('db:setPath', async (event, absPath) => {
        console.log('[IPC] db:setPath called with:', absPath);
        console.log('[IPC] db:setPath - Current pythonProcess PID:', pythonProcess?.pid);
        console.log('[IPC] db:setPath - isRestarting flag:', isRestarting);
        if (!absPath) {
            selectedDBPath = null;
            return { success: false, error: 'No path provided' };
        }

        // Validate file extension
        const validExtensions = ['.db', '.sqlite', '.sqlite3'];
        const ext = path.extname(absPath).toLowerCase();
        if (!validExtensions.includes(ext)) {
            return { success: false, error: 'Invalid file extension. Must be .db, .sqlite, or .sqlite3' };
        }

        // Validate file exists
        if (!fs.existsSync(absPath)) {
            return { success: false, error: 'File does not exist' };
        }

        const oldPath = selectedDBPath;
        selectedDBPath = absPath;
        saveLastDb(absPath);
        console.log('[IPC] db:setPath set to:', selectedDBPath);

        // Always restart backend when database path is set (first time or change)
        if (!isRestarting) {
            logBackend(`Database set to ${absPath}. Starting/restarting backend...`, 'INFO');
            isRestarting = true;
            backendReady = false;
            startPythonBackend(absPath);
            return { success: true, path: selectedDBPath, restarted: true };
        }

        return { success: true, path: selectedDBPath, restarted: false };
    });

    // IPC: Get current database path
    ipcMain.handle('db:getPath', () => {
        console.log('[IPC] db:getPath called, returning:', selectedDBPath);
        return selectedDBPath;
    });

    // IPC: Check if backend is ready
    ipcMain.handle('backend:isReady', () => {
        console.log('[IPC] backend:isReady called, returning:', backendReady);
        return backendReady;
    });


    // IPC: Start backend with current DB path
    ipcMain.handle('backend:start', async (event, options = {}) => {
        console.log('[IPC] backend:start called with options:', options);

        // Allow backend to start without database for basic endpoints like /supported-formats
        if (!selectedDBPath && !options.allowNoDatabase) {
            return {
                success: false,
                error: 'No database path set. Please select a database first.',
                status: 'no_db_path'
            };
        }

        if (pythonProcess) {
            if (isRestarting) {
                console.log('[IPC] backend:start - backend is restarting, please wait');
                return {
                    success: true,
                    status: 'restarting',
                    db: selectedDBPath,
                    pid: pythonProcess.pid
                };
            }

            console.log('[IPC] backend:start - backend already running');
            return {
                success: true,
                status: 'already_running',
                db: selectedDBPath,
                pid: pythonProcess.pid
            };
        }

        try {
            await startPythonBackend(selectedDBPath);
            // Give it a moment to start
            await new Promise(resolve => setTimeout(resolve, 2000));

            return {
                success: true,
                status: 'started',
                db: selectedDBPath,
                pid: pythonProcess?.pid
            };
        } catch (error) {
            console.error('[IPC] backend:start error:', error);
            return {
                success: false,
                error: error.message,
                status: 'failed'
            };
        }
    });

    // IPC: Folder selection for database creation
    ipcMain.handle('dialog:selectFolder', async () => {
        console.log('[IPC] dialog:selectFolder triggered');
        const { canceled, filePaths } = await dialog.showOpenDialog({
            title: 'Select Folder to Process',
            properties: ['openDirectory'],
        });
        if (canceled || filePaths.length === 0) {
            console.log('[IPC] dialog:selectFolder canceled');
            return null;
        }
        const folderPath = filePaths[0];
        console.log('[IPC] dialog:selectFolder got folder path →', folderPath);
        return folderPath;
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
        isShuttingDown = true;
        if (pythonProcess) pythonProcess.kill();
        app.quit();
    });

    // 8) Finally create the window AFTER everything is set up
    createWindow();

    app.on('activate', () => {
        if (BrowserWindow.getAllWindows().length === 0) createWindow();
    });
});

// Clean shutdown on all windows closed
app.on('window-all-closed', () => {
    isShuttingDown = true;
    if (pythonProcess) pythonProcess.kill();
    if (process.platform !== 'darwin') app.quit();
});
