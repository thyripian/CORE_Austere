// src/api.js
import axios from 'axios';

async function getPort() {
    // Wait for Electron API to be available with retries
    let attempts = 0;
    const maxAttempts = 10;

    while (attempts < maxAttempts) {
        try {
            if (!window.electronAPI) {
                console.log(`[API] Electron API not available, attempt ${attempts + 1}/${maxAttempts}`);
                await new Promise(resolve => setTimeout(resolve, 500));
                attempts++;
                continue;
            }

            const port = await window.electronAPI.getApiPort();
            console.log(`[API] Retrieved port from Electron: ${port}`);

            if (!port) {
                throw new Error('No port received from Electron');
            }

            return port;
        } catch (error) {
            console.log(`[API] Error getting port from Electron (attempt ${attempts + 1}/${maxAttempts}):`, error.message);
            attempts++;

            if (attempts >= maxAttempts) {
                throw new Error(`Failed to get port from Electron after ${maxAttempts} attempts: ${error.message}`);
            }

            await new Promise(resolve => setTimeout(resolve, 500));
        }
    }
}

async function waitForBackendReady() {
    // Wait for backend to be ready with retries
    let attempts = 0;
    const maxAttempts = 20; // 10 seconds with 500ms intervals

    while (attempts < maxAttempts) {
        try {
            if (!window.electronAPI) {
                console.log(`[API] Electron API not available, attempt ${attempts + 1}/${maxAttempts}`);
                await new Promise(resolve => setTimeout(resolve, 500));
                attempts++;
                continue;
            }

            const isReady = await window.electronAPI.backend.isReady();
            console.log(`[API] Backend ready check: ${isReady} (attempt ${attempts + 1}/${maxAttempts})`);

            if (isReady) {
                console.log(`[API] Backend is ready!`);
                return true;
            }

            attempts++;
            if (attempts < maxAttempts) {
                await new Promise(resolve => setTimeout(resolve, 500));
            }
        } catch (error) {
            console.log(`[API] Error checking backend readiness (attempt ${attempts + 1}/${maxAttempts}):`, error.message);
            attempts++;

            if (attempts >= maxAttempts) {
                throw new Error(`Backend not ready after ${maxAttempts} attempts: ${error.message}`);
            }

            await new Promise(resolve => setTimeout(resolve, 500));
        }
    }

    throw new Error(`Backend not ready after ${maxAttempts} attempts`);
}

// A wrapper around axios that uses the dynamic port
export async function apiGet(path, params = {}) {
    await waitForBackendReady();
    const port = await getPort();
    const url = `http://127.0.0.1:${port}${path}`;
    console.log(`[API] Making request to: ${url}`);
    return axios.get(url, { params });
}

// Retry wrapper for API calls
export async function apiGetWithRetry(path, params = {}, maxRetries = 3) {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            return await apiGet(path, params);
        } catch (error) {
            console.log(`[API] Attempt ${attempt} failed:`, error.message);
            if (attempt === maxRetries) {
                throw error;
            }
            // Wait before retrying
            await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
        }
    }
}

// Enhanced API functions for dynamic schema support
export async function apiPost(path, data = {}) {
    await waitForBackendReady();
    const port = await getPort();
    const url = `http://127.0.0.1:${port}${path}`;
    return axios.post(url, data);
}

// Get database schema information
export async function getSchema() {
    return apiGet('/schema');
}

// Get all tables with metadata
export async function getTables() {
    return apiGetWithRetry('/tables');
}

// Get table details
export async function getTableInfo(tableName) {
    return apiGet(`/tables/${tableName}`);
}

// Get table fields
export async function getTableFields(tableName) {
    return apiGet(`/tables/${tableName}/fields`);
}

// Elasticsearch-like search
export async function searchTable(tableName, searchRequest) {
    return apiPost(`/search/${tableName}`, searchRequest);
}

// Simple search (backward compatibility)
export async function simpleSearch(tableName, query, options = {}) {
    const params = {
        q: query,
        ...options
    };
    return apiGet(`/search/${tableName}`, params);
}

// Export KML
export async function exportKML(tableName, query, mgrsField, limit) {
    const params = {
        q: query,
        mgrs_field: mgrsField,
        limit: limit
    };
    return apiGet(`/export/kml/${tableName}`, params);
}

// Create FTS index
export async function createFTSIndex(tableName, fields) {
    return apiPost(`/tables/${tableName}/fts`, fields);
}

// Health check
export async function healthCheck() {
    return apiGet('/health');
}

// Database stats
export async function getDatabaseStats() {
    return apiGet('/stats');
}

// Database creation functions
export async function getSupportedFormats() {
    return apiGet('/supported-formats');
}

export async function createDatabase(folderPath, dbName, options = {}) {
    const data = {
        folderPath: folderPath,
        dbName: dbName,
        options: JSON.stringify(options)
    };
    return apiPost('/create-database', data);
}

export async function switchDatabase(dbPath) {
    const data = { dbPath: dbPath };
    return apiPost('/switch-database', data);
}