// src/api.js
import axios from 'axios';

async function getPort() {
    // Ask Electron for the real port
    return await window.electronAPI.getApiPort();
}

// A wrapper around axios that uses the dynamic port
export async function apiGet(path, params = {}) {
    const port = await getPort();
    const url = `http://127.0.0.1:${port}${path}`;
    return axios.get(url, { params });
}

// Enhanced API functions for dynamic schema support
export async function apiPost(path, data = {}) {
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
    return apiGet('/tables');
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