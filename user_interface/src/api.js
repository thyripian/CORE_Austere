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
