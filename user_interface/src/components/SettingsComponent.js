import React, { useState, useCallback } from 'react';
import '../styles/SettingsComponent.css';

export default function SettingsComponent() {
  const [status, setStatus] = useState('No database loaded.');

  // Drag & drop handler
  const onDrop = useCallback(async (e) => {
    e.preventDefault();
    const file = e.dataTransfer.files[0];
    if (!file || !file.path.toLowerCase().endsWith('.db')) {
      setStatus('⛔ Please drop a valid .db file.');
      return;
    }
    try {
      const returned = await window.electronAPI.loadDbFile(file.path);
      if (returned) {
        setStatus(`✅ Loaded database: ${returned}`);
      } else {
        setStatus('⛔ Load cancelled or invalid .db file.');
      }
    } catch (err) {
      setStatus(`❗ Error: ${err.message}`);
    }
  }, []);

  const onDragOver = useCallback((e) => {
    e.preventDefault();
  }, []);

  // Browse… button & box click both call this
  const handleBrowse = useCallback(async () => {
    try {
      const returned = await window.electronAPI.selectDbFile();
      if (returned && returned.toLowerCase().endsWith('.db')) {
        setStatus(`✅ Loaded database: ${returned}`);
      } else {
        setStatus('⛔ Please select a valid .db file. ⛔');
      }
    } catch (err) {
      setStatus(`❗ Error: ${err.message}`);
    }
  }, []);

  return (
    <div className="settings-container">
      <h2>Load SQLite Database</h2>

      <div
        className="upload-box"
        onDrop={onDrop}
        onDragOver={onDragOver}
        onClick={handleBrowse}
      >
        <p>Drag &amp; drop a <code>.db</code> here</p>
        <p>or click to browse …</p>
      </div>

      <button className="select-db-button" onClick={handleBrowse}>
        Browse…
      </button>

      <p className="status-text">{status}</p>
    </div>
  );
}
