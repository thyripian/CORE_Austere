import React, { useState, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import '../styles/SettingsComponent.css';

export default function SettingsComponent() {
  const [status, setStatus] = useState('No database loaded.');
  const [isLoading, setIsLoading] = useState(false);
  const navigate = useNavigate();

  // Drag & drop handler
  const onDrop = useCallback(async (e) => {
    console.log('Drop event triggered', e);
    e.preventDefault();
    e.stopPropagation();

    const file = e.dataTransfer.files[0];
    console.log('Dropped file:', file);

    if (!file) return;

    const validExtensions = ['.db', '.sqlite', '.sqlite3'];
    const ext = file.name.toLowerCase().substring(file.name.lastIndexOf('.'));

    if (!validExtensions.includes(ext)) {
      setStatus('⛔ Please drop a valid SQLite database file (.db, .sqlite, or .sqlite3).');
      return;
    }

    await handleFileSelection(file.path);
  }, []);

  const onDragOver = useCallback((e) => {
    e.preventDefault();
    e.stopPropagation();
  }, []);

  const onDragLeave = useCallback((e) => {
    e.preventDefault();
    e.stopPropagation();
  }, []);

  // Handle file selection (both drag & drop and browse)
  const handleFileSelection = useCallback(async (filePath) => {
    setIsLoading(true);
    setStatus('🔄 Loading database...');

    try {
      // Set the database path via IPC
      const result = await window.electronAPI.db.setPath(filePath);

      if (!result.success) {
        setStatus(`❌ ${result.error}`);
        return;
      }

      // Start the backend with the selected database
      const backendResult = await window.electronAPI.backend.start();

      if (!backendResult.success) {
        setStatus(`❌ ${backendResult.error}`);
        return;
      }

      setStatus(`✅ Database loaded: ${filePath.split('\\').pop()}`);

      // Notify other components that database has changed
      window.dispatchEvent(new CustomEvent('databaseChanged', {
        detail: { dbPath: filePath }
      }));

      // Navigate to search page after a short delay
      setTimeout(() => {
        navigate('/search');
      }, 1500);

    } catch (err) {
      console.error('Error selecting database:', err);
      setStatus(`❗ Error: ${err.message}`);
    } finally {
      setIsLoading(false);
    }
  }, [navigate]);

  // Browse… button & box click both call this
  const handleBrowse = useCallback(async () => {
    try {
      const returned = await window.electronAPI.dialog.openFile();
      if (returned) {
        await handleFileSelection(returned);
      }
    } catch (err) {
      console.error('Error opening file dialog:', err);
      setStatus(`❗ Error: ${err.message}`);
    }
  }, [handleFileSelection]);

  return (
    <div className="settings-container">
      <h2>Load SQLite Database</h2>

      <div
        className={`upload-box ${isLoading ? 'loading' : ''}`}
        onDrop={onDrop}
        onDragOver={onDragOver}
        onDragLeave={onDragLeave}
        onClick={handleBrowse}
      >
        {isLoading ? (
          <div className="loading-content">
            <div className="spinner"></div>
            <p>Loading database...</p>
          </div>
        ) : (
          <>
            <p>Drag &amp; drop a database file here</p>
            <p>or click to browse …</p>
            <div className="supported-formats">
              <small>Supported: .db, .sqlite, .sqlite3</small>
            </div>
          </>
        )}
      </div>

      <button
        className="select-db-button"
        onClick={handleBrowse}
        disabled={isLoading}
      >
        Browse…
      </button>

      <p className="status-text">{status}</p>
    </div>
  );
}
