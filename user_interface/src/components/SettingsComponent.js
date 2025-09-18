import React, { useState, useCallback } from 'react';
import '../styles/SettingsComponent.css';

export default function SettingsComponent() {
  const [status, setStatus] = useState('No database loaded.');
  const [isLoading, setIsLoading] = useState(false);
  const [isProcessing, setIsProcessing] = useState(false); // Prevent double calls

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

    // Prevent click event from firing after drop
    e.target.style.pointerEvents = 'none';
    setTimeout(() => {
      e.target.style.pointerEvents = 'auto';
    }, 100);

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
    if (isProcessing) {
      console.log('Already processing a file, ignoring duplicate call');
      return;
    }

    setIsProcessing(true);
    setIsLoading(true);
    setStatus('🔄 Loading database...');

    try {
      // Set the database path via IPC
      const result = await window.electronAPI.db.setPath(filePath);

      if (!result.success) {
        setStatus(`❌ ${result.error}`);
        return;
      }

      // Backend should always be restarted by db:setPath, so just wait for it to be ready
      setStatus(`🔄 Loading database... Please wait...`);

      // Wait for backend to be ready
      let attempts = 0;
      const maxAttempts = 20; // 10 seconds
      while (attempts < maxAttempts) {
        const isReady = await window.electronAPI.backend.isReady();
        if (isReady) break;
        await new Promise(resolve => setTimeout(resolve, 500));
        attempts++;
      }

      if (attempts >= maxAttempts) {
        setStatus(`❌ Backend failed to start properly`);
        return;
      }

      setStatus(`✅ Database loaded: ${filePath.split('\\').pop()}`);

      // Notify other components that database has changed
      window.dispatchEvent(new CustomEvent('databaseChanged', {
        detail: { dbPath: filePath }
      }));

      // Database loaded successfully - user can navigate manually

    } catch (err) {
      console.error('Error selecting database:', err);
      setStatus(`❗ Error: ${err.message}`);
    } finally {
      setIsLoading(false);
      setIsProcessing(false);
    }
  }, [isProcessing]);

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
