// src/components/DbCreatorComponent.js
import React, { useState, useCallback } from 'react';
import { apiGet } from '../api';
import '../styles/DbCreatorComponent.css';

export default function DbCreatorComponent() {
  const [selectedFolder, setSelectedFolder] = useState('');
  const [dbName, setDbName] = useState('');
  const [status, setStatus] = useState('No folder selected.');
  const [isProcessing, setIsProcessing] = useState(false);
  const [progress, setProgress] = useState({ current: 0, total: 0, currentFile: '' });
  const [supportedFormats, setSupportedFormats] = useState([]);
  const [processingOptions, setProcessingOptions] = useState({
    extractText: true,
    extractCoordinates: true,
    includeImages: false,
    recursive: true,
    fileTypes: ['pdf', 'txt', 'kml', 'kmz', 'doc', 'docx']
  });

  // Fetch supported file formats on component mount
  React.useEffect(() => {
    fetchSupportedFormats();
  }, []);

  const fetchSupportedFormats = async () => {
    try {
      const response = await apiGet('/supported-formats');
      setSupportedFormats(response.data);
    } catch (err) {
      console.error('Error fetching supported formats:', err);
      // Fallback to default formats
      setSupportedFormats(['pdf', 'txt', 'kml', 'kmz', 'doc', 'docx', 'md']);
    }
  };

  // Folder selection handler
  const handleFolderSelect = useCallback(async () => {
    // Check if we're running in Electron
    if (!window.electronAPI) {
      setStatus('⛔ Folder selection only works in the Electron app.');
      return;
    }

    try {
      const folderPath = await window.electronAPI.selectFolder();
      if (folderPath) {
        setSelectedFolder(folderPath);
        setStatus(`✅ Selected folder: ${folderPath}`);
        
        // Auto-generate DB name from folder name
        const folderName = folderPath.split(/[/\\]/).pop();
        setDbName(`${folderName}_database.db`);
      } else {
        setStatus('⛔ Folder selection cancelled.');
      }
    } catch (err) {
      setStatus(`❗ Error selecting folder: ${err.message}`);
    }
  }, []);

  // Drag & drop handler for folders
  const onDrop = useCallback(async (e) => {
    e.preventDefault();
    
    if (!window.electronAPI) {
      setStatus('⛔ Drag & drop only works in the Electron app.');
      return;
    }

    const items = e.dataTransfer.items;
    if (items && items.length > 0) {
      const item = items[0];
      if (item.kind === 'file') {
        const entry = item.webkitGetAsEntry();
        if (entry && entry.isDirectory) {
          const folderPath = entry.fullPath;
          setSelectedFolder(folderPath);
          setStatus(`✅ Selected folder: ${folderPath}`);
          
          // Auto-generate DB name from folder name
          const folderName = folderPath.split(/[/\\]/).pop();
          setDbName(`${folderName}_database.db`);
        } else {
          setStatus('⛔ Please drop a folder, not a file.');
        }
      }
    }
  }, []);

  const onDragOver = useCallback((e) => {
    e.preventDefault();
  }, []);

  // Start database creation process
  const handleCreateDatabase = useCallback(async () => {
    if (!selectedFolder || !dbName) {
      setStatus('⛔ Please select a folder and enter a database name.');
      return;
    }

    setIsProcessing(true);
    setProgress({ current: 0, total: 0, currentFile: '' });
    setStatus('🚀 Starting database creation...');

    try {
      // Start the processing
      const response = await apiGet('/create-database', {
        folderPath: selectedFolder,
        dbName: dbName,
        options: processingOptions
      });

      if (response.data.success) {
        setStatus(`✅ Database created successfully: ${response.data.dbPath}`);
        setProgress({ current: response.data.filesProcessed, total: response.data.totalFiles, currentFile: 'Complete!' });
        
        // Notify other components that database has changed
        window.dispatchEvent(new CustomEvent('databaseChanged', {
          detail: { dbPath: response.data.dbPath }
        }));
        
        // Show success message with link to search
        setTimeout(() => {
          setStatus(`✅ Database ready! Go to Search to query your ${response.data.filesProcessed} processed files.`);
        }, 2000);
      } else {
        setStatus(`❌ Failed to create database: ${response.data.error}`);
      }
    } catch (err) {
      setStatus(`❌ Error creating database: ${err.message}`);
    } finally {
      setIsProcessing(false);
    }
  }, [selectedFolder, dbName, processingOptions]);

  // Handle processing options changes
  const handleOptionChange = (option, value) => {
    setProcessingOptions(prev => ({
      ...prev,
      [option]: value
    }));
  };

  const handleFileTypeToggle = (fileType) => {
    setProcessingOptions(prev => ({
      ...prev,
      fileTypes: prev.fileTypes.includes(fileType)
        ? prev.fileTypes.filter(type => type !== fileType)
        : [...prev.fileTypes, fileType]
    }));
  };

  return (
    <div className="db-creator-container">
      <h2>Database Creator</h2>
      <p className="description">
        Create a searchable database from a folder of documents. 
        Supports PDF, Word docs, text files, KML files, and more.
      </p>

      {/* Folder Selection */}
      <div className="section">
        <h3>1. Select Source Folder</h3>
        <div
          className="folder-drop-zone"
          onDrop={onDrop}
          onDragOver={onDragOver}
          onClick={handleFolderSelect}
        >
          <div className="drop-zone-content">
            <p>📁 Drag & drop a folder here</p>
            <p>or click to browse for a folder</p>
            {selectedFolder && (
              <p className="selected-path">Selected: {selectedFolder}</p>
            )}
          </div>
        </div>
        <button 
          className="select-folder-button" 
          onClick={handleFolderSelect}
          disabled={isProcessing}
        >
          Browse for Folder...
        </button>
      </div>

      {/* Database Name */}
      <div className="section">
        <h3>2. Database Name</h3>
        <input
          type="text"
          value={dbName}
          onChange={(e) => setDbName(e.target.value)}
          placeholder="my_database.db"
          className="db-name-input"
          disabled={isProcessing}
        />
      </div>

      {/* Processing Options */}
      <div className="section">
        <h3>3. Processing Options</h3>
        <div className="options-grid">
          <label className="option-item">
            <input
              type="checkbox"
              checked={processingOptions.extractText}
              onChange={(e) => handleOptionChange('extractText', e.target.checked)}
              disabled={isProcessing}
            />
            Extract text content
          </label>
          
          <label className="option-item">
            <input
              type="checkbox"
              checked={processingOptions.extractCoordinates}
              onChange={(e) => handleOptionChange('extractCoordinates', e.target.checked)}
              disabled={isProcessing}
            />
            Extract GPS/MGRS coordinates
          </label>
          
          <label className="option-item">
            <input
              type="checkbox"
              checked={processingOptions.includeImages}
              onChange={(e) => handleOptionChange('includeImages', e.target.checked)}
              disabled={isProcessing}
            />
            Include embedded images
          </label>
          
          <label className="option-item">
            <input
              type="checkbox"
              checked={processingOptions.recursive}
              onChange={(e) => handleOptionChange('recursive', e.target.checked)}
              disabled={isProcessing}
            />
            Process subfolders
          </label>
        </div>

        <div className="file-types-section">
          <h4>File Types to Process:</h4>
          <div className="file-types-grid">
            {supportedFormats.map(format => (
              <label key={format} className="file-type-item">
                <input
                  type="checkbox"
                  checked={processingOptions.fileTypes.includes(format)}
                  onChange={() => handleFileTypeToggle(format)}
                  disabled={isProcessing}
                />
                .{format}
              </label>
            ))}
          </div>
        </div>
      </div>

      {/* Progress */}
      {isProcessing && (
        <div className="section">
          <h3>Progress</h3>
          <div className="progress-container">
            <div className="progress-bar">
              <div 
                className="progress-fill"
                style={{
                  width: progress.total > 0 ? `${(progress.current / progress.total) * 100}%` : '0%'
                }}
              />
            </div>
            <p className="progress-text">
              {progress.current} of {progress.total} files processed
            </p>
            {progress.currentFile && (
              <p className="current-file">Processing: {progress.currentFile}</p>
            )}
          </div>
        </div>
      )}

      {/* Create Button */}
      <div className="section">
        <button
          className="create-db-button"
          onClick={handleCreateDatabase}
          disabled={!selectedFolder || !dbName || isProcessing}
        >
          {isProcessing ? '⏳ Creating Database...' : '🚀 Create Database'}
        </button>
      </div>

      {/* Status */}
      <div className="status-section">
        <p className="status-text">{status}</p>
      </div>
    </div>
  );
}
