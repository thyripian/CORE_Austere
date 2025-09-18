// src/components/DbCreatorComponent.js
import React, { useState, useCallback } from 'react';
import { getSupportedFormats, createDatabase, switchDatabase } from '../api';
import '../styles/DbCreatorComponent.css';

export default function DbCreatorComponent() {
    const [selectedFolder, setSelectedFolder] = useState('');
    const [dbName, setDbName] = useState('');
    const [status, setStatus] = useState('No folder selected.');
    const [isProcessing, setIsProcessing] = useState(false);
    const [progress, setProgress] = useState({ current: 0, total: 0, currentFile: '' });
    const [supportedFormats, setSupportedFormats] = useState([]);
    const [loadingFormats, setLoadingFormats] = useState(true);
    const [processingOptions, setProcessingOptions] = useState({
        extractText: true,
        extractCoordinates: true,
        includeImages: false,
        recursive: true,
        fileTypes: [] // Will be populated when supportedFormats are fetched
    });

    // Fetch supported file formats on component mount
    React.useEffect(() => {
        // Add a small delay to ensure the backend is ready
        const timer = setTimeout(() => {
            fetchSupportedFormats();
        }, 1000);

        return () => clearTimeout(timer);
    }, []);

    const fetchSupportedFormats = async (retryCount = 0) => {
        const maxRetries = 3;

        try {
            console.log(`[DbCreator] Fetching supported formats (attempt ${retryCount + 1}/${maxRetries + 1})...`);
            const response = await getSupportedFormats();
            console.log('[DbCreator] Received supported formats:', response.data);
            console.log('[DbCreator] Number of formats:', response.data?.length);

            if (response.data && Array.isArray(response.data) && response.data.length > 0) {
                setSupportedFormats(response.data);
                // Update processing options to include all supported formats
                setProcessingOptions(prev => ({
                    ...prev,
                    fileTypes: response.data
                }));
                console.log('[DbCreator] Successfully set supported formats');
                setLoadingFormats(false);
            } else {
                throw new Error('Invalid response format or empty data');
            }
        } catch (err) {
            console.error(`[DbCreator] Error fetching supported formats (attempt ${retryCount + 1}):`, err);
            console.error('[DbCreator] Error details:', err.response?.data || err.message);

            // If this is the first attempt and backend might not be running, try to start it
            if (retryCount === 0 && window.electronAPI?.backend?.start) {
                console.log('[DbCreator] Backend might not be running, attempting to start it...');
                try {
                    const backendResult = await window.electronAPI.backend.start({ allowNoDatabase: true });
                    if (backendResult.success) {
                        console.log('[DbCreator] Backend started successfully, retrying in 3 seconds...');
                        setTimeout(() => {
                            fetchSupportedFormats(retryCount + 1);
                        }, 3000);
                        return;
                    }
                } catch (backendErr) {
                    console.error('[DbCreator] Failed to start backend:', backendErr);
                }
            }

            if (retryCount < maxRetries) {
                console.log(`[DbCreator] Retrying in 2 seconds... (attempt ${retryCount + 1}/${maxRetries})`);
                setTimeout(() => {
                    fetchSupportedFormats(retryCount + 1);
                }, 2000);
                return;
            }

            // Fallback to default formats after all retries failed
            const fallbackFormats = ['pdf', 'txt', 'kml', 'kmz', 'doc', 'docx', 'md', 'xlsx', 'xls', 'csv', 'json', 'xml', 'rtf', 'odt', 'ods', 'odp', 'pptx', 'ppt', 'png', 'jpg', 'jpeg', 'gif', 'bmp', 'tiff'];
            console.log('[DbCreator] Using fallback formats after all retries failed:', fallbackFormats);
            setSupportedFormats(fallbackFormats);
            setProcessingOptions(prev => ({
                ...prev,
                fileTypes: fallbackFormats
            }));
            setLoadingFormats(false);
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
            const response = await createDatabase(selectedFolder, dbName, processingOptions);

            if (response.data.success) {
                const dbPath = response.data.fullPath || response.data.dbPath;
                const fileName = dbPath.split(/[/\\]/).pop();
                setStatus(`✅ Database created successfully! Saving to Downloads folder as: ${fileName}`);
                setProgress({ current: response.data.filesProcessed, total: response.data.totalFiles, currentFile: 'Complete!' });

                // Switch to the new database
                try {
                    console.log('Attempting to switch to database:', dbPath);
                    const switchResponse = await switchDatabase(dbPath);
                    console.log('Database switch response:', switchResponse.data);
                } catch (switchError) {
                    console.error('Failed to switch database:', switchError);
                    setStatus(`⚠️ Database created but failed to switch: ${switchError.message}`);
                }

                // Notify other components that database has changed
                window.dispatchEvent(new CustomEvent('databaseChanged', {
                    detail: { dbPath: dbPath }
                }));

                // Show success message with database location
                setTimeout(() => {
                    const fileName = dbPath.split(/[/\\]/).pop();
                    setStatus(`✅ Database ready! Saved to Downloads folder as: ${fileName}\nGo to Search to query your ${response.data.filesProcessed} processed files.`);
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

    const handleSelectAllFileTypes = () => {
        setProcessingOptions(prev => ({
            ...prev,
            fileTypes: supportedFormats
        }));
    };

    const handleDeselectAllFileTypes = () => {
        setProcessingOptions(prev => ({
            ...prev,
            fileTypes: []
        }));
    };

    const isAllSelected = processingOptions.fileTypes.length === supportedFormats.length;
    const isNoneSelected = processingOptions.fileTypes.length === 0;

    return (
        <div className="db-creator-container">
            <h2>Database Creator</h2>
            <p className="description">
                Create a searchable database from a folder of documents.
                Supports PDF, Word docs, text files, KML files, and more.
                <br />
                <em>* Database will be saved to your Downloads folder for easy access.</em>
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
                    <div className="file-types-header">
                        <h4>File Types to Process:</h4>
                        <div className="file-types-controls">
                            <button
                                className="refresh-formats-button"
                                onClick={() => fetchSupportedFormats()}
                                disabled={isProcessing}
                                title="Refresh supported file formats"
                            >
                                🔄 Refresh
                            </button>
                            <button
                                className="select-all-button"
                                onClick={isAllSelected ? handleDeselectAllFileTypes : handleSelectAllFileTypes}
                                disabled={isProcessing || supportedFormats.length === 0}
                                title={isAllSelected ? "Deselect all file types" : "Select all file types"}
                            >
                                {isAllSelected ? "☐ Deselect All" : "☑ Select All"}
                            </button>
                        </div>
                    </div>
                    {loadingFormats ? (
                        <div className="formats-loading">
                            <div className="loading-spinner"></div>
                            <p>Loading supported file formats...</p>
                            <p className="loading-subtext">Starting backend and fetching format list</p>
                        </div>
                    ) : (
                        <>
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
                            <p className="formats-info">
                                Showing {supportedFormats.length} supported file formats
                            </p>
                        </>
                    )}
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
