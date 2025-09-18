import React, { useState, useEffect } from 'react';
import '../styles/DebugComponent.css';

const DebugComponent = () => {
    const [debugInfo, setDebugInfo] = useState(null);
    const [healthStatus, setHealthStatus] = useState(null);
    const [loading, setLoading] = useState(false);

    const fetchDebugInfo = async () => {
        setLoading(true);
        try {
            const info = await window.electronAPI.debug.getBackendInfo();
            setDebugInfo(info);
        } catch (error) {
            console.error('Failed to fetch debug info:', error);
        } finally {
            setLoading(false);
        }
    };

    const checkHealth = async () => {
        if (!debugInfo?.apiPort) return;

        try {
            const response = await fetch(`http://127.0.0.1:${debugInfo.apiPort}/health`);
            const data = await response.json();
            setHealthStatus({
                status: response.status,
                data: data,
                timestamp: new Date().toISOString()
            });
        } catch (error) {
            setHealthStatus({
                status: 'ERROR',
                error: error.message,
                timestamp: new Date().toISOString()
            });
        }
    };

    const openLogFile = () => {
        if (debugInfo?.logPath) {
            window.electronAPI.shell.openPath(debugInfo.logPath);
        }
    };

    useEffect(() => {
        fetchDebugInfo();
    }, []);

    return (
        <div className="debug-container">
            <h2>Backend Debug Information</h2>

            <div className="debug-actions">
                <button onClick={fetchDebugInfo} disabled={loading}>
                    {loading ? 'Loading...' : 'Refresh Info'}
                </button>
                <button onClick={checkHealth} disabled={!debugInfo?.apiPort}>
                    Check Health
                </button>
                <button onClick={openLogFile} disabled={!debugInfo?.logPath}>
                    Open Log File
                </button>
            </div>

            {debugInfo && (
                <div className="debug-info">
                    <h3>Backend Configuration</h3>
                    <div className="info-grid">
                        <div className="info-item">
                            <label>Packaged Mode:</label>
                            <span className={debugInfo.isPackaged ? 'success' : 'warning'}>
                                {debugInfo.isPackaged ? 'Yes' : 'No (Development)'}
                            </span>
                        </div>

                        <div className="info-item">
                            <label>Backend EXE Path:</label>
                            <span className="path">{debugInfo.backendExePath}</span>
                        </div>

                        <div className="info-item">
                            <label>File Exists:</label>
                            <span className={debugInfo.fileExists ? 'success' : 'error'}>
                                {debugInfo.fileExists ? 'Yes' : 'No'}
                            </span>
                        </div>

                        <div className="info-item">
                            <label>API Port:</label>
                            <span className="value">{debugInfo.apiPort || 'Not set'}</span>
                        </div>

                        <div className="info-item">
                            <label>Database Path:</label>
                            <span className="value">{debugInfo.dbPath || 'Not set'}</span>
                        </div>

                        <div className="info-item">
                            <label>Backend Ready:</label>
                            <span className={debugInfo.backendReady ? 'success' : 'error'}>
                                {debugInfo.backendReady ? 'Yes' : 'No'}
                            </span>
                        </div>

                        <div className="info-item">
                            <label>Process PID:</label>
                            <span className="value">{debugInfo.processPid || 'Not running'}</span>
                        </div>

                        <div className="info-item">
                            <label>Working Directory:</label>
                            <span className="path">{debugInfo.workingDir}</span>
                        </div>

                        <div className="info-item">
                            <label>Log File:</label>
                            <span className="path">{debugInfo.logPath}</span>
                        </div>
                    </div>
                </div>
            )}

            {healthStatus && (
                <div className="health-status">
                    <h3>Health Check Result</h3>
                    <div className="info-grid">
                        <div className="info-item">
                            <label>Status Code:</label>
                            <span className={healthStatus.status === 200 ? 'success' : 'error'}>
                                {healthStatus.status}
                            </span>
                        </div>

                        <div className="info-item">
                            <label>Timestamp:</label>
                            <span className="value">{healthStatus.timestamp}</span>
                        </div>

                        {healthStatus.data && (
                            <div className="info-item">
                                <label>Response Data:</label>
                                <pre className="json-response">
                                    {JSON.stringify(healthStatus.data, null, 2)}
                                </pre>
                            </div>
                        )}

                        {healthStatus.error && (
                            <div className="info-item">
                                <label>Error:</label>
                                <span className="error">{healthStatus.error}</span>
                            </div>
                        )}
                    </div>
                </div>
            )}

            <div className="debug-commands">
                <h3>Manual Verification Commands</h3>
                <p>Run these commands in PowerShell to verify backend status:</p>

                {debugInfo?.apiPort && (
                    <div className="command-block">
                        <label>Health Check:</label>
                        <code>curl.exe http://127.0.0.1:{debugInfo.apiPort}/health</code>
                    </div>
                )}

                {debugInfo?.apiPort && (
                    <div className="command-block">
                        <label>Port Check:</label>
                        <code>netstat -ano | findstr {debugInfo.apiPort}</code>
                    </div>
                )}

                {debugInfo?.processPid && (
                    <div className="command-block">
                        <label>Process Check:</label>
                        <code>Get-Process -Id {debugInfo.processPid}</code>
                    </div>
                )}
            </div>
        </div>
    );
};

export default DebugComponent;
