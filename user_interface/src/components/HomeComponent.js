import React, { useState, useEffect } from 'react';
import '../styles/HomeComponent.css';

const HomeComponent = () => {
  const [toastMessage, setToastMessage] = useState(
    'No database loaded. Select database in Settings before querying.'
  );
  const [apiPort, setApiPort] = useState(null);
  const [backendError, setBackendError] = useState(null);

  useEffect(() => {
    // 1) Fetch last DB path using new API
    if (window.electronAPI?.db?.getPath) {
      window.electronAPI.db.getPath().then((path) => {
        if (path) {
          setToastMessage(`🔗 Using database: ${path.split('\\').pop()}`);
        }
      });
    }
    // 2) Fetch the dynamically allocated port
    if (window.electronAPI?.getApiPort) {
      window.electronAPI.getApiPort().then((port) => {
        setApiPort(port);
      });
    }

    // 3) Listen for backend errors
    const handleBackendError = (event, error) => {
      setBackendError(error);
    };

    window.addEventListener('backend-error', handleBackendError);

    return () => {
      window.removeEventListener('backend-error', handleBackendError);
    };
  }, []);

  return (
    <div className="home-content">
      <h1><b>Welcome to SCOUT!</b></h1>
      <p>Standalone CORE Offline Utility Tool</p>

      {backendError && (
        <div className="backend-error">
          <h3>⚠️ Backend Error: {backendError.title}</h3>
          <p>{backendError.message}</p>
          <p>Log file: {backendError.logPath}</p>
          <button onClick={() => setBackendError(null)}>Dismiss</button>
        </div>
      )}

      {/* Permanent bottom-left port indicator */}
      <div className="port-indicator">
        Active Port: {apiPort ?? '—'}
      </div>

      {/* Toast at bottom-right */}
      <div className="db-status-toast">{toastMessage}</div>
    </div>
  );
};

export default HomeComponent;
