import React, { useState, useEffect } from 'react';
import '../styles/HomeComponent.css';

const HomeComponent = () => {
  const [toastMessage, setToastMessage] = useState(
    'No database loaded. Select database in Settings before querying.'
  );
  const [apiPort, setApiPort] = useState(null);

  useEffect(() => {
    // 1) Fetch last DB path
    if (window.electronAPI?.getLastDb) {
      window.electronAPI.getLastDb().then((path) => {
        if (path) {
          setToastMessage(`🔗 Using database: ${path}`);
        }
      });
    }
    // 2) Fetch the dynamically allocated port
    if (window.electronAPI?.getApiPort) {
      window.electronAPI.getApiPort().then((port) => {
        setApiPort(port);
      });
    }
  }, []);

  return (
    <div className="home-content">
      <h1><b>Welcome to SCOUT!</b></h1>
      <p>Standalone CORE Offline Utility Tool</p>

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
