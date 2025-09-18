// src/components/SearchComponent.js
import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { getTables, getTableFields } from '../api';
import '../styles/SearchComponent.css';

function SearchComponent() {
  const [query, setQuery] = useState('');
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState('');
  const [tableFields, setTableFields] = useState([]);
  const [searchFields, setSearchFields] = useState([]);
  const [loadingTables, setLoadingTables] = useState(true);
  const [tableError, setTableError] = useState(null);

  const navigate = useNavigate();

  // Fetch available tables from the backend with retry logic
  const fetchTables = (retryCount = 0) => {
    setLoadingTables(true);
    setTableError(null);
    console.log(`[SearchComponent] Fetching tables (attempt ${retryCount + 1})`);
    getTables()
      .then(res => {
        console.log('Tables loaded:', res.data);
        setTables(res.data);
        if (res.data.length > 0) {
          setSelectedTable(res.data[0].name);
        }
      })
      .catch(err => {
        console.error('Error fetching tables:', err);
        if (retryCount < 3) {
          console.log(`Retrying in 2 seconds... (attempt ${retryCount + 1}/3)`);
          setTimeout(() => {
            fetchTables(retryCount + 1);
          }, 2000);
        } else {
          setTableError('No database loaded. Please go to Settings to load a database or Create a new one.');
        }
      })
      .finally(() => {
        if (retryCount === 0) {
          setLoadingTables(false);
        }
      });
  };

  // Check for database path on mount and redirect if none
  useEffect(() => {
    const checkDatabasePath = async () => {
      try {
        const dbPath = await window.electronAPI.db.getPath();
        console.log('[SearchComponent] Current DB path:', dbPath);

        if (dbPath) {
          // Database path exists, proceed with fetching tables
          console.log('[SearchComponent] Database path found, fetching tables...');
          fetchTables();
        } else {
          console.log('[SearchComponent] No database path found, showing empty state');
        }
      } catch (error) {
        console.error('[SearchComponent] Error checking database path:', error);
      }
    };

    checkDatabasePath();
  }, []); // Empty dependency array - only run on mount

  // Database changes are handled manually by user navigation

  // Visibility changes are handled manually by user navigation

  // Add refresh button handler
  const handleRefresh = () => {
    console.log('[SearchComponent] Manual refresh triggered');
    setLoadingTables(true);
    setTableError(null);
    fetchTables();
  };

  // Fetch table fields when table changes
  useEffect(() => {
    if (selectedTable) {
      getTableFields(selectedTable)
        .then(res => {
          setTableFields(res.data.fields);
          setSearchFields(res.data.searchable_fields);
        })
        .catch(err => {
          console.error('Error fetching table fields:', err);
        });
    }
  }, [selectedTable]);

  const handleSearch = () => {
    if (!selectedTable) {
      setTableError('No table available - please refresh or check database connection');
      return;
    }

    // Use wildcard query if search field is empty to return all records
    const searchQuery = query.trim() || '*';

    // Store search parameters for results component to pick up
    window.searchParams = {
      query: searchQuery,
      table: selectedTable,
      searchFields: null, // Use universal search across all fields
      advancedSearch: false
    };

    // Navigate to results page to show search results
    navigate('/results');
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter') {
      handleSearch();
    }
  };

  const selectedTableInfo = tables.find(t => t.name === selectedTable);

  return (
    <div className="page-content">
      <div className="search-container">
        {loadingTables && (
          <div className="table-info">
            <h4>Loading Database...</h4>
            <div className="info-grid">
              <div className="info-item">
                <strong>Status:</strong> Connecting to backend...
              </div>
            </div>
          </div>
        )}

        {tableError && (
          <div className="table-info">
            <h4>No Database Loaded</h4>
            <div className="info-grid">
              <div className="info-item">
                <strong>Status:</strong> {tableError}
              </div>
              <div className="info-item">
                <button onClick={handleRefresh} className="refresh-button">
                  🔄 Retry Connection
                </button>
              </div>
              <div className="info-item">
                <strong>Options:</strong>
                <a href="/settings" className="nav-link">Load Database</a> |
                <a href="/create" className="nav-link">Create Database</a>
              </div>
            </div>
          </div>
        )}

        {selectedTableInfo && !loadingTables && !tableError && (
          <div className="table-info">
            <h4>Database Information</h4>
            <div className="info-grid">
              <div className="info-item">
                <strong>Table:</strong> {selectedTable}
              </div>
              <div className="info-item">
                <strong>Entries:</strong> {selectedTableInfo.row_count.toLocaleString()}
              </div>
              {selectedTableInfo.highest_classification && selectedTableInfo.highest_classification !== "None" && (
                <div className="info-item">
                  <strong>Highest Classification:</strong>
                  <span className={`classification-badge classification-${selectedTableInfo.highest_classification.toLowerCase().replace(/\s+/g, '-')}`}>
                    {selectedTableInfo.highest_classification}
                  </span>
                </div>
              )}
            </div>
          </div>
        )}

        <div className="search-box">
          <input
            type="text"
            value={query}
            onChange={e => setQuery(e.target.value)}
            onKeyDown={handleKeyDown}
            className="search-input"
            placeholder="Enter search terms..."
            disabled={loadingTables || !!tableError}
          />
          <button
            onClick={handleSearch}
            className="search-button"
            disabled={loadingTables || !!tableError}
          >
            Search
          </button>
        </div>

        <div className="search-help">
          <h4>Search Examples:</h4>
          <div className="example-queries">
            <div className="example">
              <strong>Return all records:</strong> <code>(leave empty)</code>
            </div>
            <div className="example">
              <strong>Simple text:</strong> <code>machine learning</code>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default SearchComponent;
