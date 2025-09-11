// src/components/SearchComponent.js
import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { apiGet } from '../api';
import '../styles/SearchComponent.css';

function SearchComponent() {
  const [query, setQuery] = useState('');
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState('');
  const [loadingTables, setLoadingTables] = useState(true);
  const [tableError, setTableError] = useState(null);

  const navigate = useNavigate();

  // Fetch available tables from the backend on mount
  useEffect(() => {
    let mounted = true;
    setLoadingTables(true);
    apiGet('/tables')
      .then(res => {
        if (!mounted) return;
        setTables(res.data);
        if (res.data.length > 0) {
          setSelectedTable(res.data[0]);
        }
      })
      .catch(err => {
        console.error('Error fetching tables:', err);
        if (mounted) setTableError('Unable to load tables');
      })
      .finally(() => {
        if (mounted) setLoadingTables(false);
      });
    return () => { mounted = false; };
  }, []);

  const handleSearch = () => {
    if (!selectedTable) {
      setTableError('Please select a table');
      return;
    }
    if (!query.trim()) {
      return;
    }
    navigate('/results', { state: { query, table: selectedTable } });
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter') {
      handleSearch();
    }
  };

  return (
    <div className="page-content">
      <div className="search-container">
        <h2>Search</h2>

        {loadingTables ? (
          <p>Loading tables…</p>
        ) : tableError ? (
          <p className="error">{tableError}</p>
        ) : (
          <div className="table-select">
            <label htmlFor="table-dropdown">Table:</label>
            <select
              id="table-dropdown"
              value={selectedTable}
              onChange={e => setSelectedTable(e.target.value)}
            >
              {tables.map(tbl => (
                <option key={tbl} value={tbl}>{tbl}</option>
              ))}
            </select>
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
      </div>
    </div>
  );
}

export default SearchComponent;
