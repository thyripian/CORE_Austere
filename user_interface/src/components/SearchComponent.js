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

  // Fetch available tables from the backend
  const fetchTables = () => {
    setLoadingTables(true);
    setTableError(null);
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
        setTableError('Unable to load tables - make sure a database is loaded');
      })
      .finally(() => {
        setLoadingTables(false);
      });
  };

  // Fetch tables on mount
  useEffect(() => {
    fetchTables();
  }, []);

  // Add refresh button handler
  const handleRefresh = () => {
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

    navigate('/results', {
      state: {
        query: searchQuery,
        table: selectedTable,
        searchFields: null, // Use universal search across all fields
        advancedSearch: false
      }
    });
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
        {selectedTableInfo && (
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
