// src/components/SearchResultsComponent.js
import React, { useState, useEffect } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { apiGet } from '../api';
import '../styles/SearchResultsComponent.css';

export default function SearchResultsComponent() {
  const location = useLocation();
  const navigate = useNavigate();
  const { query, table } = location.state || {};

  const [results, setResults] = useState([]);
  const [status, setStatus] = useState('Loading results…');
  const [exportStatus, setExportStatus] = useState('');
  const [hasMGRS, setHasMGRS] = useState(false);

  // Fetch search results
  useEffect(() => {
    if (!query || !table) {
      setStatus('⚠️ Missing query or table. Go back and try again.');
      return;
    }
    apiGet(`/search/${table}`, { query })
      .then(res => {
        if (!res.data.length) {
          setStatus(`No results for “${query}” in “${table}.”`);
        } else {
          setResults(res.data);
          setStatus('');
        }
      })
      .catch(err => {
        console.error(err);
        setStatus(`Error: ${err.message}`);
      });
  }, [query, table]);

  // Check if this table has an MGRS column
  useEffect(() => {
    if (!table) return;
    apiGet(`/columns/${table}`)
      .then(res => {
        setHasMGRS(Array.isArray(res.data) && res.data.includes('MGRS'));
      })
      .catch(err => {
        console.error('Error fetching columns:', err);
        setHasMGRS(false);
      });
  }, [table]);

  // Determine id and snippet fields
  const columns = results.length ? Object.keys(results[0]) : [];
  const idField = columns[0] || '';
  const snippetField =
    results.length
      ? columns.find(col => typeof results[0][col] === 'string') || idField
      : idField;

  // Generate KMZ handler
  const handleGenerateKMZ = async () => {
    setExportStatus('Generating KMZ…');
    try {
      const saved = await window.electronAPI.exportKml(
        table, query, 'MGRS', 10000
      );
      setExportStatus(saved
        ? `✅ Saved KMZ to ${saved}`
        : '✖️ Canceled'
      );
    } catch (e) {
      setExportStatus(`❗ ${e.message}`);
    }
    setTimeout(() => setExportStatus(''), 4000);
  };

  // Navigate to full report
  const openRecord = item => {
    const id = item[idField];
    navigate(`/report/${encodeURIComponent(item[idField])}`, { state: { record: item } });
  };

  // Show status if loading or error/no-results
  if (status) {
    return (
      <div className="search-results-page">
        <p className={status.startsWith('Error') ? 'status error' : 'status'}>
          {status}
        </p>
      </div>
    );
  }

  // Render results as cards
  return (
    <div className={`search-results-page ${hasMGRS ? 'with-toolbar' : 'no-toolbar'}`}>
      <div className="sr-toolbar">
        {hasMGRS && (
          <button className="sr-kmz-btn" onClick={handleGenerateKMZ}>
            Generate KMZ
          </button>
        )}
        {exportStatus && (
          <span className="sr-export-status">{exportStatus}</span>
        )}
      </div>

      <h2 className="sr-header">
        {results.length} results for “{query}” in <em>{table}</em>
      </h2>

      <div className="sr-list">
        {results.map((item, idx) => {
          const idVal = item[idField];
          const rawSnippet = item[snippetField];
          const snippet = rawSnippet
            ? String(rawSnippet).slice(0, 140) + '…'
            : 'No preview available.';
          return (
            <div
              key={idx}
              className="sr-card"
              onClick={() => openRecord(item)}
            >
              <h3 className="sr-title">
                {snippetField === idField
                  ? String(idVal)
                  : String(item[snippetField]).split('\n')[0].slice(0, 50) + '…'}
              </h3>
              <div className="sr-url">
                {table}/{idVal}
              </div>
              <p className="sr-snippet">{snippet}</p>
            </div>
          );
        })}
      </div>
    </div>
  );
}
