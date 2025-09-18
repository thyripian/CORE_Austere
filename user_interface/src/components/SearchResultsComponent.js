// src/components/SearchResultsComponent.js
import React, { useState, useEffect } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { simpleSearch, searchTable, getTableInfo, exportKML } from '../api';
import '../styles/SearchResultsComponent.css';

export default function SearchResultsComponent() {
  const location = useLocation();
  const navigate = useNavigate();
  // Get search parameters from window.searchParams instead of navigation state
  const { query, table, searchFields, advancedSearch } = window.searchParams || {};

  const [searchResult, setSearchResult] = useState(null);
  const [status, setStatus] = useState('Loading results…');
  const [exportStatus, setExportStatus] = useState('');
  const [tableInfo, setTableInfo] = useState(null);
  const [hasMGRS, setHasMGRS] = useState(false);
  const [facets, setFacets] = useState({});
  const [aggregations, setAggregations] = useState({});

  // Fetch search results
  useEffect(() => {
    if (!query || !table) {
      setStatus('⚠️ Missing query or table. Go back and try again.');
      return;
    }

    const performSearch = async () => {
      try {
        let result;
        if (advancedSearch) {
          // Elasticsearch-like search
          const searchRequest = {
            query: query,
            fields: searchFields,
            size: 50,
            from: 0,
            aggregations: {
              field_terms: {
                type: 'terms',
                field: searchFields[0] || 'id'
              }
            }
          };
          result = await searchTable(table, searchRequest);
          setSearchResult(result.data);
          setFacets(result.data.facets || {});
          setAggregations(result.data.aggregations || {});
        } else {
          // Simple search
          result = await simpleSearch(table, query, { size: 50 });
          setSearchResult(result.data);
        }
        setStatus('');
      } catch (err) {
        console.error(err);
        setStatus(`Error: ${err.message}`);
      }
    };

    performSearch();
  }, [query, table, searchFields, advancedSearch]);

  // Get table information
  useEffect(() => {
    if (!table) return;
    getTableInfo(table)
      .then(res => {
        setTableInfo(res.data);
        setHasMGRS(res.data.mgrs_fields && res.data.mgrs_fields.length > 0);
      })
      .catch(err => {
        console.error('Error fetching table info:', err);
        setHasMGRS(false);
      });
  }, [table]);

  // Get results and total count
  const results = searchResult?.hits || searchResult?.hits?.hits || [];
  const total = searchResult?.total?.value || searchResult?.total || 0;
  const took = searchResult?.took || 0;

  // Helper function to highlight search terms in text
  const highlightSearchTerm = (text, searchTerm, shouldHighlight = false) => {
    if (!searchTerm || !text || !shouldHighlight) return text;

    const regex = new RegExp(`(${searchTerm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi');
    const parts = text.split(regex);

    return parts.map((part, index) => {
      if (regex.test(part)) {
        return <span key={index} className="sr-match-text-highlight">{part}</span>;
      }
      return part;
    });
  };

  // Determine id and snippet fields
  const columns = results.length ? Object.keys(results[0]._source || results[0]) : [];
  const idField = tableInfo?.id_fields?.[0] || columns[0] || '';

  // Better field selection for title and snippet
  const getBestField = (record, priorityFields) => {
    for (const field of priorityFields) {
      if (record[field] && String(record[field]).trim()) {
        return field;
      }
    }
    return null;
  };

  // Priority order for title fields (most descriptive first)
  const titlePriorityFields = ['name', 'title', 'description', 'subject', 'topic', 'label', 'caption', 'filename', 'file_name'];
  const snippetPriorityFields = ['description', 'content', 'text', 'full_text', 'summary', 'abstract', 'details', 'name', 'title'];

  // Get the best title and snippet fields from the first record
  const firstRecord = results.length ? (results[0]._source || results[0]) : {};
  const titleField = getBestField(firstRecord, titlePriorityFields) ||
    (searchFields?.[0]) ||
    columns.find(col => typeof firstRecord[col] === 'string') ||
    idField;
  const snippetField = getBestField(firstRecord, snippetPriorityFields) ||
    (searchFields?.[0]) ||
    columns.find(col => typeof firstRecord[col] === 'string') ||
    idField;

  // Generate KML handler
  const handleGenerateKML = async () => {
    setExportStatus('Generating KML…');
    try {
      const mgrsField = tableInfo?.mgrs_fields?.[0] || 'MGRS';
      const response = await exportKML(table, query, mgrsField, 10000);

      // Create download link
      const blob = new Blob([response.data], { type: 'application/vnd.google-earth.kml+xml' });
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `${table}.kml`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);

      setExportStatus('✅ KML downloaded successfully');
    } catch (e) {
      setExportStatus(`❗ ${e.message}`);
    }
    setTimeout(() => setExportStatus(''), 4000);
  };

  // Navigate to full report
  const openRecord = item => {
    const record = item._source || item;
    const id = record[idField];
    navigate(`/report/${encodeURIComponent(id)}`, { state: { record } });
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
        <div className="sr-stats">
          <span className="sr-count">
            {total.toLocaleString()} results ({took}ms)
          </span>
        </div>
        {hasMGRS && (
          <button className="sr-kmz-btn" onClick={handleGenerateKML}>
            Export KML
          </button>
        )}
        {exportStatus && (
          <span className="sr-export-status">{exportStatus}</span>
        )}
      </div>

      <h2 className="sr-header">
        Search results for "{query}" in <em>{table}</em>
      </h2>

      {/* Facets */}
      {Object.keys(facets).length > 0 && (
        <div className="sr-facets">
          <h4>Filters</h4>
          {Object.entries(facets).map(([field, values]) => (
            <div key={field} className="facet-group">
              <strong>{field}:</strong>
              {values.slice(0, 5).map((facet, idx) => (
                <span key={idx} className="facet-item">
                  {facet.value} ({facet.count})
                </span>
              ))}
            </div>
          ))}
        </div>
      )}

      {/* Aggregations */}
      {Object.keys(aggregations).length > 0 && (
        <div className="sr-aggregations">
          <h4>Statistics</h4>
          {Object.entries(aggregations).map(([name, agg]) => (
            <div key={name} className="agg-group">
              <strong>{name}:</strong>
              {agg.buckets ? (
                <div className="agg-buckets">
                  {agg.buckets.slice(0, 3).map((bucket, idx) => (
                    <span key={idx} className="agg-item">
                      {bucket.key}: {bucket.doc_count}
                    </span>
                  ))}
                </div>
              ) : (
                <span className="agg-stats">
                  Min: {agg.min}, Max: {agg.max}, Avg: {agg.avg?.toFixed(2)}
                </span>
              )}
            </div>
          ))}
        </div>
      )}

      <div className="sr-list">
        {results.map((item, idx) => {
          const record = item._source || item;
          const idVal = record[idField];

          // Get the best title from the record
          const titleValue = getBestField(record, titlePriorityFields)
            ? record[getBestField(record, titlePriorityFields)]
            : (record[titleField] || idVal || 'Untitled');

          // Get the best snippet from the record
          const snippetValue = getBestField(record, snippetPriorityFields)
            ? record[getBestField(record, snippetPriorityFields)]
            : (record[snippetField] || 'No preview available.');

          // Get match information
          const matches = record._matches || [];

          return (
            <div
              key={idx}
              className="sr-card"
              onClick={() => openRecord(item)}
            >
              <h3 className="sr-title">
                {String(titleValue)}
              </h3>
              <div className="sr-url">
                {table}/{idVal}
              </div>
              <p className="sr-snippet">{String(snippetValue)}</p>

              {/* Show matching fields */}
              {matches.length > 0 && (
                <div className="sr-matches">
                  <div className="sr-matches-label">Matches in:</div>
                  {matches.slice(0, 3).map((match, matchIdx) => (
                    <div key={matchIdx} className="sr-match-item">
                      <span className="sr-match-field">{match.field}:</span>
                      <span className="sr-match-value">
                        {match.is_full_text && match.has_context ?
                          highlightSearchTerm(match.context, query, match.should_highlight) :
                          highlightSearchTerm(
                            match.has_context ? match.context : match.value,
                            query,
                            match.should_highlight
                          )
                        }
                      </span>
                    </div>
                  ))}
                  {matches.length > 3 && (
                    <div className="sr-match-more">
                      +{matches.length - 3} more field{matches.length - 3 !== 1 ? 's' : ''}
                    </div>
                  )}
                </div>
              )}

              {item._score && (
                <div className="sr-score">
                  Score: {item._score.toFixed(2)}
                </div>
              )}
            </div>
          );
        })}
      </div>

      {results.length === 0 && (
        <div className="sr-no-results">
          <p>No results found for "{query}" in {table}</p>
          <p>Try adjusting your search terms or check the table structure.</p>
        </div>
      )}
    </div>
  );
}
