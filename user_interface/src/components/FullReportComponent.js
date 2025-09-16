// src/components/FullReportComponent.js
import React, { useEffect, useState } from 'react';
import { useParams, useNavigate, useLocation } from 'react-router-dom';
import { apiGet } from '../api';
import '../styles/FullReportComponent.css';

export default function FullReportComponent() {
    const { reportId } = useParams();
    const location = useLocation();
    const navigate = useNavigate();

    // Try to get the record from navigation state first
    const [record, setRecord] = useState(location.state?.record || null);
    const [error, setError] = useState(null);
    const [reportName, setReportName] = useState('N/A');
    const [tableInfo, setTableInfo] = useState(null);

    useEffect(() => {
        // If we already have the record (via state), skip fetching
        if (record) {
            deriveReportName(record);
            return;
        }

        // Otherwise fetch it from the backend
        async function fetchRecord() {
            try {
                const res = await apiGet(`/report/${encodeURIComponent(reportId)}`);
                const data = res.data;
                setRecord(data);
                deriveReportName(data);
            } catch (err) {
                setError(`An error occurred: ${err.message}`);
            }
        }

        fetchRecord();
    }, [reportId, record]);

    // Fetch table information to get the actual schema
    useEffect(() => {
        async function fetchTableInfo() {
            if (!record) return;

            try {
                // First try to get all tables to see what's available
                const tablesRes = await apiGet('/tables');
                const tables = tablesRes.data;
                console.log('Available tables:', tables);

                // Try to find the table that contains this record
                let tableName = record?.table || 'test';

                // If we have tables, try to find the right one
                if (tables && tables.length > 0) {
                    // Look for a table that might contain this record
                    const possibleTable = tables.find(t =>
                        t.name === tableName ||
                        t.name === 'test' ||
                        t.name.toLowerCase().includes('test')
                    );
                    if (possibleTable) {
                        tableName = possibleTable.name;
                    } else {
                        // Use the first available table
                        tableName = tables[0].name;
                    }
                }

                console.log('Fetching table info for:', tableName);
                const res = await apiGet(`/tables/${tableName}`);
                setTableInfo(res.data);
            } catch (err) {
                console.warn('Table info not available, using fallback schema:', err.message);
                // Create a fallback table info based on the record's actual fields
                const fields = Object.keys(record).map(fieldName => ({
                    name: fieldName,
                    type: 'text', // Default type
                    nullable: true,
                    is_primary_key: fieldName === 'id',
                    is_indexed: false,
                    sample_values: [],
                    searchable: true,
                    sortable: true,
                    filterable: true
                }));

                setTableInfo({
                    fields: fields,
                    id_fields: ['id'],
                    searchable_fields: fields.map(f => f.name),
                    sortable_fields: fields.map(f => f.name),
                    filterable_fields: fields.map(f => f.name)
                });
            }
        }

        if (record) {
            fetchTableInfo();
        }
    }, [record]);

    // Helper to extract the best title for the report
    function deriveReportName(data) {
        if (!data) {
            setReportName('N/A');
            return;
        }

        // Priority order for title fields
        const titleFields = [
            'name', 'title', 'report_name', 'filename', 'file_name',
            'description', 'subject', 'topic', 'label', 'caption',
            'id', 'uuid', 'hash', 'sha256_hash'
        ];

        // Look for the best title field
        for (const field of titleFields) {
            if (data[field] && data[field] !== 'N/A' && data[field] !== '') {
                let title = String(data[field]);

                // Clean up the title
                if (field === 'file_path' || field === 'filename' || field === 'file_name') {
                    const parts = title.split(/[/\\]/);
                    title = parts[parts.length - 1] || title;
                    // Remove file extension
                    title = title.split('.').slice(0, -1).join('.') || title;
                }

                // Truncate very long titles
                if (title.length > 50) {
                    title = title.substring(0, 47) + '...';
                }

                setReportName(title);
                return;
            }
        }

        // Fallback to ID if nothing else is found
        if (data.id) {
            setReportName(`Record ${data.id}`);
        } else {
            setReportName('Report Details');
        }
    }

    // Formatting helpers
    const formatList = items => {
        if (Array.isArray(items)) return items.length ? items.join(', ') : 'N/A';
        return typeof items === 'string' && items.trim() ? items : 'N/A';
    };
    const formatText = text =>
        text && text !== 'none_found' ? text : 'N/A';

    // Error state
    if (error) {
        return (
            <div className="full-report-container">
                <p className="error-message">{error}</p>
                <button onClick={() => navigate(-1)}>Go Back</button>
            </div>
        );
    }

    // Loading state
    if (!record) {
        return (
            <div className="full-report-container">
                <p>Loading full report...</p>
            </div>
        );
    }

    // Render the full report
    return (
        <div className="full-report-container">
            <button
                className="back-button"
                onClick={() => navigate(-1)}
            >
                Go Back
            </button>

            <h2>
                {reportName}
                {reportName !== 'Report Details' && reportName !== 'N/A' && !reportName.startsWith('Record ') && ' Details'}
            </h2>
            {tableInfo && (
                <div className="report-context">
                    <span className="table-badge">
                        Table: {tableInfo.name || 'Unknown'}
                    </span>
                    {record?.id && (
                        <span className="record-id">
                            ID: {record.id}
                        </span>
                    )}
                </div>
            )}

            <div className="form-style">
                {/* Dynamically render fields based on actual database schema */}
                {tableInfo?.fields ? (
                    tableInfo.fields.map((field) => {
                        const fieldName = field.name;
                        const fieldValue = record[fieldName];

                        // Skip certain fields that are handled specially
                        if (fieldName === 'id' && tableInfo.id_fields?.includes(fieldName)) {
                            return null; // Skip ID field as it's not user-friendly
                        }

                        return (
                            <div key={fieldName} className="form-group">
                                <label>{fieldName.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}:</label>
                                <div className="form-value">
                                    {field.type === 'datetime' || field.type === 'date'
                                        ? (fieldValue ? new Date(fieldValue).toLocaleString() : 'N/A')
                                        : field.type === 'json'
                                            ? (fieldValue ? JSON.stringify(fieldValue, null, 2) : 'N/A')
                                            : formatText(fieldValue)
                                    }
                                </div>
                            </div>
                        );
                    })
                ) : (
                    // Fallback to hardcoded fields if table info is not available
                    <>
                        <div className="form-group">
                            <label>ID:</label>
                            <div className="form-value">{formatText(record.id)}</div>
                        </div>
                        <div className="form-group">
                            <label>Name:</label>
                            <div className="form-value">{formatText(record.name)}</div>
                        </div>
                        <div className="form-group">
                            <label>Value:</label>
                            <div className="form-value">{formatText(record.value)}</div>
                        </div>
                        <div className="form-group">
                            <label>Description:</label>
                            <div className="form-value">{formatText(record.description)}</div>
                        </div>
                    </>
                )}
            </div>

            {/* Images Section - only show if images field exists */}
            {Array.isArray(record.images) && record.images.length > 0 && (
                <div className="form-section">
                    <h3>Images</h3>
                    <div className="images-grid">
                        {record.images.map((imgData, i) => (
                            <img
                                key={i}
                                src={`data:image/jpeg;base64,${imgData}`}
                                alt={`Report image ${i}`}
                                className="report-image"
                            />
                        ))}
                    </div>
                </div>
            )}

            {/* Full Text Section - only show if full_text field exists */}
            {record.full_text && (
                <div className="form-section">
                    <h3>Full Text</h3>
                    <div className="form-value full-text">
                        {formatText(record.full_text)}
                    </div>
                </div>
            )}
        </div>
    );
}
