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

    // Helper to extract reportName from file_path if available
    function deriveReportName(data) {
        if (data?.file_path) {
            const parts = data.file_path.split(/[/\\]/);
            const fname = parts[parts.length - 1] || 'N/A';
            const noExt = fname.split('.').slice(0, -1).join('.') || fname;
            setReportName(noExt);
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

            <h2>{reportName} Details</h2>

            <div className="form-style">
                <div className="form-group">
                    <label>File Hash:</label>
                    <div className="form-value">
                        {formatText(record.SHA256_hash)}
                    </div>
                </div>
                <div className="form-group">
                    <label>Classification:</label>
                    <div className="form-value">
                        {formatText(record.highest_classification)}
                    </div>
                </div>
                <div className="form-group">
                    <label>Caveats:</label>
                    <div className="form-value">{formatText(record.caveats)}</div>
                </div>
                <div className="form-group">
                    <label>Report Name:</label>
                    <div className="form-value">{reportName}</div>
                </div>
                <div className="form-group">
                    <label>Locations:</label>
                    <div className="form-value">
                        {formatList(record.locations)}
                    </div>
                </div>
                <div className="form-group">
                    <label>Timeframes:</label>
                    <div className="form-value">
                        {formatList(record.timeframes)}
                    </div>
                </div>
                <div className="form-group">
                    <label>Subjects:</label>
                    <div className="form-value">
                        {formatList(record.subjects?.split('|'))}
                    </div>
                </div>
                <div className="form-group">
                    <label>Topics:</label>
                    <div className="form-value">
                        {formatList(record.topics?.split('|'))}
                    </div>
                </div>
                <div className="form-group">
                    <label>Keywords:</label>
                    <div className="form-value">
                        {formatList(record.keywords?.split(','))}
                    </div>
                </div>
                <div className="form-group">
                    <label>MGRS:</label>
                    <div className="form-value">
                        {formatList(record.MGRS)}
                    </div>
                </div>
                <div className="form-group">
                    <label>Processed Time:</label>
                    <div className="form-value">
                        {record.processed_time
                            ? new Date(record.processed_time).toLocaleString()
                            : 'N/A'}
                    </div>
                </div>
            </div>

            {/* Images Section */}
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

            {/* Full Text Section */}
            <div className="form-section">
                <h3>Full Text</h3>
                <div className="form-value full-text">
                    {formatText(record.full_text)}
                </div>
            </div>
        </div>
    );
}
