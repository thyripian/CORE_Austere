-- ================================================================
-- SQLite Schema for CORE‐Scout (schema_offline.sql)
-- Creates `reports` table, an FTS5 virtual table `reports_fts`,
-- and triggers to keep them in sync.
-- ================================================================

-- 1) Drop existing tables (only if you’re rebuilding from scratch)
DROP TABLE IF EXISTS reports;
DROP TABLE IF EXISTS reports_fts;

-- 2) Main "reports" table: contains all metadata, MGRS string, and images as a base64‐pickled BLOB
CREATE TABLE reports (
  pk INTEGER PRIMARY KEY AUTOINCREMENT,       -- internal rowid for FTS linkage
  id TEXT UNIQUE NOT NULL,                    -- document ID (e.g. file-hash or UUID)
  file_hash TEXT,
  highest_classification TEXT,
  caveats TEXT,
  file_path TEXT,
  locations TEXT,         -- pipe-delimited or JSON (e.g. "LocA|LocB")
  timeframes TEXT,        -- same format (e.g. "2021-01-01|2021-02-15")
  subjects TEXT,
  topics TEXT,
  keywords TEXT,
  MGRS TEXT,              -- pipe-delimited MGRS strings (e.g. "34SXD1234|34SXD5678")
  images BLOB,            -- base64-encoded pickle of list<BytesIO>
  full_text TEXT,
  processed_time TEXT     -- ISO 8601 timestamp (e.g. "2025-06-05T14:23:45Z")
);

-- 3) Create FTS5 virtual table that indexes the three searchable columns:
--    subjects, keywords, and full_text. It is “linked” to `reports` via content=… and content_rowid=…
CREATE VIRTUAL TABLE reports_fts
USING fts5(
  subjects,           -- full-text index on “subjects”
  keywords,           -- full-text index on “keywords”
  full_text,          -- full-text index on “full_text”
  content='reports',  -- link to the “reports” table
  content_rowid='pk', -- use reports.pk as the FTS5 rowid
  tokenize='porter'   -- Porter stemmer; you can change to 'unicode61' if desired
);

-- 4) Trigger: after inserting into `reports`, automatically insert into `reports_fts`
CREATE TRIGGER reports_ai AFTER INSERT ON reports BEGIN
  INSERT INTO reports_fts(rowid, subjects, keywords, full_text)
    VALUES (new.pk, new.subjects, new.keywords, new.full_text);
END;

-- 5) Trigger: after deleting from `reports`, automatically delete from `reports_fts`
CREATE TRIGGER reports_ad AFTER DELETE ON reports BEGIN
  DELETE FROM reports_fts WHERE rowid = old.pk;
END;

-- 6) Trigger: after updating `reports`, automatically update the corresponding `reports_fts` entry
CREATE TRIGGER reports_au AFTER UPDATE ON reports BEGIN
  UPDATE reports_fts
    SET subjects   = new.subjects,
        keywords   = new.keywords,
        full_text  = new.full_text
  WHERE rowid = old.pk;
END;
