import argparse
import os
import io
import zipfile
import mgrs
from simplekml import Kml
from fastapi.responses import StreamingResponse
from database_operations.export_kmz import generate_kmz_from_mgrs
from database_operations.file_processor import FileProcessor

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from database_operations.sqlite_operations import SQLiteDatabase

def main():
    parser = argparse.ArgumentParser(description="CORE-Scout (Lite): SQLite Explorer")
    parser.add_argument("--db", "-d", required=True, help="Path to the SQLite database file")
    parser.add_argument("--port", "-p", type=int, default=8000,
                    help="Port to listen on (loopback only)")
    args = parser.parse_args()
    
    # ── DEBUGGING: confirm we got the right DB & port
    print(f"[run_app] Opening SQLite DB at: {args.db}", flush=True)
    print(f"[run_app] Binding FastAPI to 127.0.0.1:{args.port}", flush=True)

    if not os.path.exists(args.db):
        raise RuntimeError(f"SQLite DB not found at: {args.db}")

    # Global database reference that can be switched
    current_db = {"instance": None, "path": args.db}
    
    def get_db():
        if current_db["instance"] is None:
            current_db["instance"] = SQLiteDatabase(current_db["path"])
            current_db["instance"].connect()
        return current_db["instance"]
    
    def switch_database(new_db_path: str):
        if current_db["instance"]:
            current_db["instance"].conn.close()
        current_db["path"] = new_db_path
        current_db["instance"] = SQLiteDatabase(new_db_path)
        current_db["instance"].connect()
        print(f"[run_app] Switched to database: {new_db_path}", flush=True)
        return current_db["instance"]
    
    # Initialize database
    db = get_db()

    app = FastAPI(title="SCOUT", version="1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/tables")
    def get_tables_route():
        """Return a list of all non-sqlite_ internal tables."""
        return get_db().list_tables()

    @app.get("/columns/{table_name}")
    def get_columns_route(table_name: str):
        """Return column names for the given table."""
        try:
            return get_db().list_columns(table_name)
        except Exception as e:
            raise HTTPException(status_code=404, detail=str(e))

    @app.get("/search/{table_name}")
    def search_table_route(table_name: str, query: str):
        """Perform a LIKE-based search across all text columns in the table."""
        try:
            return get_db().search_table(table_name, query)
        except ValueError as ve:
            # e.g., no text columns
            raise HTTPException(status_code=400, detail=str(ve))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/report/{sha256_hash}")
    def get_report(sha256_hash: str):
        return get_db().get_record_by_hash(sha256_hash)
    
    @app.get("/database-info")
    def get_database_info():
        """Return information about the currently loaded database."""
        return {
            "path": current_db["path"],
            "name": os.path.basename(current_db["path"]),
            "tables": get_db().list_tables()
        }
    
    @app.get("/files")
    def get_all_files():
        """Return all files in the database with their metadata."""
        try:
            db_instance = get_db()
            cursor = db_instance.cursor
            
            # Get all files from reports table
            cursor.execute("""
                SELECT 
                    id,
                    file_path,
                    subjects,
                    topics,
                    keywords,
                    MGRS,
                    processed_time,
                    highest_classification
                FROM reports 
                ORDER BY processed_time DESC
            """)
            
            files = []
            for row in cursor.fetchall():
                row_dict = dict(row)
                file_path = row_dict['file_path']
                
                # Extract file info
                file_name = os.path.basename(file_path) if file_path else 'Unknown'
                file_ext = os.path.splitext(file_name)[1].lower() if file_name else ''
                
                # Determine file type category
                file_type = 'unknown'
                if file_ext in ['.pdf']:
                    file_type = 'pdf'
                elif file_ext in ['.doc', '.docx']:
                    file_type = 'document'
                elif file_ext in ['.txt', '.md', '.log']:
                    file_type = 'text'
                elif file_ext in ['.kml', '.kmz']:
                    file_type = 'geographic'
                elif file_ext in ['.jpg', '.jpeg', '.png', '.gif']:
                    file_type = 'image'
                
                files.append({
                    'id': row_dict['id'],
                    'name': file_name,
                    'extension': file_ext,
                    'type': file_type,
                    'path': file_path,
                    'subject': row_dict['subjects'] or 'No subject',
                    'topic': row_dict['topics'] or 'General',
                    'keywords': row_dict['keywords'] or '',
                    'coordinates': row_dict['MGRS'] or '',
                    'processed': row_dict['processed_time'],
                    'classification': row_dict['highest_classification'] or 'UNCLASSIFIED'
                })
            
            return {
                'files': files,
                'total': len(files)
            }
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.post("/switch-database")
    def switch_database_route(dbPath: str):
        """Switch to a different database."""
        try:
            if not os.path.exists(dbPath):
                raise HTTPException(status_code=404, detail=f"Database file not found: {dbPath}")
            
            switch_database(dbPath)
            return {
                "success": True,
                "message": f"Switched to database: {dbPath}",
                "database": get_database_info()
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/supported-formats")
    def get_supported_formats():
        """Return list of supported file formats for processing."""
        processor = FileProcessor()
        return processor.get_supported_formats()
    
    @app.get("/create-database")
    def create_database_route(
        folderPath: str,
        dbName: str,
        options: str = None
    ):
        """Create a new database from files in a folder."""
        try:
            import json
            
            # Parse options if provided
            processing_options = {}
            if options:
                try:
                    processing_options = json.loads(options)
                except:
                    processing_options = {}
            
            # Default options
            default_options = {
                'extractText': True,
                'extractCoordinates': True,
                'includeImages': False,
                'recursive': True,
                'fileTypes': ['pdf', 'txt', 'kml', 'kmz', 'doc', 'docx']
            }
            processing_options = {**default_options, **processing_options}
            
            # Initialize file processor
            processor = FileProcessor()
            
            # Scan folder for files
            files = processor.scan_folder(folderPath, processing_options)
            
            if not files:
                raise HTTPException(status_code=400, detail="No supported files found in the specified folder")
            
            # Create new database
            db_path = os.path.join(os.path.dirname(folderPath), dbName)
            
            # Apply schema to new database
            import subprocess
            import sys
            result = subprocess.run([
                sys.executable, "apply_schema.py", "--db", db_path
            ], capture_output=True, text=True)
            
            if result.returncode != 0:
                raise HTTPException(status_code=500, detail=f"Failed to create database schema: {result.stderr}")
            
            # Connect to new database
            new_db = SQLiteDatabase(db_path)
            new_db.connect()
            
            # Process files and insert into database
            processed_count = 0
            for file_path in files:
                try:
                    file_data = processor.process_file(file_path, processing_options)
                    
                    # Insert into database
                    cursor = new_db.cursor
                    cursor.execute("""
                        INSERT INTO reports (
                            id, file_hash, highest_classification, caveats, file_path,
                            locations, timeframes, subjects, topics, keywords, MGRS,
                            images, full_text, processed_time
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        file_data["id"], file_data["file_hash"], file_data["highest_classification"],
                        file_data["caveats"], file_data["file_path"], file_data["locations"],
                        file_data["timeframes"], file_data["subjects"], file_data["topics"],
                        file_data["keywords"], file_data["MGRS"], file_data["images"],
                        file_data["full_text"], file_data["processed_time"]
                    ))
                    processed_count += 1
                    
                except Exception as e:
                    print(f"Error processing {file_path}: {str(e)}")
                    continue
            
            new_db.conn.commit()
            
            # Switch to the newly created database
            switch_database(db_path)
            
            return {
                "success": True,
                "dbPath": db_path,
                "filesProcessed": processed_count,
                "totalFiles": len(files),
                "stats": processor.stats,
                "switched": True,
                "message": f"Database created and switched to: {os.path.basename(db_path)}"
            }
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

        
    @app.get("/export/kml/{table_name}")
    def export_kml_route(
        table_name: str,
        query: str,
        mgrs_col: str = "MGRS",
        limit: int = 10000
    ):
        # 1) fetch matching rows
        rows = get_db().search_table(table_name, query, limit=limit)
        # 2) generate KMZ bytes
        kmz_bytes = generate_kmz_from_mgrs(rows, mgrs_col)
        # 3) stream back as download
        buffer = io.BytesIO(kmz_bytes)
        headers = {
            "Content-Disposition": f'attachment; filename="{table_name}.kmz"'
        }
        return StreamingResponse(
            buffer,
            media_type="application/vnd.google-earth.kmz",
            headers=headers
        )
    
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=args.port)

if __name__ == "__main__":
    main()
