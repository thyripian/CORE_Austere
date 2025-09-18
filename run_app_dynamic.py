"""
Dynamic FastAPI application for CORE_Austere
Automatically adapts to any SQLite database schema
Provides Elasticsearch-like functionality
"""

import argparse
import os
import io
import json
from typing import Dict, List, Any
from fastapi import FastAPI, HTTPException, Query, Body
from pydantic import BaseModel
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from database_operations.dynamic_schema_manager import DynamicSchemaManager

class CreateDatabaseRequest(BaseModel):
    folderPath: str
    dbName: str
    options: str

class SwitchDatabaseRequest(BaseModel):
    dbPath: str = None

def main():
    parser = argparse.ArgumentParser(description="CORE-Scout (Dynamic): Universal SQLite Explorer")
    parser.add_argument("--db", "-d", required=False, help="Path to the SQLite database file")
    # Default port comes from environment variable API_PORT if available
    default_port = int(os.getenv('API_PORT', '8000'))
    parser.add_argument("--port", "-p", type=int, default=default_port,
                    help="Port to listen on (loopback only)")
    args = parser.parse_args()
    
    print(f"[run_app_dynamic] Binding FastAPI to 127.0.0.1:{args.port}", flush=True)

    # Check for database path from command line args or environment variable
    db_path = args.db or os.getenv('DB_PATH')
    
    # Initialize schema manager (will be None initially)
    global schema_manager
    schema_manager = None
    if db_path and os.path.exists(db_path):
        print(f"[run_app_dynamic] Opening SQLite DB at: {db_path}", flush=True)
        schema_manager = DynamicSchemaManager(db_path)
        if not schema_manager.connect():
            print(f"[run_app_dynamic] Warning: Failed to connect to database, starting without database", flush=True)
            schema_manager = None
    else:
        print(f"[run_app_dynamic] Starting without database - use /switch-database to load one", flush=True)

    app = FastAPI(
        title="CORE-Scout Dynamic API", 
        version="2.0",
        description="Universal SQLite database explorer with Elasticsearch-like functionality"
    )
    
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/")
    def root():
        """Root endpoint with API information"""
        schema_info = schema_manager.get_schema_info()
        return {
            "name": "CORE-Scout Dynamic API",
            "version": "2.0",
            "database": os.path.basename(args.db),
            "total_tables": schema_info['total_tables'],
            "fts_available": schema_info['fts_available'],
            "description": "Universal SQLite database explorer with Elasticsearch-like functionality"
        }

    @app.get("/schema")
    def get_schema():
        """Get complete database schema information"""
        if not schema_manager:
            return {"tables": {}, "total_tables": 0, "fts_available": False}
        return schema_manager.get_schema_info()

    @app.get("/tables")
    def get_tables():
        """Get list of all tables with metadata"""
        if not schema_manager:
            return []
        
        schema_info = schema_manager.get_schema_info()
        tables = []
        
        for table_name, table_info in schema_info['tables'].items():
            tables.append({
                'name': table_name,
                'row_count': table_info['row_count'],
                'field_count': len(table_info['fields']),
                'searchable_fields': table_info['searchable_fields'],
                'mgrs_fields': table_info['mgrs_fields'],
                'id_fields': table_info['id_fields'],
                'highest_classification': table_info['highest_classification']
            })
        
        return tables

    @app.get("/tables/{table_name}")
    def get_table_info(table_name: str):
        """Get detailed information about a specific table"""
        if not schema_manager:
            raise HTTPException(status_code=400, detail="No database loaded")
        
        schema_info = schema_manager.get_schema_info()
        
        if table_name not in schema_info['tables']:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
        
        return schema_info['tables'][table_name]

    @app.get("/tables/{table_name}/fields")
    def get_table_fields(table_name: str):
        """Get field information for a table"""
        if not schema_manager:
            raise HTTPException(status_code=400, detail="No database loaded")
        
        schema_info = schema_manager.get_schema_info()
        
        if table_name not in schema_info['tables']:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
        
        table_info = schema_info['tables'][table_name]
        return {
            'fields': table_info['fields'],
            'searchable_fields': table_info['searchable_fields'],
            'sortable_fields': table_info['sortable_fields'],
            'filterable_fields': table_info['filterable_fields']
        }

    @app.post("/search/{table_name}")
    def search_table(
        table_name: str,
        search_request: Dict[str, Any] = Body(...)
    ):
        """Elasticsearch-like search endpoint with full query DSL support"""
        try:
            if not schema_manager:
                raise HTTPException(status_code=400, detail="No database loaded")
            
            query = search_request.get('query', '*')
            fields = search_request.get('fields')
            filters = search_request.get('filters', {})
            sort = search_request.get('sort', [])
            size = search_request.get('size', 10)
            from_ = search_request.get('from', 0)
            aggregations = search_request.get('aggregations', {})
            use_elasticsearch_query = search_request.get('use_elasticsearch_query', True)
            
            # Validate parameters
            if size > 10000:
                size = 10000
            if from_ < 0:
                from_ = 0
            
            result = schema_manager.search(
                table_name=table_name,
                query=query,
                fields=fields,
                filters=filters,
                sort=sort,
                size=size,
                from_=from_,
                aggregations=aggregations,
                use_elasticsearch_query=use_elasticsearch_query
            )
            
            return {
                'took': result.took,
                'timed_out': False,
                'hits': {
                    'total': {'value': result.total, 'relation': 'eq'},
                    'max_score': result.max_score,
                    'hits': [
                        {
                            '_index': table_name,
                            '_type': '_doc',
                            '_id': str(i),
                            '_score': 1.0,
                            '_source': hit
                        }
                        for i, hit in enumerate(result.hits)
                    ]
                },
                'aggregations': result.aggregations,
                'facets': result.facets
            }
            
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) from e

    @app.get("/search/{table_name}")
    def search_table_simple(
        table_name: str,
        q: str = Query("*", description="Search query"),
        fields: str = Query(None, description="Comma-separated fields to search"),
        filters: str = Query(None, description="JSON string of filters"),
        sort: str = Query(None, description="JSON string of sort criteria"),
        size: int = Query(10, ge=1, le=10000),
        from_: int = Query(0, ge=0, alias="from")
    ):
        """Simple search endpoint for backward compatibility"""
        try:
            if not schema_manager:
                raise HTTPException(status_code=400, detail="No database loaded")
            
            # Parse parameters
            search_fields = fields.split(',') if fields else None
            search_filters = json.loads(filters) if filters else {}
            search_sort = json.loads(sort) if sort else []
            
            result = schema_manager.search(
                table_name=table_name,
                query=q,
                fields=search_fields,
                filters=search_filters,
                sort=search_sort,
                size=size,
                from_=from_
            )
            
            return {
                'total': result.total,
                'hits': result.hits,
                'took': result.took,
                'facets': result.facets
            }
            
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"Invalid JSON in parameters: {e}") from e
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) from e

    @app.get("/export/kml/{table_name}")
    def export_kml(
        table_name: str,
        q: str = Query("*", description="Search query"),
        mgrs_field: str = Query(None, description="MGRS field name"),
        limit: int = Query(10000, ge=1, le=50000)
    ):
        """Export search results as KML file"""
        try:
            if not schema_manager:
                raise HTTPException(status_code=400, detail="No database loaded")
            
            kml_bytes, metadata = schema_manager.export_kmz(
                table_name=table_name,
                query=q,
                mgrs_field=mgrs_field,
                limit=limit
            )
            
            if not kml_bytes:
                raise HTTPException(
                    status_code=400, 
                    detail=metadata.get('error', 'Export failed')
                )
            
            # Stream back as download
            buffer = io.BytesIO(kml_bytes)
            headers = {
                "Content-Disposition": f'attachment; filename="{table_name}.kml"',
                "X-Export-Metadata": json.dumps(metadata)
            }
            return StreamingResponse(
                buffer,
                media_type="application/vnd.google-earth.kml+xml",
                headers=headers
            )
            
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) from e

    @app.post("/tables/{table_name}/fts")
    def create_fts_index(
        table_name: str,
        fields: List[str] = Body(None, description="Fields to index")
    ):
        """Create FTS5 index for a table"""
        try:
            if not schema_manager:
                raise HTTPException(status_code=400, detail="No database loaded")
            
            success = schema_manager.create_fts_index(table_name, fields)
            if success:
                return {"message": f"FTS5 index created for table {table_name}"}
            else:
                raise HTTPException(
                    status_code=400, 
                    detail="Failed to create FTS5 index. Check if FTS5 is available and table has text columns."
                )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) from e

    @app.get("/supported-formats")
    def get_supported_formats():
        """Return list of supported file formats for processing."""
        from database_operations.file_processor import FileProcessor
        processor = FileProcessor()
        return processor.get_supported_formats()
    
    @app.post("/create-database")
    def create_database_route(request: CreateDatabaseRequest):
        """Create a new database from files in a folder."""
        try:
            print(f"[CREATE-DB] Starting database creation for folder: {request.folderPath}")
            print(f"[CREATE-DB] Database name: {request.dbName}")
            print(f"[CREATE-DB] Options: {request.options}")
            
            from database_operations.file_processor import FileProcessor
            from database_operations.sqlite_operations import SQLiteDatabase
            
            # Parse options if provided
            processing_options = {}
            if request.options:
                try:
                    processing_options = json.loads(request.options)
                    print(f"[CREATE-DB] Parsed options: {processing_options}")
                except Exception as e:
                    print(f"[CREATE-DB] Error parsing options: {e}")
                    processing_options = {}
            
            # Default options
            default_options = {
                'extractText': True,
                'extractCoordinates': True,
                'includeImages': False,
                'recursive': True,
                'fileTypes': ['pdf', 'txt', 'kml', 'kmz', 'doc', 'docx', 'xlsx', 'xls', 'pptx', 'ppt']
            }
            processing_options = {**default_options, **processing_options}
            
            # Initialize file processor
            processor = FileProcessor()
            print(f"[CREATE-DB] File processor initialized")
            
            # Scan folder for files
            print(f"[CREATE-DB] Scanning folder: {request.folderPath}")
            files = processor.scan_folder(request.folderPath, processing_options)
            print(f"[CREATE-DB] Found {len(files)} files to process")
            
            if not files:
                print(f"[CREATE-DB] No supported files found in folder: {request.folderPath}")
                raise HTTPException(status_code=400, detail="No supported files found in the specified folder")
            
            # Create new database path in Downloads folder
            downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")
            
            # Ensure Downloads folder exists
            if not os.path.exists(downloads_path):
                os.makedirs(downloads_path, exist_ok=True)
                print(f"[CREATE-DB] Created Downloads folder: {downloads_path}")
            
            db_path = os.path.join(downloads_path, request.dbName)
            print(f"[CREATE-DB] Saving database to: {db_path}")
            
            # Create new database with basic schema
            new_db = SQLiteDatabase(db_path)
            new_db.connect()
            
            # Create reports table with standard schema
            cursor = new_db.cursor
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS reports (
                    id TEXT PRIMARY KEY,
                    file_hash TEXT,
                    highest_classification TEXT DEFAULT 'UNCLASSIFIED',
                    caveats TEXT,
                    file_path TEXT,
                    locations TEXT,
                    timeframes TEXT,
                    subjects TEXT,
                    topics TEXT,
                    keywords TEXT,
                    MGRS TEXT,
                    images TEXT,
                    full_text TEXT,
                    processed_time TEXT
                )
            """)
            
            # Process files and insert into database
            processed_count = 0
            for file_path in files:
                try:
                    file_data = processor.process_file(file_path, processing_options)
                    
                    # Insert into database
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
            new_db.conn.close()
            
            print(f"[CREATE-DB] Database created at: {db_path}")
            print(f"[CREATE-DB] Files processed: {processed_count}/{len(files)}")
            
            return {
                "success": True,
                "dbPath": db_path,
                "filesProcessed": processed_count,
                "totalFiles": len(files),
                "stats": processor.stats,
                "message": f"Database created successfully: {os.path.basename(db_path)}",
                "fullPath": os.path.abspath(db_path)
            }
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/switch-database")
    def switch_database_route(request: SwitchDatabaseRequest):
        """Switch to a different database."""
        try:
            db_path = request.dbPath
            if not db_path or not os.path.exists(db_path):
                raise HTTPException(status_code=404, detail=f"Database file not found: {db_path}")
            
            # Switch the schema manager to the new database
            global schema_manager
            if schema_manager:
                success = schema_manager.switch_database(db_path)
            else:
                # Create new schema manager
                schema_manager = DynamicSchemaManager(db_path)
                success = schema_manager.connect()
            
            print(f"[SWITCH-DB] Switched to database: {db_path}, success: {success}")
            
            if not success:
                raise HTTPException(status_code=500, detail="Failed to switch database")
            
            return {
                "success": True,
                "message": f"Switched to database: {os.path.basename(db_path)}",
                "dbPath": db_path
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/health")
    def health_check():
        """Health check endpoint"""
        try:
            if not schema_manager:
                return {
                    "status": "healthy",
                    "database_connected": False,
                    "tables_accessible": False,
                    "fts_available": False,
                    "total_tables": 0
                }
            
            schema_info = schema_manager.get_schema_info()
            return {
                "status": "healthy",
                "database_connected": True,
                "tables_accessible": schema_info['total_tables'] > 0,
                "fts_available": schema_info['fts_available'],
                "total_tables": schema_info['total_tables']
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "database_connected": False
            }

    @app.get("/stats")
    def get_database_stats():
        """Get database statistics"""
        try:
            if not schema_manager:
                return {
                    "total_tables": 0,
                    "total_rows": 0,
                    "fts_available": False,
                    "fts_tables": [],
                    "database_size": 0
                }
            
            schema_info = schema_manager.get_schema_info()
            total_rows = sum(table['row_count'] for table in schema_info['tables'].values())
            
            return {
                "total_tables": schema_info['total_tables'],
                "total_rows": total_rows,
                "fts_available": schema_info['fts_available'],
                "fts_tables": schema_info['fts_tables'],
                "database_size": os.path.getsize(schema_manager.db_path) if os.path.exists(schema_manager.db_path) else 0
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) from e

    # Cleanup on shutdown
    @app.on_event("shutdown")
    def shutdown_event():
        if schema_manager:
            schema_manager.close()
    
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=args.port)

if __name__ == "__main__":
    main()
