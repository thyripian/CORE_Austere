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
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from database_operations.dynamic_schema_manager import DynamicSchemaManager

def main():
    parser = argparse.ArgumentParser(description="CORE-Scout (Dynamic): Universal SQLite Explorer")
    parser.add_argument("--db", "-d", required=True, help="Path to the SQLite database file")
    parser.add_argument("--port", "-p", type=int, default=8000,
                    help="Port to listen on (loopback only)")
    args = parser.parse_args()
    
    print(f"[run_app_dynamic] Opening SQLite DB at: {args.db}", flush=True)
    print(f"[run_app_dynamic] Binding FastAPI to 127.0.0.1:{args.port}", flush=True)

    if not os.path.exists(args.db):
        raise RuntimeError(f"SQLite DB not found at: {args.db}")

    # Initialize dynamic schema manager
    schema_manager = DynamicSchemaManager(args.db)
    if not schema_manager.connect():
        raise RuntimeError("Failed to connect to database or analyze schema")

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
        return schema_manager.get_schema_info()

    @app.get("/tables")
    def get_tables():
        """Get list of all tables with metadata"""
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
        schema_info = schema_manager.get_schema_info()
        
        if table_name not in schema_info['tables']:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
        
        return schema_info['tables'][table_name]

    @app.get("/tables/{table_name}/fields")
    def get_table_fields(table_name: str):
        """Get field information for a table"""
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

    @app.get("/health")
    def health_check():
        """Health check endpoint"""
        try:
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
            schema_info = schema_manager.get_schema_info()
            total_rows = sum(table['row_count'] for table in schema_info['tables'].values())
            
            return {
                "total_tables": schema_info['total_tables'],
                "total_rows": total_rows,
                "fts_available": schema_info['fts_available'],
                "fts_tables": schema_info['fts_tables'],
                "database_size": os.path.getsize(args.db) if os.path.exists(args.db) else 0
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) from e

    # Cleanup on shutdown
    @app.on_event("shutdown")
    def shutdown_event():
        schema_manager.close()
    
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=args.port)

if __name__ == "__main__":
    main()
