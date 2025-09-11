import argparse
import os
import io
import zipfile
import mgrs
from simplekml import Kml
from fastapi.responses import StreamingResponse
from database_operations.export_kmz import generate_kmz_from_mgrs

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

    db = SQLiteDatabase(args.db)
    db.connect()

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
        return db.list_tables()

    @app.get("/columns/{table_name}")
    def get_columns_route(table_name: str):
        """Return column names for the given table."""
        try:
            return db.list_columns(table_name)
        except Exception as e:
            raise HTTPException(status_code=404, detail=str(e))

    @app.get("/search/{table_name}")
    def search_table_route(table_name: str, query: str):
        """Perform a LIKE-based search across all text columns in the table."""
        try:
            return db.search_table(table_name, query)
        except ValueError as ve:
            # e.g., no text columns
            raise HTTPException(status_code=400, detail=str(ve))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/report/{sha256_hash}")
    def get_report(sha256_hash: str):
        return db.get_record_by_hash(sha256_hash)

        
    @app.get("/export/kml/{table_name}")
    def export_kml_route(
        table_name: str,
        query: str,
        mgrs_col: str = "MGRS",
        limit: int = 10000
    ):
        # 1) fetch matching rows
        rows = db.search_table(table_name, query, limit=limit)
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
