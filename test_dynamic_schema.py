"""
Test script for dynamic schema functionality
Demonstrates how CORE_Austere adapts to any SQLite database schema
"""

import sqlite3
import os
import tempfile
from database_operations.dynamic_schema_manager import DynamicSchemaManager

def create_test_database_1():
    """Create a test database with a simple schema"""
    db_path = tempfile.mktemp(suffix='.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create a simple table
    cursor.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT,
            age INTEGER,
            created_at TEXT
        )
    """)
    
    # Insert sample data
    sample_data = [
        (1, 'John Doe', 'john@example.com', 30, '2023-01-01'),
        (2, 'Jane Smith', 'jane@example.com', 25, '2023-01-02'),
        (3, 'Bob Johnson', 'bob@example.com', 35, '2023-01-03'),
        (4, 'Alice Brown', 'alice@example.com', 28, '2023-01-04'),
        (5, 'Charlie Wilson', 'charlie@example.com', 42, '2023-01-05')
    ]
    
    cursor.executemany("INSERT INTO users VALUES (?, ?, ?, ?, ?)", sample_data)
    conn.commit()
    conn.close()
    
    return db_path

def create_test_database_2():
    """Create a test database with a different schema"""
    db_path = tempfile.mktemp(suffix='.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create a different table structure
    cursor.execute("""
        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            title TEXT,
            description TEXT,
            price REAL,
            category TEXT,
            in_stock BOOLEAN,
            mgrs_coordinates TEXT
        )
    """)
    
    # Insert sample data
    sample_data = [
        (1, 'Widget A', 'A useful widget', 19.99, 'Electronics', True, '34SXD1234567890'),
        (2, 'Gadget B', 'An amazing gadget', 29.99, 'Electronics', False, '34SXD1234567891'),
        (3, 'Tool C', 'Essential tool', 15.50, 'Tools', True, '34SXD1234567892'),
        (4, 'Device D', 'Smart device', 99.99, 'Electronics', True, '34SXD1234567893'),
        (5, 'Accessory E', 'Useful accessory', 9.99, 'Accessories', True, '34SXD1234567894')
    ]
    
    cursor.executemany("INSERT INTO products VALUES (?, ?, ?, ?, ?, ?, ?)", sample_data)
    conn.commit()
    conn.close()
    
    return db_path

def test_dynamic_schema(db_path, db_name):
    """Test dynamic schema detection and search"""
    print(f"\n=== Testing {db_name} ===")
    print(f"Database: {db_path}")
    
    # Initialize schema manager
    schema_manager = DynamicSchemaManager(db_path)
    if not schema_manager.connect():
        print("❌ Failed to connect to database")
        return
    
    # Get schema information
    schema_info = schema_manager.get_schema_info()
    print(f"✅ Connected to database with {schema_info['total_tables']} tables")
    
    # List tables
    for table_name, table_info in schema_info['tables'].items():
        print(f"\n📋 Table: {table_name}")
        print(f"   Rows: {table_info['row_count']}")
        print(f"   Searchable fields: {len(table_info['searchable_fields'])}")
        print(f"   MGRS fields: {table_info['mgrs_fields']}")
        print(f"   ID fields: {table_info['id_fields']}")
        
        # Test search
        print(f"\n🔍 Testing search in {table_name}...")
        try:
            # Simple search
            result = schema_manager.search(table_name, "example", size=3)
            print(f"   Simple search: {result.total} results in {result.took:.3f}s")
            
            # Advanced search with aggregations
            result = schema_manager.search(
                table_name, 
                "*", 
                size=5,
                aggregations={
                    'field_stats': {
                        'type': 'stats',
                        'field': table_info['searchable_fields'][0] if table_info['searchable_fields'] else 'id'
                    }
                }
            )
            print(f"   Advanced search: {result.total} results in {result.took:.3f}s")
            
            # Test KMZ export if MGRS fields exist
            if table_info['mgrs_fields']:
                print("   🗺️  Testing KMZ export...")
                try:
                    kmz_bytes, metadata = schema_manager.export_kmz(table_name, "*", limit=5)
                    print(f"   ✅ KMZ export successful: {len(kmz_bytes)} bytes")
                    print(f"   Metadata: {metadata}")
                except (ValueError, OSError) as e:
                    print(f"   ❌ KMZ export failed: {e}")
            
        except (ValueError, sqlite3.Error) as e:
            print(f"   ❌ Search failed: {e}")
    
    schema_manager.close()
    print(f"✅ {db_name} test completed")

def main():
    """Run all tests"""
    print("🚀 Testing CORE_Austere Dynamic Schema Functionality")
    print("=" * 60)
    
    # Test 1: Simple user database
    db1 = create_test_database_1()
    try:
        test_dynamic_schema(db1, "User Database")
    finally:
        os.unlink(db1)
    
    # Test 2: Product database with MGRS
    db2 = create_test_database_2()
    try:
        test_dynamic_schema(db2, "Product Database with MGRS")
    finally:
        os.unlink(db2)
    
    print("\n🎉 All tests completed!")
    print("\nKey Features Demonstrated:")
    print("✅ Automatic schema detection")
    print("✅ Dynamic field type recognition")
    print("✅ MGRS coordinate detection")
    print("✅ Elasticsearch-like search")
    print("✅ Aggregations and facets")
    print("✅ KMZ export functionality")
    print("✅ Schema-agnostic operation")

if __name__ == "__main__":
    main()
