"""
Dynamic Schema Manager for CORE_Austere
Automatically detects and adapts to any SQLite database schema
Provides Elasticsearch-like functionality
"""

import sqlite3
import re
from typing import Dict, List, Any, Optional, Set, Tuple, Union
from dataclasses import dataclass, asdict
from enum import Enum
import threading
from datetime import datetime
from .elasticsearch_query_parser import ElasticsearchQueryParser

class FieldType(Enum):
    TEXT = "text"
    INTEGER = "integer"
    REAL = "real"
    BLOB = "blob"
    DATE = "date"
    DATETIME = "datetime"
    BOOLEAN = "boolean"
    JSON = "json"
    MGRS = "mgrs"
    UNKNOWN = "unknown"

@dataclass
class FieldInfo:
    name: str
    type: FieldType
    nullable: bool
    is_primary_key: bool
    is_indexed: bool
    sample_values: List[str]
    searchable: bool
    sortable: bool
    filterable: bool

@dataclass
class TableInfo:
    name: str
    row_count: int
    fields: List[FieldInfo]
    searchable_fields: List[str]
    sortable_fields: List[str]
    filterable_fields: List[str]
    mgrs_fields: List[str]
    id_fields: List[str]
    highest_classification: str
    created_at: str
    updated_at: str

@dataclass
class SearchResult:
    total: int
    hits: List[Dict[str, Any]]
    aggregations: Dict[str, Any]
    facets: Dict[str, Any]
    took: float
    max_score: float

class DynamicSchemaManager:
    """Manages dynamic schema detection and Elasticsearch-like operations"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.tables: Dict[str, TableInfo] = {}
        self._lock = threading.Lock()
        self._fts_tables: Set[str] = set()
        self._query_parser: Optional[ElasticsearchQueryParser] = None
        
        # Elasticsearch-like field mappings
        self.field_mappings = {
            'text': FieldType.TEXT,
            'varchar': FieldType.TEXT,
            'char': FieldType.TEXT,
            'clob': FieldType.TEXT,
            'string': FieldType.TEXT,
            'integer': FieldType.INTEGER,
            'int': FieldType.INTEGER,
            'bigint': FieldType.INTEGER,
            'real': FieldType.REAL,
            'float': FieldType.REAL,
            'double': FieldType.REAL,
            'blob': FieldType.BLOB,
            'date': FieldType.DATE,
            'datetime': FieldType.DATETIME,
            'timestamp': FieldType.DATETIME,
            'boolean': FieldType.BOOLEAN,
            'bool': FieldType.BOOLEAN
        }
        
        # MGRS column patterns
        self.mgrs_patterns = [
            r'mgrs', r'grid', r'coord', r'location', r'loc',
            r'lat.*lon', r'latitude.*longitude', r'geo'
        ]
        
        # ID column patterns
        self.id_patterns = [
            r'^id$', r'^pk$', r'^key$', r'^uuid$', r'^hash$',
            r'^sha\d+$', r'^.*_id$', r'^.*_key$'
        ]

    def connect(self) -> bool:
        """Connect to database and initialize schema"""
        try:
            self.conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False
            )
            self.conn.row_factory = sqlite3.Row
            self._analyze_schema()
            self._query_parser = ElasticsearchQueryParser(self)
            return True
        except (sqlite3.Error, OSError) as e:
            print(f"Failed to connect to database: {e}")
            return False

    def switch_database(self, new_db_path: str) -> bool:
        """Switch to a different database"""
        try:
            # Close current connection
            if self.conn:
                self.conn.close()
            
            # Update path and connect to new database
            self.db_path = new_db_path
            return self.connect()
        except Exception as e:
            print(f"Failed to switch database: {e}")
            return False

    def _analyze_schema(self):
        """Analyze database schema and build field mappings"""
        if not self.conn:
            return
        
        cursor = self.conn.cursor()
        
        # Get all tables
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """)
        table_names = [row[0] for row in cursor.fetchall()]
        
        # Analyze each table
        for table_name in table_names:
            self._analyze_table(table_name)
        
        # Check for existing FTS tables
        self._detect_fts_tables()

    def _analyze_table(self, table_name: str):
        """Analyze a single table's structure and content"""
        cursor = self.conn.cursor()
        
        # Get column information
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        # Analyze fields
        fields = []
        for col in columns:
            field_info = self._analyze_field(table_name, col, row_count)
            fields.append(field_info)
        
        # Determine field categories
        searchable_fields = [f.name for f in fields if f.searchable]
        sortable_fields = [f.name for f in fields if f.sortable]
        filterable_fields = [f.name for f in fields if f.filterable]
        mgrs_fields = [f.name for f in fields if f.type == FieldType.MGRS]
        id_fields = [f.name for f in fields if f.is_primary_key or f.name in [f.name for f in fields if self._is_id_field(f.name)]]
        
        # Find highest classification level
        highest_classification = self._get_highest_classification(table_name, fields)
        
        # Create table info
        table_info = TableInfo(
            name=table_name,
            row_count=row_count,
            fields=fields,
            searchable_fields=searchable_fields,
            sortable_fields=sortable_fields,
            filterable_fields=filterable_fields,
            mgrs_fields=mgrs_fields,
            id_fields=id_fields,
            highest_classification=highest_classification,
            created_at=datetime.now().isoformat(),
            updated_at=datetime.now().isoformat()
        )
        
        self.tables[table_name] = table_info

    def _analyze_field(self, table_name: str, column_info: tuple, row_count: int) -> FieldInfo:
        """Analyze a single field's properties"""
        col_name = column_info[1]
        col_type = column_info[2].lower()
        not_null = bool(column_info[3])
        is_pk = bool(column_info[5])
        
        # Determine field type
        field_type = self._determine_field_type(col_type, col_name)
        
        # Get sample values
        sample_values = self._get_sample_values(table_name, col_name, min(5, row_count))
        
        # Determine field capabilities
        # Fields to exclude from search entirely
        excluded_search_fields = {
            'classification', 'class', 'security_classification', 'security_class',
            'clearance', 'clearance_level', 'sensitivity', 'sensitivity_level',
            'mgrs', 'mgrs_primary', 'mgrs_secondary', 'coordinates', 'lat', 'lon',
            'latitude', 'longitude', 'id', 'record_id', 'primary_key'
        }
        
        searchable = (field_type not in [FieldType.BLOB, FieldType.UNKNOWN] and 
                     not is_pk and 
                     col_name.lower() not in excluded_search_fields)
        sortable = field_type in [FieldType.TEXT, FieldType.INTEGER, FieldType.REAL, FieldType.DATE, FieldType.DATETIME]
        filterable = field_type != FieldType.BLOB
        
        # Check if it's indexed
        is_indexed = self._is_field_indexed(table_name, col_name)
        
        return FieldInfo(
            name=col_name,
            type=field_type,
            nullable=not not_null,
            is_primary_key=is_pk,
            is_indexed=is_indexed,
            sample_values=sample_values,
            searchable=searchable,
            sortable=sortable,
            filterable=filterable
        )

    def _determine_field_type(self, col_type: str, col_name: str) -> FieldType:
        """Determine the Elasticsearch-like field type"""
        # Check for MGRS fields
        if self._is_mgrs_field(col_name):
            return FieldType.MGRS
        
        # Check for JSON fields
        if 'json' in col_type or col_name.lower() in ['metadata', 'data', 'config', 'settings']:
            return FieldType.JSON
        
        # Check for date/datetime fields
        if any(pattern in col_name.lower() for pattern in ['date', 'time', 'created', 'updated', 'modified']):
            if 'date' in col_type:
                return FieldType.DATE
            elif 'datetime' in col_type or 'timestamp' in col_type:
                return FieldType.DATETIME
        
        # Map SQLite types to field types
        return self.field_mappings.get(col_type, FieldType.UNKNOWN)

    def _is_mgrs_field(self, field_name: str) -> bool:
        """Check if field contains MGRS coordinates"""
        field_lower = field_name.lower()
        return any(re.search(pattern, field_lower) for pattern in self.mgrs_patterns)

    def _is_id_field(self, field_name: str) -> bool:
        """Check if field is an ID field"""
        field_lower = field_name.lower()
        return any(re.search(pattern, field_lower) for pattern in self.id_patterns)

    def _get_sample_values(self, table_name: str, field_name: str, limit: int) -> List[str]:
        """Get sample values from a field"""
        if not self.conn or limit <= 0:
            return []
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"""
                SELECT DISTINCT {field_name} FROM {table_name} 
                WHERE {field_name} IS NOT NULL AND {field_name} != ''
                LIMIT {limit}
            """)
            values = [str(row[0]) for row in cursor.fetchall()]
            return values
        except sqlite3.Error:
            return []

    def _is_field_indexed(self, table_name: str, field_name: str) -> bool:
        """Check if field has an index"""
        if not self.conn:
            return False
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"PRAGMA index_list({table_name})")
            indexes = cursor.fetchall()
            
            for index in indexes:
                cursor.execute(f"PRAGMA index_info({index[1]})")
                index_columns = [row[2] for row in cursor.fetchall()]
                if field_name in index_columns:
                    return True
            return False
        except sqlite3.Error:
            return False

    def _detect_fts_tables(self):
        """Detect existing FTS virtual tables"""
        if not self.conn:
            return
        
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name LIKE '%_fts'
            """)
            self._fts_tables = {row[0] for row in cursor.fetchall()}
        except:
            self._fts_tables = set()

    def get_schema_info(self) -> Dict[str, Any]:
        """Get complete schema information"""
        return {
            'database': self.db_path,
            'tables': {name: asdict(table) for name, table in self.tables.items()},
            'total_tables': len(self.tables),
            'fts_available': self._check_fts5_available(),
            'fts_tables': list(self._fts_tables)
        }

    def _check_fts5_available(self) -> bool:
        """Check if FTS5 is available"""
        if not self.conn:
            return False
        
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT fts5(1)")
            return True
        except sqlite3.Error:
            return False

    def search(self, 
               table_name: str, 
               query: Union[str, Dict[str, Any]] = "*", 
               fields: List[str] = None,
               filters: Dict[str, Any] = None,
               sort: List[Dict[str, str]] = None,
               size: int = 10,
               from_: int = 0,
               aggregations: Dict[str, Any] = None,
               use_elasticsearch_query: bool = True) -> SearchResult:
        """Elasticsearch-like search functionality"""
        
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found")
        
        table_info = self.tables[table_name]
        start_time = datetime.now()
        
        # Build search query using Elasticsearch query parser if enabled
        if use_elasticsearch_query and self._query_parser and isinstance(query, dict):
            search_query = self._build_elasticsearch_query(
                table_name, query, fields, filters, sort, size, from_
            )
        else:
            search_query = self._build_search_query(
                table_name, query, fields, filters, sort, size, from_
            )
        
        # Execute search
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute(search_query['sql'], search_query['params'])
            raw_results = [dict(row) for row in cursor.fetchall()]
            
            # Add match information for each result
            if query != "*" and query.strip():
                search_text = str(query).lower() if isinstance(query, str) else str(query.get('query', '')).lower()
                for result in raw_results:
                    result['_matches'] = self._find_matching_fields(result, search_text)
            
            # Calculate relevance scores if using Elasticsearch queries
            if use_elasticsearch_query and self._query_parser and isinstance(query, dict):
                scored_results = []
                for result in raw_results:
                    score = self._query_parser.calculate_relevance_score(result, query, table_name)
                    scored_results.append({
                        '_source': result,
                        '_score': score,
                        '_id': result.get(table_info.id_fields[0] if table_info.id_fields else 'id', ''),
                        '_index': table_name,
                        '_type': '_doc'
                    })
                results = scored_results
            else:
                results = raw_results
        
        # Get total count
        count_query = self._build_count_query(table_name, query, fields, filters)
        with self._lock:
            cursor.execute(count_query['sql'], count_query['params'])
            total = cursor.fetchone()[0]
        
        # Calculate aggregations
        agg_results = {}
        if aggregations:
            agg_results = self._calculate_aggregations(table_name, aggregations, filters)
        
        # Calculate facets
        facets = self._calculate_facets(table_name, table_info.filterable_fields, filters)
        
        took = (datetime.now() - start_time).total_seconds()
        
        return SearchResult(
            total=total,
            hits=results,
            aggregations=agg_results,
            facets=facets,
            took=took,
            max_score=1.0  # SQLite doesn't have relevance scoring
        )

    def _build_search_query(self, table_name: str, query: str, fields: List[str], 
                           filters: Dict[str, Any], sort: List[Dict[str, str]], 
                           size: int, from_: int) -> Dict[str, Any]:
        """Build SQL search query"""
        where_conditions = []
        params = []
        
        # Text search - search ALL fields for universal search (excluding classification fields)
        if query != "*" and query.strip():
            # Fields to exclude from search entirely
            excluded_search_fields = {
                'classification', 'class', 'security_classification', 'security_class',
                'clearance', 'clearance_level', 'sensitivity', 'sensitivity_level',
                'mgrs', 'mgrs_primary', 'mgrs_secondary', 'coordinates', 'lat', 'lon',
                'latitude', 'longitude', 'id', 'record_id', 'primary_key'
            }
            
            if fields:
                # If specific fields are provided, use those (but still exclude classification fields)
                search_fields = [f for f in fields 
                               if f in [field.name for field in self.tables[table_name].fields]
                               and f.lower() not in excluded_search_fields]
            else:
                # Search ALL fields in the table for universal search (excluding classification fields)
                search_fields = [field.name for field in self.tables[table_name].fields 
                               if field.name.lower() not in excluded_search_fields]
            
            if search_fields:
                search_conditions = []
                for field in search_fields:
                    # Convert field value to string for LIKE comparison
                    search_conditions.append(f"CAST({field} AS TEXT) LIKE ?")
                    params.append(f"%{query}%")
                where_conditions.append(f"({' OR '.join(search_conditions)})")
        
        # Filters
        if filters:
            for field, value in filters.items():
                if field in self.tables[table_name].filterable_fields:
                    if isinstance(value, list):
                        placeholders = ','.join(['?' for _ in value])
                        where_conditions.append(f"{field} IN ({placeholders})")
                        params.extend(value)
                    else:
                        where_conditions.append(f"{field} = ?")
                        params.append(value)
        
        # Build WHERE clause
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        # Build ORDER BY clause
        order_clause = ""
        if sort:
            order_parts = []
            for sort_item in sort:
                field = sort_item['field']
                direction = sort_item.get('order', 'asc').upper()
                if field in self.tables[table_name].sortable_fields:
                    order_parts.append(f"{field} {direction}")
            if order_parts:
                order_clause = "ORDER BY " + ", ".join(order_parts)
        
        # Build final query
        sql = f"""
            SELECT * FROM {table_name}
            {where_clause}
            {order_clause}
            LIMIT {size} OFFSET {from_}
        """.strip()
        
        return {'sql': sql, 'params': params}

    def _build_elasticsearch_query(self, table_name: str, query: Dict[str, Any], 
                                  fields: List[str], filters: Dict[str, Any], 
                                  sort: List[Dict[str, str]], size: int, from_: int) -> Dict[str, Any]:
        """Build SQL query using Elasticsearch query parser"""
        
        where_conditions = []
        params = []
        
        # Parse the Elasticsearch query
        try:
            query_where, query_params = self._query_parser.parse_query(query, table_name)
            if query_where:
                where_conditions.append(f"({query_where})")
                params.extend(query_params)
        except Exception as e:
            print(f"Error parsing Elasticsearch query: {e}")
            # Fallback to simple text search - use universal search (excluding classification fields)
            if isinstance(query, dict) and 'query' in query:
                query_text = str(query['query'])
                table_info = self.tables[table_name]
                
                # Fields to exclude from search entirely
                excluded_search_fields = {
                    'classification', 'class', 'security_classification', 'security_class',
                    'clearance', 'clearance_level', 'sensitivity', 'sensitivity_level',
                    'mgrs', 'mgrs_primary', 'mgrs_secondary', 'coordinates', 'lat', 'lon',
                    'latitude', 'longitude', 'id', 'record_id', 'primary_key'
                }
                
                if fields:
                    # If specific fields are provided, use those (but still exclude classification fields)
                    search_fields = [f for f in fields 
                                   if f in [field.name for field in table_info.fields]
                                   and f.lower() not in excluded_search_fields]
                else:
                    # Search ALL fields in the table for universal search (excluding classification fields)
                    search_fields = [field.name for field in table_info.fields 
                                   if field.name.lower() not in excluded_search_fields]
                
                if search_fields:
                    search_conditions = []
                    for field in search_fields:
                        # Convert field value to string for LIKE comparison
                        search_conditions.append(f"CAST({field} AS TEXT) LIKE ?")
                        params.append(f"%{query_text}%")
                    where_conditions.append(f"({' OR '.join(search_conditions)})")
        
        # Add filters
        if filters:
            for field, value in filters.items():
                if field in self.tables[table_name].filterable_fields:
                    if isinstance(value, list):
                        placeholders = ','.join(['?' for _ in value])
                        where_conditions.append(f"{field} IN ({placeholders})")
                        params.extend(value)
                    else:
                        where_conditions.append(f"{field} = ?")
                        params.append(value)
        
        # Build WHERE clause
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        # Build ORDER BY clause
        order_clause = ""
        if sort:
            order_parts = []
            for sort_item in sort:
                field = sort_item['field']
                direction = sort_item.get('order', 'asc').upper()
                if field in self.tables[table_name].sortable_fields:
                    order_parts.append(f"{field} {direction}")
            if order_parts:
                order_clause = "ORDER BY " + ", ".join(order_parts)
        
        # Build final query
        sql = f"""
            SELECT * FROM {table_name}
            {where_clause}
            {order_clause}
            LIMIT {size} OFFSET {from_}
        """.strip()
        
        return {'sql': sql, 'params': params}

    def _build_count_query(self, table_name: str, query: Union[str, Dict[str, Any]], fields: List[str], 
                          filters: Dict[str, Any]) -> Dict[str, Any]:
        """Build count query for total results"""
        where_conditions = []
        params = []
        
        # Handle different query types
        if isinstance(query, dict):
            # Use Elasticsearch query parser for dictionary queries
            if self._query_parser:
                try:
                    query_where, query_params = self._query_parser.parse_query(query, table_name)
                    if query_where:
                        where_conditions.append(f"({query_where})")
                        params.extend(query_params)
                except Exception as e:
                    print(f"Error parsing query in count: {e}")
        elif isinstance(query, str) and query != "*" and query.strip():
            # Fields to exclude from search entirely
            excluded_search_fields = {
                'classification', 'class', 'security_classification', 'security_class',
                'clearance', 'clearance_level', 'sensitivity', 'sensitivity_level',
                'mgrs', 'mgrs_primary', 'mgrs_secondary', 'coordinates', 'lat', 'lon',
                'latitude', 'longitude', 'id', 'record_id', 'primary_key'
            }
            
            if fields:
                # If specific fields are provided, use those (but still exclude classification fields)
                search_fields = [f for f in fields 
                               if f in [field.name for field in self.tables[table_name].fields]
                               and f.lower() not in excluded_search_fields]
            else:
                # Search ALL fields in the table for universal search (excluding classification fields)
                search_fields = [field.name for field in self.tables[table_name].fields 
                               if field.name.lower() not in excluded_search_fields]
            
            if search_fields:
                search_conditions = []
                for field in search_fields:
                    # Convert field value to string for LIKE comparison
                    search_conditions.append(f"CAST({field} AS TEXT) LIKE ?")
                    params.append(f"%{query}%")
                where_conditions.append(f"({' OR '.join(search_conditions)})")
        
        # Filters
        if filters:
            for field, value in filters.items():
                if field in self.tables[table_name].filterable_fields:
                    if isinstance(value, list):
                        placeholders = ','.join(['?' for _ in value])
                        where_conditions.append(f"{field} IN ({placeholders})")
                        params.extend(value)
                    else:
                        where_conditions.append(f"{field} = ?")
                        params.append(value)
        
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        sql = f"SELECT COUNT(*) FROM {table_name} {where_clause}".strip()
        return {'sql': sql, 'params': params}

    def _calculate_aggregations(self, table_name: str, aggregations: Dict[str, Any], 
                               filters: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate aggregations (terms, stats, etc.)"""
        results = {}
        
        for agg_name, agg_config in aggregations.items():
            agg_type = agg_config.get('type', 'terms')
            field = agg_config.get('field')
            
            if not field or field not in self.tables[table_name].filterable_fields:
                continue
            
            if agg_type == 'terms':
                results[agg_name] = self._terms_aggregation(table_name, field, filters)
            elif agg_type == 'stats':
                results[agg_name] = self._stats_aggregation(table_name, field, filters)
        
        return results

    def _terms_aggregation(self, table_name: str, field: str, filters: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate terms aggregation"""
        where_conditions = []
        params = []
        
        # Apply filters
        if filters:
            for f_field, f_value in filters.items():
                if f_field != field and f_field in self.tables[table_name].filterable_fields:
                    if isinstance(f_value, list):
                        placeholders = ','.join(['?' for _ in f_value])
                        where_conditions.append(f"{f_field} IN ({placeholders})")
                        params.extend(f_value)
                    else:
                        where_conditions.append(f"{f_field} = ?")
                        params.append(f_value)
        
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        sql = f"""
            SELECT {field}, COUNT(*) as count
            FROM {table_name}
            {where_clause}
            GROUP BY {field}
            ORDER BY count DESC
            LIMIT 100
        """.strip()
        
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute(sql, params)
            buckets = [{'key': row[0], 'doc_count': row[1]} for row in cursor.fetchall()]
        
        return {
            'buckets': buckets,
            'sum_other_doc_count': 0
        }

    def _stats_aggregation(self, table_name: str, field: str, filters: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate stats aggregation"""
        where_conditions = []
        params = []
        
        # Apply filters
        if filters:
            for f_field, f_value in filters.items():
                if f_field != field and f_field in self.tables[table_name].filterable_fields:
                    if isinstance(f_value, list):
                        placeholders = ','.join(['?' for _ in f_value])
                        where_conditions.append(f"{f_field} IN ({placeholders})")
                        params.extend(f_value)
                    else:
                        where_conditions.append(f"{f_field} = ?")
                        params.append(f_value)
        
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        sql = f"""
            SELECT 
                COUNT(*) as count,
                MIN({field}) as min,
                MAX({field}) as max,
                AVG({field}) as avg,
                SUM({field}) as sum
            FROM {table_name}
            {where_clause}
        """.strip()
        
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute(sql, params)
            row = cursor.fetchone()
        
        return {
            'count': row[0],
            'min': row[1],
            'max': row[2],
            'avg': row[3],
            'sum': row[4]
        }

    def _find_matching_fields(self, record: Dict[str, Any], search_text: str) -> List[Dict[str, str]]:
        """Find which fields in a record contain the search text with context"""
        matches = []
        
        # Fields to exclude from match reporting (but still include in search)
        excluded_fields = {
            'classification', 'class', 'security_classification', 'security_class',
            'clearance', 'clearance_level', 'sensitivity', 'sensitivity_level',
            'mgrs', 'mgrs_primary', 'mgrs_secondary', 'coordinates', 'lat', 'lon',
            'latitude', 'longitude', 'id', 'record_id', 'primary_key'
        }
        
        for field_name, field_value in record.items():
            if field_value is not None and field_name != '_matches':
                field_str = str(field_value).lower()
                if search_text in field_str:
                    # Skip excluded fields from match reporting
                    if field_name.lower() in excluded_fields:
                        continue
                    
                    # Find the position of the match for highlighting
                    match_pos = field_str.find(search_text)
                    original_value = str(field_value)
                    
                    # Generate context for full_text or content fields, highlight for all fields
                    if field_name in ['full_text', 'content']:
                        context = self._generate_context(original_value, search_text, match_pos)
                        matches.append({
                            'field': field_name,
                            'value': str(field_value),
                            'match_position': match_pos,
                            'match_length': len(search_text),
                            'context': context,
                            'has_context': context != original_value,
                            'is_full_text': True,
                            'should_highlight': True
                        })
                    else:
                        matches.append({
                            'field': field_name,
                            'value': str(field_value),
                            'match_position': match_pos,
                            'match_length': len(search_text),
                            'context': original_value,
                            'has_context': False,
                            'is_full_text': False,
                            'should_highlight': True
                        })
        
        return matches

    def _generate_context(self, text: str, search_text: str, match_pos: int) -> str:
        """Generate context around a match (8 words before and after)"""
        words = text.split()
        search_words = search_text.split()
        
        # If text is short or has few words, return as is
        if len(words) <= 16 or len(search_words) > 1:
            return text
        
        # Find which word contains the match
        word_index = 0
        char_count = 0
        for i, word in enumerate(words):
            if char_count + len(word) > match_pos:
                word_index = i
                break
            char_count += len(word) + 1  # +1 for space
        
        # Get 8 words before and after
        start_word = max(0, word_index - 8)
        end_word = min(len(words), word_index + len(search_words) + 8)
        
        context_words = words[start_word:end_word]
        context = ' '.join(context_words)
        
        # Add ellipses if we truncated
        if start_word > 0:
            context = '... ' + context
        if end_word < len(words):
            context = context + ' ...'
        
        return context

    def _get_highest_classification(self, table_name: str, fields: List[FieldInfo]) -> str:
        """Get the highest classification level found in the database"""
        # Find classification fields
        classification_fields = []
        for field in fields:
            field_name_lower = field.name.lower()
            if field_name_lower in ['classification', 'class', 'security_classification', 'security_class',
                                   'clearance', 'clearance_level', 'sensitivity', 'sensitivity_level']:
                classification_fields.append(field.name)
        
        if not classification_fields:
            return "None"
        
        # Classification hierarchy (highest to lowest)
        classification_hierarchy = [
            'top secret', 'top_secret', 'ts',
            'secret', 's',
            'confidential', 'c',
            'restricted', 'r',
            'unclassified', 'u', 'cui',
            'public', 'p'
        ]
        
        try:
            # Get all unique classification values
            classification_values = set()
            for field_name in classification_fields:
                cursor = self.conn.cursor()
                cursor.execute(f"SELECT DISTINCT {field_name} FROM {table_name} WHERE {field_name} IS NOT NULL")
                values = cursor.fetchall()
                for value in values:
                    if value[0]:
                        classification_values.add(str(value[0]).lower().strip())
            
            if not classification_values:
                return "None"
            
            # Find the highest classification
            for level in classification_hierarchy:
                if level in classification_values:
                    return level.title()
            
            # If no standard classification found, return the first one
            return list(classification_values)[0].title()
            
        except Exception as e:
            print(f"Error getting highest classification: {e}")
            return "Unknown"

    def _calculate_facets(self, table_name: str, facet_fields: List[str], 
                         filters: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate facets for filtering"""
        facets = {}
        
        for field in facet_fields[:5]:  # Limit to 5 facets
            if field not in self.tables[table_name].filterable_fields:
                continue
            
            # Get unique values for this field
            where_conditions = []
            params = []
            
            # Apply other filters (exclude current field)
            if filters:
                for f_field, f_value in filters.items():
                    if f_field != field and f_field in self.tables[table_name].filterable_fields:
                        if isinstance(f_value, list):
                            placeholders = ','.join(['?' for _ in f_value])
                            where_conditions.append(f"{f_field} IN ({placeholders})")
                            params.extend(f_value)
                        else:
                            where_conditions.append(f"{f_field} = ?")
                            params.append(f_value)
            
            where_clause = ""
            if where_conditions:
                where_clause = "WHERE " + " AND ".join(where_conditions)
            
            sql = f"""
                SELECT {field}, COUNT(*) as count
                FROM {table_name}
                {where_clause}
                GROUP BY {field}
                ORDER BY count DESC
                LIMIT 10
            """.strip()
            
            with self._lock:
                cursor = self.conn.cursor()
                cursor.execute(sql, params)
                facets[field] = [{'value': row[0], 'count': row[1]} for row in cursor.fetchall()]
        
        return facets

    def create_fts_index(self, table_name: str, fields: List[str] = None) -> bool:
        """Create FTS5 index for better search performance"""
        if not self._check_fts5_available():
            return False
        
        if table_name not in self.tables:
            return False
        
        table_info = self.tables[table_name]
        if fields is None:
            fields = table_info.searchable_fields[:3]  # Limit to 3 fields
        
        if not fields:
            return False
        
        fts_table_name = f"{table_name}_fts"
        
        try:
            with self._lock:
                cursor = self.conn.cursor()
                
                # Drop existing FTS table if it exists
                cursor.execute(f"DROP TABLE IF EXISTS {fts_table_name}")
                
                # Create FTS5 table
                sql = f"""
                    CREATE VIRTUAL TABLE {fts_table_name} USING fts5(
                        {', '.join(fields)},
                        content='{table_name}',
                        content_rowid='rowid'
                    )
                """
                cursor.execute(sql)
                
                # Populate FTS table
                cursor.execute(f"INSERT INTO {fts_table_name}({fts_table_name}) VALUES('rebuild')")
                
                self.conn.commit()
                self._fts_tables.add(fts_table_name)
                return True
                
        except (sqlite3.Error, OSError) as e:
            print(f"Failed to create FTS index: {e}")
            return False

    def export_kmz(self, table_name: str, query: str = "*", 
                   mgrs_field: str = None, limit: int = 10000) -> Tuple[bytes, Dict[str, Any]]:
        """Export search results as KMZ file"""
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found")
        
        table_info = self.tables[table_name]
        
        # Find MGRS field
        if mgrs_field is None:
            mgrs_fields = table_info.mgrs_fields
            if not mgrs_fields:
                raise ValueError(f"No MGRS fields found in table '{table_name}'")
            mgrs_field = mgrs_fields[0]
        
        # Perform search
        search_result = self.search(table_name, query, size=limit)
        
        if not search_result.hits:
            return b'', {'error': 'No data found for export'}
        
        # Generate KMZ
        from .export_kmz import generate_kmz_from_mgrs
        kmz_bytes = generate_kmz_from_mgrs(search_result.hits, mgrs_field)
        
        metadata = {
            'table': table_name,
            'mgrs_field': mgrs_field,
            'total_rows': len(search_result.hits),
            'query': query,
            'export_size': len(kmz_bytes)
        }
        
        return kmz_bytes, metadata

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
