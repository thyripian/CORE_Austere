"""
Elasticsearch Query Parser for CORE_Austere
Provides comprehensive Elasticsearch-like query capabilities
"""

import re
import json
import sqlite3
from typing import Dict, List, Any, Optional, Tuple, Union
from dataclasses import dataclass
from enum import Enum
import math
from datetime import datetime, date

class QueryType(Enum):
    MATCH = "match"
    MATCH_PHRASE = "match_phrase"
    MATCH_PHRASE_PREFIX = "match_phrase_prefix"
    MULTI_MATCH = "multi_match"
    BOOL = "bool"
    TERM = "term"
    TERMS = "terms"
    RANGE = "range"
    EXISTS = "exists"
    WILDCARD = "wildcard"
    REGEXP = "regexp"
    FUZZY = "fuzzy"
    PREFIX = "prefix"
    QUERY_STRING = "query_string"
    SIMPLE_QUERY_STRING = "simple_query_string"

@dataclass
class QueryClause:
    query_type: QueryType
    field: Optional[str]
    value: Any
    boost: float = 1.0
    operator: str = "or"  # "and" or "or"
    fuzziness: Optional[str] = None
    slop: int = 0  # For phrase queries
    minimum_should_match: Optional[Union[int, str]] = None

class ElasticsearchQueryParser:
    """Parses Elasticsearch-like queries and converts them to SQLite"""
    
    def __init__(self, schema_manager):
        self.schema_manager = schema_manager
        self.stop_words = {
            'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from',
            'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the',
            'to', 'was', 'will', 'with'
        }
        
    def parse_query(self, query: Union[str, Dict[str, Any]], table_name: str) -> Tuple[str, List[Any]]:
        """Parse Elasticsearch query and return SQL WHERE clause and parameters"""
        
        if isinstance(query, str):
            # Simple string query - treat as match query
            return self._parse_match_query(query, table_name, None)
        elif isinstance(query, dict):
            return self._parse_query_dict(query, table_name)
        else:
            raise ValueError("Query must be string or dictionary")
    
    def _parse_query_dict(self, query_dict: Dict[str, Any], table_name: str) -> Tuple[str, List[Any]]:
        """Parse Elasticsearch query dictionary"""
        
        if 'bool' in query_dict:
            return self._parse_bool_query(query_dict['bool'], table_name)
        elif 'match' in query_dict:
            return self._parse_match_query(query_dict['match'], table_name, None)
        elif 'match_phrase' in query_dict:
            return self._parse_match_phrase_query(query_dict['match_phrase'], table_name)
        elif 'multi_match' in query_dict:
            return self._parse_multi_match_query(query_dict['multi_match'], table_name)
        elif 'term' in query_dict:
            return self._parse_term_query(query_dict['term'], table_name)
        elif 'terms' in query_dict:
            return self._parse_terms_query(query_dict['terms'], table_name)
        elif 'range' in query_dict:
            return self._parse_range_query(query_dict['range'], table_name)
        elif 'wildcard' in query_dict:
            return self._parse_wildcard_query(query_dict['wildcard'], table_name)
        elif 'regexp' in query_dict:
            return self._parse_regexp_query(query_dict['regexp'], table_name)
        elif 'query_string' in query_dict:
            return self._parse_query_string(query_dict['query_string'], table_name)
        elif 'simple_query_string' in query_dict:
            return self._parse_simple_query_string(query_dict['simple_query_string'], table_name)
        else:
            raise ValueError(f"Unsupported query type: {list(query_dict.keys())}")
    
    def _parse_bool_query(self, bool_query: Dict[str, Any], table_name: str) -> Tuple[str, List[Any]]:
        """Parse bool query (must, should, must_not, filter)"""
        
        conditions = []
        params = []
        
        # Must clauses (AND)
        if 'must' in bool_query:
            for clause in bool_query['must']:
                where_clause, clause_params = self._parse_query_dict(clause, table_name)
                if where_clause:
                    conditions.append(f"({where_clause})")
                    params.extend(clause_params)
        
        # Should clauses (OR)
        if 'should' in bool_query:
            should_conditions = []
            for clause in bool_query['should']:
                where_clause, clause_params = self._parse_query_dict(clause, table_name)
                if where_clause:
                    should_conditions.append(f"({where_clause})")
                    params.extend(clause_params)
            
            if should_conditions:
                conditions.append(f"({' OR '.join(should_conditions)})")
        
        # Must not clauses (NOT)
        if 'must_not' in bool_query:
            for clause in bool_query['must_not']:
                where_clause, clause_params = self._parse_query_dict(clause, table_name)
                if where_clause:
                    conditions.append(f"NOT ({where_clause})")
                    params.extend(clause_params)
        
        # Filter clauses (same as must but no scoring)
        if 'filter' in bool_query:
            for clause in bool_query['filter']:
                where_clause, clause_params = self._parse_query_dict(clause, table_name)
                if where_clause:
                    conditions.append(f"({where_clause})")
                    params.extend(clause_params)
        
        if not conditions:
            return "", []
        
        return " AND ".join(conditions), params
    
    def _parse_match_query(self, match_query: Union[str, Dict[str, Any]], table_name: str, field: Optional[str] = None) -> Tuple[str, List[Any]]:
        """Parse match query"""
        
        if isinstance(match_query, str):
            query_text = match_query
            boost = 1.0
            operator = "or"
        else:
            if isinstance(match_query, dict) and len(match_query) == 1:
                field = list(match_query.keys())[0]
                match_value = match_query[field]
            else:
                field = match_query.get('field', field)
                match_value = match_query.get('query', match_query.get('value', ''))
            
            query_text = match_value if isinstance(match_value, str) else str(match_value)
            boost = match_query.get('boost', 1.0)
            operator = match_query.get('operator', 'or')
        
        if not field:
            # Search across all text fields
            table_info = self.schema_manager.tables.get(table_name)
            if not table_info:
                return "", []
            
            text_fields = table_info.searchable_fields
            if not text_fields:
                return "", []
            
            conditions = []
            params = []
            
            for text_field in text_fields:
                field_condition, field_params = self._build_text_search_conditions(
                    text_field, query_text, operator, boost
                )
                if field_condition:
                    conditions.append(field_condition)
                    params.extend(field_params)
            
            if conditions:
                return f"({' OR '.join(conditions)})", params
            else:
                return "", []
        else:
            return self._build_text_search_conditions(field, query_text, operator, boost)
    
    def _parse_match_phrase_query(self, match_phrase: Dict[str, Any], table_name: str) -> Tuple[str, List[Any]]:
        """Parse match phrase query"""
        
        if isinstance(match_phrase, dict) and len(match_phrase) == 1:
            field = list(match_phrase.keys())[0]
            phrase = match_phrase[field]
        else:
            field = match_phrase.get('field')
            phrase = match_phrase.get('query', match_phrase.get('value', ''))
        
        if not field:
            return "", []
        
        # For phrase matching, we need exact phrase matching
        # SQLite doesn't have native phrase search, so we use LIKE with word boundaries
        phrase_escaped = phrase.replace("'", "''")
        return f"{field} LIKE ?", [f"%{phrase_escaped}%"]
    
    def _parse_multi_match_query(self, multi_match: Dict[str, Any], table_name: str) -> Tuple[str, List[Any]]:
        """Parse multi-match query"""
        
        query_text = multi_match.get('query', '')
        fields = multi_match.get('fields', [])
        operator = multi_match.get('operator', 'or')
        boost = multi_match.get('boost', 1.0)
        
        if not fields:
            # Use all searchable fields
            table_info = self.schema_manager.tables.get(table_name)
            if table_info:
                fields = table_info.searchable_fields
        
        conditions = []
        params = []
        
        for field in fields:
            field_boost = boost
            if isinstance(field, str) and '^' in field:
                field, boost_str = field.split('^')
                field_boost = float(boost_str)
            
            field_condition, field_params = self._build_text_search_conditions(
                field, query_text, operator, field_boost
            )
            if field_condition:
                conditions.append(field_condition)
                params.extend(field_params)
        
        if conditions:
            return f"({' OR '.join(conditions)})", params
        else:
            return "", []
    
    def _parse_term_query(self, term_query: Dict[str, Any], table_name: str) -> Tuple[str, List[Any]]:
        """Parse term query (exact match)"""
        
        if isinstance(term_query, dict) and len(term_query) == 1:
            field = list(term_query.keys())[0]
            value = term_query[field]
        else:
            field = term_query.get('field')
            value = term_query.get('value')
        
        if not field:
            return "", []
        
        return f"{field} = ?", [value]
    
    def _parse_terms_query(self, terms_query: Dict[str, Any], table_name: str) -> Tuple[str, List[Any]]:
        """Parse terms query (exact match with multiple values)"""
        
        if isinstance(terms_query, dict) and len(terms_query) == 1:
            field = list(terms_query.keys())[0]
            values = terms_query[field]
        else:
            field = terms_query.get('field')
            values = terms_query.get('value', [])
        
        if not field or not values:
            return "", []
        
        placeholders = ','.join(['?' for _ in values])
        return f"{field} IN ({placeholders})", list(values)
    
    def _parse_range_query(self, range_query: Dict[str, Any], table_name: str) -> Tuple[str, List[Any]]:
        """Parse range query"""
        
        conditions = []
        params = []
        
        for field, range_spec in range_query.items():
            field_conditions = []
            
            if 'gte' in range_spec:
                field_conditions.append(f"{field} >= ?")
                params.append(range_spec['gte'])
            if 'gt' in range_spec:
                field_conditions.append(f"{field} > ?")
                params.append(range_spec['gt'])
            if 'lte' in range_spec:
                field_conditions.append(f"{field} <= ?")
                params.append(range_spec['lte'])
            if 'lt' in range_spec:
                field_conditions.append(f"{field} < ?")
                params.append(range_spec['lt'])
            
            if field_conditions:
                conditions.append(f"({' AND '.join(field_conditions)})")
        
        if conditions:
            return " AND ".join(conditions), params
        else:
            return "", []
    
    def _parse_wildcard_query(self, wildcard_query: Dict[str, Any], table_name: str) -> Tuple[str, List[Any]]:
        """Parse wildcard query"""
        
        if isinstance(wildcard_query, dict) and len(wildcard_query) == 1:
            field = list(wildcard_query.keys())[0]
            pattern = wildcard_query[field]
        else:
            field = wildcard_query.get('field')
            pattern = wildcard_query.get('value', wildcard_query.get('wildcard', ''))
        
        if not field:
            return "", []
        
        # Convert Elasticsearch wildcards to SQL LIKE patterns
        sql_pattern = pattern.replace('*', '%').replace('?', '_')
        return f"{field} LIKE ?", [sql_pattern]
    
    def _parse_regexp_query(self, regexp_query: Dict[str, Any], table_name: str) -> Tuple[str, List[Any]]:
        """Parse regexp query"""
        
        if isinstance(regexp_query, dict) and len(regexp_query) == 1:
            field = list(regexp_query.keys())[0]
            pattern = regexp_query[field]
        else:
            field = regexp_query.get('field')
            pattern = regexp_query.get('value', regexp_query.get('regexp', ''))
        
        if not field:
            return "", []
        
        return f"{field} REGEXP ?", [pattern]
    
    def _parse_query_string(self, query_string: Union[str, Dict[str, Any]], table_name: str) -> Tuple[str, List[Any]]:
        """Parse query_string query"""
        
        if isinstance(query_string, str):
            query_text = query_string
            default_field = None
        else:
            query_text = query_string.get('query', '')
            default_field = query_string.get('default_field')
        
        return self._parse_simple_query_string(query_text, table_name, default_field)
    
    def _parse_simple_query_string(self, query_string: Union[str, Dict[str, Any]], table_name: str, default_field: Optional[str] = None) -> Tuple[str, List[Any]]:
        """Parse simple_query_string query"""
        
        if isinstance(query_string, dict):
            query_text = query_string.get('query', '')
            default_field = query_string.get('default_field', default_field)
        else:
            query_text = query_string
        
        # Parse query string syntax
        return self._parse_query_string_syntax(query_text, table_name, default_field)
    
    def _parse_query_string_syntax(self, query_text: str, table_name: str, default_field: Optional[str] = None) -> Tuple[str, List[Any]]:
        """Parse query string syntax (field:value, +required, -excluded, etc.)"""
        
        # Split by spaces but preserve quoted strings
        tokens = self._tokenize_query_string(query_text)
        
        conditions = []
        params = []
        
        for token in tokens:
            if not token:
                continue
            
            # Handle field:value syntax
            if ':' in token and not token.startswith('"'):
                field, value = token.split(':', 1)
                value = value.strip('"')
                
                if value.startswith('*') or value.endswith('*'):
                    # Wildcard query
                    sql_pattern = value.replace('*', '%')
                    conditions.append(f"{field} LIKE ?")
                    params.append(sql_pattern)
                else:
                    # Term query
                    conditions.append(f"{field} = ?")
                    params.append(value)
            
            # Handle quoted phrases
            elif token.startswith('"') and token.endswith('"'):
                phrase = token[1:-1]
                if default_field:
                    conditions.append(f"{default_field} LIKE ?")
                    params.append(f"%{phrase}%")
                else:
                    # Search across all text fields
                    table_info = self.schema_manager.tables.get(table_name)
                    if table_info:
                        for field in table_info.searchable_fields:
                            conditions.append(f"{field} LIKE ?")
                            params.append(f"%{phrase}%")
            
            # Handle required terms (+)
            elif token.startswith('+'):
                term = token[1:].strip('"')
                if default_field:
                    conditions.append(f"{default_field} LIKE ?")
                    params.append(f"%{term}%")
                else:
                    # Search across all text fields
                    table_info = self.schema_manager.tables.get(table_name)
                    if table_info:
                        for field in table_info.searchable_fields:
                            conditions.append(f"{field} LIKE ?")
                            params.append(f"%{term}%")
            
            # Handle excluded terms (-)
            elif token.startswith('-'):
                term = token[1:].strip('"')
                if default_field:
                    conditions.append(f"NOT ({default_field} LIKE ?)")
                    params.append(f"%{term}%")
                else:
                    # Exclude from all text fields
                    table_info = self.schema_manager.tables.get(table_name)
                    if table_info:
                        for field in table_info.searchable_fields:
                            conditions.append(f"NOT ({field} LIKE ?)")
                            params.append(f"%{term}%")
            
            # Handle regular terms
            else:
                term = token.strip('"')
                if default_field:
                    conditions.append(f"{default_field} LIKE ?")
                    params.append(f"%{term}%")
                else:
                    # Search across all text fields
                    table_info = self.schema_manager.tables.get(table_name)
                    if table_info:
                        for field in table_info.searchable_fields:
                            conditions.append(f"{field} LIKE ?")
                            params.append(f"%{term}%")
        
        if conditions:
            return " AND ".join(conditions), params
        else:
            return "", []
    
    def _tokenize_query_string(self, query_text: str) -> List[str]:
        """Tokenize query string preserving quoted strings"""
        
        tokens = []
        current_token = ""
        in_quotes = False
        
        for char in query_text:
            if char == '"' and not in_quotes:
                in_quotes = True
                current_token += char
            elif char == '"' and in_quotes:
                in_quotes = False
                current_token += char
            elif char == ' ' and not in_quotes:
                if current_token:
                    tokens.append(current_token)
                    current_token = ""
            else:
                current_token += char
        
        if current_token:
            tokens.append(current_token)
        
        return tokens
    
    def _build_text_search_conditions(self, field: str, query_text: str, operator: str, boost: float) -> Tuple[List[str], List[Any]]:
        """Build text search conditions for a field"""
        
        # Tokenize the query text
        tokens = self._tokenize_text(query_text)
        
        if operator == "and":
            # All tokens must be present
            conditions = []
            params = []
            for token in tokens:
                conditions.append(f"{field} LIKE ?")
                params.append(f"%{token}%")
            if conditions:
                return f"({' AND '.join(conditions)})", params
            else:
                return "", []
        else:
            # Any token can be present (OR)
            conditions = []
            params = []
            for token in tokens:
                conditions.append(f"{field} LIKE ?")
                params.append(f"%{token}%")
            if conditions:
                return f"({' OR '.join(conditions)})", params
            else:
                return "", []
    
    def _tokenize_text(self, text: str) -> List[str]:
        """Tokenize text for search"""
        
        # Simple tokenization - split on whitespace and punctuation
        tokens = re.findall(r'\b\w+\b', text.lower())
        
        # Remove stop words
        tokens = [token for token in tokens if token not in self.stop_words]
        
        return tokens
    
    def calculate_relevance_score(self, row: Dict[str, Any], query: Union[str, Dict[str, Any]], table_name: str) -> float:
        """Calculate relevance score for a row based on query"""
        
        if isinstance(query, str):
            query_text = query
        else:
            # Extract text from query for scoring
            query_text = self._extract_query_text(query)
        
        if not query_text:
            return 1.0
        
        score = 0.0
        query_tokens = self._tokenize_text(query_text)
        
        table_info = self.schema_manager.tables.get(table_name)
        if not table_info:
            return 1.0
        
        for field in table_info.searchable_fields:
            field_value = str(row.get(field, '')).lower()
            field_score = 0.0
            
            for token in query_tokens:
                if token in field_value:
                    # Basic TF scoring
                    tf = field_value.count(token)
                    field_score += tf
            
            # Apply field boost if available
            score += field_score
        
        return max(score, 0.1)  # Minimum score
    
    def _extract_query_text(self, query: Dict[str, Any]) -> str:
        """Extract text content from query for scoring"""
        
        if 'match' in query:
            match_value = query['match']
            if isinstance(match_value, str):
                return match_value
            elif isinstance(match_value, dict):
                return str(list(match_value.values())[0])
        elif 'query_string' in query:
            return str(query['query_string'])
        elif 'simple_query_string' in query:
            return str(query['simple_query_string'])
        
        return ""
