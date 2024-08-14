from dataclasses import dataclass
from enum import Enum

class ObjectType(str,Enum):
    TABLE="table",
    VIEW="view",
    INDEX="index",
    FUNCTION="function",
    PROCEDURE="procedure"


@dataclass
class DatabaseObject:
    object_schema:str
    object_name:str
    object_definition:str
    object_type:ObjectType

@dataclass
class TableInfo:
    table_schema:str
    table_name:str
    column_name:str
    data_type:str
    data_type_length:str
    is_nullable:str
    ordinal_position:int


@dataclass
class IndexInfo:
    index_schema:str
    table_name:str
    index_name:str
    index_column_name:str
    index_type:str
    column_position:int
    is_included_column:bool
    is_descending_key:bool

@dataclass
class ConnectionInfo:
    host:str
    database:str
    user:str
    password:str



