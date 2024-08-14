import urllib
from sqlalchemy import create_engine,text
from config import GET_VIEW_CODE,GET_TABLE_SQL,GET_FUNCTION_SQL,GET_PROCEDURE_SQL,GET_INDEX_SQL
from typing import List,Dict
from model import DatabaseObject,ObjectType,TableInfo,IndexInfo
from func import group_by


def get_connection_string(host:str,database_name:str,user:str,password:str)->str:

    driver = "{ODBC Driver 17 for SQL Server}"

    odbc_str = f"DRIVER={driver};SERVER={host};PORT=1433;UID={user};DATABASE={database_name};PWD={password}"

    connection_string = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(odbc_str)}"

    return connection_string

def test_connection(connection_str:str)->bool:
    """
    Test whether we can connect to database
    """
    try:
        engine = create_engine(connection_str)

        with engine.connect() as connection:
            return True
    except:
        return False

def get_view_object(connection_str:str)->List[DatabaseObject]:

    result:List[DatabaseObject] = list()

    engine = create_engine(connection_str)

    with engine.connect() as connection:
        result_set = connection.execute(statement=text(GET_VIEW_CODE))

        for row in result_set:
            result.append(DatabaseObject(object_schema=row[0],\
                                     object_name=row[1],\
                                    object_definition=row[2],\
                                    object_type=ObjectType.VIEW))

    return result

def get_procedure_object(connection_str:str)->List[DatabaseObject]:

    result:List[DatabaseObject] = list()

    engine = create_engine(connection_str)

    with engine.connect() as connection:
        result_set = connection.execute(statement=text(GET_PROCEDURE_SQL))

        for row in result_set:
            result.append(DatabaseObject(object_schema=row[0],\
                                     object_name=row[1],\
                                    object_definition=row[2],\
                                    object_type=ObjectType.PROCEDURE))

    return result

def get_function_object(connection_str:str)->List[DatabaseObject]:

    result:List[DatabaseObject] = list()

    engine = create_engine(connection_str)

    with engine.connect() as connection:
        result_set = connection.execute(statement=text(GET_FUNCTION_SQL))

        for row in result_set:
            result.append(DatabaseObject(object_schema=row[0],\
                                     object_name=row[1],\
                                    object_definition=row[2],\
                                    object_type=ObjectType.FUNCTION))

    return result

def get_table_object(connection_str:str)->List[DatabaseObject]:
    result:List[DatabaseObject] = list()

    engine = create_engine(connection_str)

    table_info:List[TableInfo] = list()

    with engine.connect() as connection:
        result_set = connection.execute(statement=text(GET_TABLE_SQL))

        for row in result_set:
            table_info.append(TableInfo(
                table_schema=row[0],
                table_name=row[1],
                column_name=row[2],
                data_type=row[3],
                data_type_length=row[4],
                is_nullable=row[5],
                ordinal_position=int(row[6])
            ))

    result = create_table_object(table_info=table_info)

    return result


def get_index_object(connection_str:str)->List[DatabaseObject]:
    result:List[DatabaseObject] = list()

    engine = create_engine(connection_str)

    index_info:List[IndexInfo] = list()

    with engine.connect() as connection:
        result_set = connection.execute(statement=text(GET_INDEX_SQL))

        for row in result_set:
            index_info.append(IndexInfo(
                index_schema=row[0],
                table_name=row[1],
                index_name=row[2],
                index_column_name=row[3],
                index_type=row[4],
                column_position=row[5],
                is_included_column=row[6],
                is_descending_key=row[7]
            ))

    result = create_index_object(index_info=index_info)

    return result

def create_table_object(table_info:List[TableInfo])->List[DatabaseObject]:

    table_object:List[DatabaseObject] = list()

    grouped_table_info:Dict[str,List[TableInfo]] = group_by(objects=table_info,\
                                                            group_key_func=lambda x: x.table_schema+"."+x.table_name)

    for object_key in grouped_table_info:
        sorted_table_info = sorted(grouped_table_info[object_key],\
                                   key=lambda x:x.ordinal_position)
        
        first_object = sorted_table_info[0]
        
        table_definition = f"CREATE TABLE [{first_object.table_schema}].[{first_object.table_name}]\n(\n"

        for index in range(len(sorted_table_info)):

            x = sorted_table_info[index]

            if x.data_type_length is None:
                table_definition+=f"[{x.column_name}] {x.data_type} "
            else:
                if x.data_type_length=="max":
                    table_definition+=f"[{x.column_name}] {x.data_type}(max) "
                else:
                    if "(" not in x.data_type_length:
                        x.data_type_length="("+x.data_type_length+")"

                    table_definition+=f"[{x.column_name}] {x.data_type}{x.data_type_length} "

            if x.is_nullable=="YES":
                table_definition+="NULL"
            else:
                table_definition+="NOT NULL"

            is_last_column = index==len(sorted_table_info)-1

            if not is_last_column:
                table_definition+=","
            
            table_definition+="\n"


        table_definition+=");"

        table_object.append(DatabaseObject(object_schema=first_object.table_schema,\
                                           object_name=first_object.table_name,\
                                            object_definition=table_definition,\
                                            object_type=ObjectType.TABLE))
        
    return table_object

def create_index_object(index_info:List[IndexInfo])->List[DatabaseObject]:

    index_object:List[DatabaseObject] = list()


    grouped_index_info:Dict[str,List[IndexInfo]] = group_by(objects=index_info,\
                                                            group_key_func=lambda x: x.index_schema+"."+x.table_name+"."+x.index_name)
    
    for object_key in grouped_index_info:    

        sorted_index_column = sorted([x for x in grouped_index_info[object_key] if not x.is_included_column],\
                                     key=lambda x:x.column_position)

        sorted_include_column = sorted([x for x in grouped_index_info[object_key] if x.is_included_column],\
                                     key=lambda x:x.column_position) 

        first_object = grouped_index_info[object_key][0]

        index_definition = f"CREATE {first_object.index_type} INDEX [{first_object.index_name}]\n"
        index_definition+= f"ON [{first_object.index_schema}].[{first_object.table_name}] ("

        for index in range(len(sorted_index_column)):

            x = sorted_index_column[index]

            index_definition+=f"[{x.index_column_name}] "

            if x.is_descending_key:
                index_definition+="DESC"
            else:
                index_definition+="ASC"

            is_last_column = index==len(sorted_index_column)

            if not is_last_column:
                index_definition+=","

        index_definition+=")"

        has_included_column = len(sorted_include_column)>0

        if has_included_column:
            index_definition+="\nINCLUDE ("

            for index in range(len(sorted_include_column)):

                x = sorted_include_column[index]

                index_definition+=f"[{x.index_column_name}] "

                if x.is_descending_key:
                    index_definition+="DESC"
                else:
                    index_definition+="ASC"

                is_last_column = index==len(sorted_index_column)

                if not is_last_column:
                    index_definition+=","


            index_definition+=")"
        
        index_definition+=";"


        index_object.append(DatabaseObject(object_schema=first_object.index_schema,\
                                           object_name=first_object.index_name,\
                                            object_definition=index_definition,\
                                            object_type=ObjectType.INDEX))

    return index_object
