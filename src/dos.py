import sys
from typing import Optional,List
from model import ConnectionInfo,DatabaseObject,ObjectType
from database import get_connection_string,test_connection,get_table_object
from database import get_view_object,get_function_object,get_procedure_object,get_index_object
from config import REMOTE_DIR
from pathlib import Path

def help_command():
    print("Usage: python dos.py <remote> <database_uri>")
    print()
    print("Arguments:")
    print("  <remote>        : An identifier for the remote database")
    print("  <database_uri>  : The URI used to connect to the database, in the format 'user:password@host/databaseName'")
    print()
    print("Example:")
    print("  python dos.py my_remote_db user:password@host/mydatabase")
    print()
    print("Explanation:")
    print("  - The '<remote>' argument is used to label and organize the output files in a directory named after this identifier")
    print("  - The '<database_uri>' argument specifies the connection details for the database. It should follow the 'user:password@host/databaseName' format")
    print("    - 'user' is the username for the database connection")
    print("    - 'password' is the password for the database connection")
    print("    - 'host' is the address of the database server")
    print("    - 'databaseName' is the name of the specific database to connect to")

def get_connection_info(database_uri:str)->Optional[ConnectionInfo]:
    """
    Return host,database,user,password
    """

    host_separator = database_uri.rfind("@")

    if host_separator<=0:
        return None
    
    host_block = database_uri[host_separator+1:]

    database_separator = host_block.rfind("/")

    if database_separator<=0:
        return None

    host = host_block[:database_separator]

    database = host_block[database_separator+1:]

    credential_block = database_uri[:host_separator]


    user_separator = credential_block.rfind(":")

    if user_separator<=0:
        return None
    
    #print(credential_block)

    user = credential_block[:user_separator]

    password = credential_block[user_separator+1:]

    return ConnectionInfo(host=host,\
                          database=database,\
                          user=user,\
                          password=password)


    
def main(argv):

    if len(argv)==2:
        remote_name = argv[0]

        database_uri = argv[1]

        connection_info = get_connection_info(database_uri=database_uri)

        if connection_info is None:
            help_command()
            return
        
        connection_str = get_connection_string(host=connection_info.host,\
                                               database_name=connection_info.database,\
                                               user=connection_info.user,\
                                               password=connection_info.password)


        print("testing connection:",end="")
        
        if test_connection(connection_str=connection_str):
            print("success")
        else:
            print("fail")
            return
        
        database_objects:List[DatabaseObject] = list()

        print("extracting tables:",end="")

        try:
            database_objects.extend(get_table_object(connection_str=connection_str))
            print("success")
        except:
            print("fail")
            return
        

        print("extracting views:",end="")

        try:
            database_objects.extend(get_view_object(connection_str=connection_str))
            print("success")
        except:
            print("fail")
            return
        
        print("extracting functions:",end="")

        try:
            database_objects.extend(get_function_object(connection_str=connection_str))
            print("success")
        except:
            print("fail")
            return
        
        print("extracting procedures:",end="")

        try:
            database_objects.extend(get_procedure_object(connection_str=connection_str))
            print("success")
        except:
            print("fail")
            return

        print("extracting indexes:",end="")

        try:
            database_objects.extend(get_index_object(connection_str=connection_str))
            print("success")
        except:
            print("fail")
            return
        
        dos_path = Path(REMOTE_DIR)

        if not dos_path.exists():
            print(f"No {dos_path} folder is found.create {dos_path} folder")
            dos_path.mkdir()

        print("saving database objects:",end="")

        try:
            for database_object in database_objects:
                write_database_object(root_dir=REMOTE_DIR,\
                                    remote_name=remote_name,\
                                    database_object=database_object)
            print("success")
        except:
            print("fail")
        



    else:
        help_command()
    

def write_database_object(root_dir:str,remote_name:str,database_object:DatabaseObject):

    database_object_folder = {
        ObjectType.TABLE:"table",
        ObjectType.VIEW:"view",
        ObjectType.FUNCTION:"func",
        ObjectType.PROCEDURE:"procedure",
        ObjectType.INDEX:"index"
    }

    database_file_path = Path(f"{root_dir}/{remote_name}/{database_object_folder[database_object.object_type]}"+\
                         f"/{database_object.object_schema}.{database_object.object_name}.sql")
    
    database_file_path.parent.mkdir(parents=True, exist_ok=True)

    with database_file_path.open("w") as file:
        file.write(database_object.object_definition)


if __name__=="__main__":
    main(sys.argv[1:])