from database import create_table_object,create_index_object
from model import TableInfo,ObjectType,IndexInfo

def test_single_column_table():

    column = TableInfo(table_schema="test",\
              table_name="foo",\
              column_name="c1",
              data_type="varchar",\
              data_type_length="max",
              is_nullable="YES",\
              ordinal_position=1)
    
    database_objects = create_table_object(table_info=[column])

    assert len(database_objects)==1

    assert database_objects[0].object_schema=="test"
    assert database_objects[0].object_name=="foo"
    assert database_objects[0].object_type==ObjectType.TABLE

    expected_object_definition = "CREATE TABLE [test].[foo]([c1] varchar(max) NULL);".split(" ")

    actual_object_definition = database_objects[0].object_definition.replace("\n","").split(" ")

    assert len(expected_object_definition)==len(actual_object_definition)

    for index in range(len(expected_object_definition)):
        assert expected_object_definition[index]==actual_object_definition[index]


def test_multiple_column_table():

    str_column = TableInfo(table_schema="test",\
              table_name="foo",\
              column_name="c1",
              data_type="varchar",\
              data_type_length="max",
              is_nullable="YES",\
              ordinal_position=1)
    
    int_column = TableInfo(table_schema="test",\
              table_name="foo",\
              column_name="c2",
              data_type="int",\
              data_type_length=None,
              is_nullable="NO",\
              ordinal_position=2)
        
    database_objects = create_table_object(table_info=[str_column,int_column])

    assert len(database_objects)==1

    assert database_objects[0].object_schema=="test"
    assert database_objects[0].object_name=="foo"
    assert database_objects[0].object_type==ObjectType.TABLE

    expected_object_definition = "CREATE TABLE [test].[foo]([c1] varchar(max) NULL,[c2] int NOT NULL);".split(" ")

    actual_object_definition = database_objects[0].object_definition.replace("\n","").split(" ")

    assert len(expected_object_definition)==len(actual_object_definition)

    for index in range(len(expected_object_definition)):
        assert expected_object_definition[index]==actual_object_definition[index]

def test_single_index_column_single_include_column_index():

    index_column = IndexInfo(index_schema="test",\
                             table_name="foo",\
                             index_name="idx_test",\
                             index_column_name="c1",
                             index_type="NON CLUSTERED",
                             column_position=1,
                             is_included_column=False,
                             is_descending_key=True)
    
    include_column = IndexInfo(index_schema="test",\
                             table_name="foo",\
                             index_name="idx_test",\
                             index_column_name="ic1",
                             index_type="NON CLUSTERED",
                             column_position=2,
                             is_included_column=True,
                             is_descending_key=True)
    
    database_objects = create_index_object(index_info=[index_column,include_column])

    assert len(database_objects)==1

    assert database_objects[0].object_schema=="test"
    assert database_objects[0].object_name=="idx_test"
    assert database_objects[0].object_type==ObjectType.INDEX

    expected_object_definition = "CREATE NON CLUSTERED INDEX [idx_test]ON [test].[foo] ([c1] DESC)INCLUDE ([ic1] DESC);".split(" ")

    actual_object_definition = database_objects[0].object_definition.replace("\n","").split(" ")

    assert len(expected_object_definition)==len(actual_object_definition)

    for index in range(len(expected_object_definition)):
        assert expected_object_definition[index]==actual_object_definition[index]
    


def test_multiple_index_column_multiple_include_column_index():

    index_column_1 = IndexInfo(index_schema="test",\
                             table_name="foo",\
                             index_name="idx_test",\
                             index_column_name="c1",
                             index_type="NON CLUSTERED",
                             column_position=1,
                             is_included_column=False,
                             is_descending_key=True)
    
    index_column_2 = IndexInfo(index_schema="test",\
                             table_name="foo",\
                             index_name="idx_test",\
                             index_column_name="c2",
                             index_type="NON CLUSTERED",
                             column_position=2,
                             is_included_column=False,
                             is_descending_key=True)
    

    include_column_1 = IndexInfo(index_schema="test",\
                             table_name="foo",\
                             index_name="idx_test",\
                             index_column_name="ic1",
                             index_type="NON CLUSTERED",
                             column_position=3,
                             is_included_column=True,
                             is_descending_key=True)
    
    include_column_2 = IndexInfo(index_schema="test",\
                             table_name="foo",\
                             index_name="idx_test",\
                             index_column_name="ic2",
                             index_type="NON CLUSTERED",
                             column_position=4,
                             is_included_column=True,
                             is_descending_key=True)
    
    database_objects = create_index_object(index_info=[index_column_1,\
                                                       index_column_2,\
                                                       include_column_1,\
                                                       include_column_2])

    assert len(database_objects)==1

    assert database_objects[0].object_schema=="test"
    assert database_objects[0].object_name=="idx_test"
    assert database_objects[0].object_type==ObjectType.INDEX

    print(database_objects[0].object_definition)
    
    expected_object_definition = "CREATE NON CLUSTERED INDEX [idx_test]ON [test].[foo] ([c1] DESC,[c2] DESC)INCLUDE ([ic1] DESC,[ic2] DESC);".split(" ")

    actual_object_definition = database_objects[0].object_definition.replace("\n","").split(" ")

    assert len(expected_object_definition)==len(actual_object_definition)

    for index in range(len(expected_object_definition)):
        assert expected_object_definition[index]==actual_object_definition[index]
    


def test_main():
    test_single_column_table()
    test_multiple_column_table()
    test_single_index_column_single_include_column_index()
    test_multiple_index_column_multiple_include_column_index()

if __name__=="__main__":
    test_main()