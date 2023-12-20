# ! Orders -> SELECT * FROM Orders;
# ! Orders L -> SELECT * FROM Orders LIMIT 1;
# ! Orders L2 -> SELECT * FROM Orders LIMIT 2;
# ! Orders L1 -> SELECT * FROM Orders LIMIT 1;
# ! Orders 12345667 -> SELECT * FROM Orders WHERE OrderID = 12345667;
# ! Orders 2152 -> SELECT * FROM Orders WHERE OrderID = 2152;
# ! Orders OrderTypeID=15 -> SELECT * FROM Orders WHERE OrderTypeID = 15;
# ! Orders 111321 L -> SELECT * FROM Orders WHERE OrderID = 11321 LIMIT 1;

# !ChainID Orders -> SELECT ChainID FROM Orders;
# !ChainID Orders L -> SELECT ChainID FROM Orders LIMIT 1;
# !ChainID Orders L2 -> SELECT ChainID FROM Orders LIMIT 2;
# !ChainID Orders L1 -> SELECT ChainID FROM Orders LIMIT 1;
# !ChainID Orders 12345667 -> SELECT ChainID FROM Orders WHERE OrderID = 12345667;
# !ChainID Orders 2152 -> SELECT ChainID FROM Orders WHERE OrderID = 2152;
# !ChainID Orders OrderTypeID=15 -> SELECT ChainID FROM Orders WHERE OrderTypeID = 15;
# !ChainID Orders 111321 L -> SELECT ChainID FROM Orders WHERE OrderID = 11321 LIMIT 1;

# !ChainID Orders 8888888 -> SELECT ChainID FROM Orders WHERE OrderID = 8888888;
# !ChainID,OrderTypeID Orders -> SELECT ChainID,OrderTypeID FROM Orders;
# !ChainID,OrderTypeID Orders 123 -> SELECT ChainID,OrderTypeID FROM Orders WHERE OrderID = 123;
# !ChainID,UserID Orders OrderTypeID=15 -> SELECT ChainID,OrderTypeID FROM Orders WHERE OrderTypeID = 15;
# !ChainID,UserID Orders OrderTypeID=15 L -> SELECT ChainID,OrderTypeID FROM Orders WHERE OrderTypeID = 15 LIMIT 1;
# !ChainID,UserID Orders OrderTypeID=15 L58 -> SELECT ChainID,OrderTypeID FROM Orders WHERE OrderTypeID = 15 LIMIT 58;
# ! Orders X,Y -> SELECT * FROM Orders WHERE OrderID IN (X,Y);

def convert_hoffsql_select_to_sql_select_clause(input):
    input = input.strip()

    if input == '' or input == None:
        raise Exception("Select part needs to be defined!")

    if (input == "!"):
        return "*"

    if ' ' in input:
        raise Exception("Select part cannot contain spaces!")

    print("This must be a list of columns, just return it without the exclamation point: " + input[1:])
    return input[1:]

def convert_hoffsql_from_to_sql_from_clause(input):
    input = input.strip()
    # throw exception if contains spaces
    if ' ' in input:
        raise Exception("from part cannot contain spaces!")

    if ',' in input:
        raise Exception("from part cannot contain commas (yet)!")

    # Should split on comma here later, but for now only support one table
    first_table = input

    return input, first_table

def convert_hoffsql_where_to_sql_where_clause(where, main_table):
    where = where.strip()
    main_table = main_table.strip()

    if where == '' or where == None:
        raise Exception("where part needs to be defined!")

    if ' ' in input:
        raise Exception("where part cannot contain spaces!")

    if main_table == '' or main_table == None:
        raise Exception("main_table needs to be defined")

    if (where.isdigit()):
        # Only a number means, take the first table in fromclause and use the name + ID as query
        print("main_table : " + main_table)

        if (main_table[-1:] == "s"):
            # table's last letter is s, this is the plural style
            print("Plural style of the main table " + main_table)
            pk_name = main_table[:-1] + "ID"
            print("Predicted name of PK: " + pk_name)
        else:
            pk_name = main_table + "ID"
            print("Singular style of the main table " + main_table)
            print("Predicted name of PK: " + pk_name)

        return pk_name + " = " + where
    else:
        # Just return where clause as is
        return where

def convert_hoffsql_limit_to_sql_limit_clause(limit):
    limit = limit.strip()
    if limit == None or limit == '':
        raise Exception('No limit part defined!');

    if limit[0] != 'L':
        raise Exception("Limit part must start with \"L\"")

    limit = limit[1:]

    print("Limit part stripped from L: " + limit)

    # default to 1
    limit_rows = 1

    if limit != '':
        if limit.isdigit():
            limit_rows = limit
        else:
            raise Exception("Cannot parse hoffsql limit part to an integer!")

    return str(limit_rows)

def convert_to_sql(hoffsql):



    list_of_parts = hoffsql.split(" ")
    print(list_of_parts)
    print(len(list_of_parts))

    if len(list_of_parts) < 2:
        raise Exception("Need at least a select and from part, i.e. 2 elements")

    select_part = list_of_parts[0].strip()
    if select_part == '' or select_part == None:
        raise Exception("Select part needs to exist!")

    print("SELECT PART: " + select_part)
    sql_select_clause = convert_hoffsql_select_to_sql_select_clause(select_part)
    print("SELECT clause: " + sql_select_clause)

    from_part = list_of_parts[1].strip()
    if from_part == '' or from_part == None:
        raise Exception("From part needs to exist!")

    print("FROM PART: " + from_part)

    sql_from_clause, primary_table = convert_hoffsql_from_to_sql_from_clause(from_part)
    print("FROM clause: " + sql_from_clause + ", primary table " + primary_table)

    sql_limit_clause = None
    sql_where_clause = None
    if len(list_of_parts) > 2:
        if list_of_parts[2][0] == 'L':
            # This is the limit part
            print("LIMIT PART: " + list_of_parts[2])
            sql_limit_clause = convert_hoffsql_limit_to_sql_limit_clause(list_of_parts[2])
        else:
            # This is the where part
            print("WHERE PART: " + list_of_parts[2])
            sql_where_clause = convert_hoffsql_where_to_sql_where_clause(list_of_parts[2], primary_table)
            print("WHERE clause: " + sql_where_clause)

    if len(list_of_parts) > 3:
        if list_of_parts[3][0] != 'L':
            raise Exception("This third part of the hoffsql query must be a limit clause at this part, but doesn't start with L")
        print("LIMIT PART: " + list_of_parts[3])
        sql_limit_clause = convert_hoffsql_limit_to_sql_limit_clause(list_of_parts[3])

    sql_query = "SELECT " + sql_select_clause + " FROM " + sql_from_clause
    if sql_where_clause != None:
        if sql_where_clause.strip() == '':
            raise Exception("sql where clause should not be empty string at this point!")

        sql_query = sql_query + " WHERE " + sql_where_clause

    if sql_limit_clause != None:
        if sql_limit_clause.strip() == '':
            raise Exception("sql limit clause should not be empty string at this point!")

        sql_query = sql_query + " LIMIT " + sql_limit_clause

    sql_query = sql_query + ";"

    print("result query: " + sql_query)

    return sql_query

def test(actual, expected, throw):
    if (actual == None):
        print("FAIL actual cannot be null")
        return

    if (expected == None):
        print("FAIL expected cannot be null")
        return

    if (actual == expected):
        print("Test successful! \n\n\n")
        return

    if throw:
        raise Exception("FAIL expected \"" + expected + "\" but was \"" + actual + "\"")
    else:
        print("FAIL expected \"" + expected + "\" but was \"" + actual + "\"")

if __name__ == "__main__":

    print ("Test hoffsql to sql:\n\n")

    # These pass:
    # test(convert_to_sql("! Orders"), "SELECT * FROM Orders;", True)
    # test(convert_to_sql("! Orders L"), "SELECT * FROM Orders LIMIT 1;", True)
    # test(convert_to_sql("! Orders L5"), "SELECT * FROM Orders LIMIT 5;", True)
    # test(convert_to_sql("! Orders L1"), "SELECT * FROM Orders LIMIT 1;", True)
    # test(convert_to_sql("! Orders 12345667"), "SELECT * FROM Orders WHERE OrderID = 12345667;", True)
    # test(convert_to_sql("! Orders 12345667 L"), "SELECT * FROM Orders WHERE OrderID = 12345667 LIMIT 1;", True)
    # test(convert_to_sql("! Orders 12345667 L4"), "SELECT * FROM Orders WHERE OrderID = 12345667 LIMIT 4;", True)
    # test(convert_to_sql("!ChainID Orders L2"), "SELECT ChainID FROM Orders LIMIT 2;", True)
    # test(convert_to_sql("!ChainID,OrderID Orders L2"), "SELECT ChainID,OrderID FROM Orders LIMIT 2;", True)
    # test(convert_to_sql("!ChainID Orders 18942810 L"), "SELECT ChainID FROM Orders WHERE OrderID = 18942810 LIMIT 1;", True)
    # test(convert_to_sql("!ChainID,OrderID Orders 1935091 L2"), "SELECT ChainID,OrderID FROM Orders WHERE OrderID = 1935091 LIMIT 2;", True)

    test(convert_to_sql("! Orders orderstatusid=100 L2"), "SELECT ChainID,OrderID FROM Orders WHERE OrderID = 1935091 LIMIT 2;", True)

    # test(convert_to_hoffsql("! Orders"), "SELECT * FROM Orders;")
    # test(convert_to_hoffsql("! Orders"), "SELECT * FROM Orders;")
    # test(convert_to_hoffsql("! Orders"), "SELECT * FROM Orders;")
    # test(convert_to_hoffsql("! Orders"), "SELECT * FROM Orders;")
    # test(convert_to_hoffsql("! Orders"), "SELECT * FROM Orders;")
    # test(convert_to_hoffsql("! Orders"), "SELECT * FROM Orders;")
    # test(convert_to_hoffsql("! Orders"), "SELECT * FROM Orders;")
    # test(convert_to_hoffsql("! Orders"), "SELECT * FROM Orders;")
    # test(convert_to_hoffsql("! Orders"), "SELECT * FROM Orders;")
    # test(convert_to_hoffsql("! Orders"), "SELECT * FROM Orders;")
