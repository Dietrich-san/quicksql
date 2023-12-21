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

def convert_hoffsql_select_to_sql_select_clause(debug_print, input):
    input = input.strip()

    if input == '' or input == None:
        raise Exception("Select part needs to be defined!")

    if (input == "!"):
        return "*"

    if ' ' in input:
        raise Exception("Select part cannot contain spaces!")

    if debug_print:
        print("This must be a list of columns, just return it without the exclamation point: " + input[1:])

    return input[1:]

def convert_hoffsql_from_to_sql_from_clause(debug_print, input):
    input = input.strip()
    # throw exception if contains spaces
    if ' ' in input:
        raise Exception("from part cannot contain spaces!")

    if ',' in input:
        raise Exception("from part cannot contain commas (yet)!")

    # Should split on comma here later, but for now only support one table
    first_table = input

    return input, first_table

def convert_hoffsql_where_to_sql_where_clause(debug_print, where, main_table):
    where = where.strip()
    main_table = main_table.strip()

    if where == '' or where == None:
        raise Exception("where part needs to be defined!")

    if ' ' in where:
        raise Exception("where part cannot contain spaces!")

    if main_table == '' or main_table == None:
        raise Exception("main_table needs to be defined")

    if (where.isdigit()):
        # Only a number means, take the first table in fromclause and use the name + ID as query
        print("main_table : " + main_table)

        if (main_table[-1:] == "s"):
            # table's last letter is s, this is the plural style
            if debug_print:
                print("Plural style of the main table " + main_table)
            pk_name = main_table[:-1] + "ID"
            if debug_print:
                print("Predicted name of PK: " + pk_name)
        else:
            pk_name = main_table + "ID"
            if debug_print:
                print("Singular style of the main table " + main_table)
                print("Predicted name of PK: " + pk_name)

        return pk_name + " = " + where
    else:
        # Just return where clause as is
        return where

def convert_hoffsql_limit_to_sql_limit_clause(debug_print, limit):
    limit = limit.strip()
    if limit == None or limit == '':
        raise Exception('No limit part defined!');

    if limit[0] != 'L':
        raise Exception("Limit part must start with \"L\"")

    if ' ' in limit:
        raise Exception("limit part cannot contain spaces!")

    limit = limit[1:]

    if debug_print:
        print("Limit part stripped from L: " + limit)

    # default to 1
    limit_rows = 1

    if limit != '':
        if limit.isdigit():
            limit_rows = limit
        else:
            raise Exception("Cannot parse hoffsql limit part to an integer!")

    return str(limit_rows)

def convert_hoffsql_orderby_to_sql_orderby_clause(debug_print, orderby):
    orderby = orderby.strip()

    if orderby == None or orderby == '':
        raise Exception('No orderby part defined!');

    if orderby[0] != 'O':
        raise Exception("Orderby part must start with \"O\"")

    if ' ' in orderby:
        raise Exception("orderby part cannot contain spaces!")

    if len(orderby) == 1:
        if orderby == 'O':
            # If only an O was indicated, we default to first column (1)
            orderby_columnlist = '1'
            # Default to desc
            direction = 'DESC'
        else:
            raise Exception("The orderby part has to start with O and since the size is 1 it has to be just an O at this point, orderby is " + orderby)

    elif len(orderby) == 2:
        if orderby[1] == 'A':
            if debug_print:
                print("Ascending direction defined")
            direction = 'ASC'

        elif orderby[1] == 'D':
            if debug_print:
                print("Descending direction defined")
            direction = 'DESC'

        else:
            raise Exception("Second character of orderby part must be A for ascending, or D for descending")

        # If only an O and a A/D (direction) was indicated, we default to first column (1)
        orderby_columnlist = '1'

    elif len(orderby) >= 3:
        if orderby[1] == 'A':
            if debug_print:
                print("Ascending direction defined")
            direction = 'ASC'

        elif orderby[1] == 'D':
            if debug_print:
                print("Descending direction defined")
            direction = 'DESC'

        else:
            raise Exception("Second character of orderby part must be A for ascending, or D for descending")

        if orderby[2] != '_':
            raise Exception("The third character of the orderby part must be underscore (_)!")

        orderby_columnlist = orderby[3:]

        if orderby_columnlist == '':
            raise Exception("No column list to order by entered! Must follow the underscore!")

        if ',' in orderby_columnlist:
            raise Exception("Quicksql does not allow multi column order by clauses!")

        if debug_print:
            print("Order by column list: " + orderby_columnlist)

    else:
        raise Exception("Impossible case in order by! Order by part: " + orderby)

    return orderby_columnlist + ' ' + direction

class QuickSQLPart:
    pass

def convert_to_sql(print_debug, hoffsql):
    print("Converting quicksql: " + hoffsql)
    list_of_parts = hoffsql.split(" ")
    if print_debug:
        print(list_of_parts)
        print(len(list_of_parts))

    if len(list_of_parts) < 2:
        raise Exception("Need at least a select and from part, i.e. 2 elements")

    select_part = list_of_parts[0].strip()
    if select_part == '' or select_part == None:
        raise Exception("Select part needs to exist!")

    if print_debug:
        print("SELECT PART: " + select_part)
    sql_select_clause = convert_hoffsql_select_to_sql_select_clause(print_debug, select_part)
    if print_debug:
        print("SELECT clause: " + sql_select_clause)

    from_part = list_of_parts[1].strip()
    if from_part == '' or from_part == None:
        raise Exception("From part needs to exist!")

    if print_debug:
        print("FROM PART: " + from_part)

    sql_from_clause, primary_table = convert_hoffsql_from_to_sql_from_clause(print_debug, from_part)

    if print_debug:
        print("FROM clause: " + sql_from_clause + ", primary table " + primary_table)



    sql_limit_clause = None
    sql_where_clause = None
    sql_orderby_clause = None
    if len(list_of_parts) > 2:
        if list_of_parts[2][0] == 'L':
            # This is the limit part
            if print_debug:
                print("LIMIT PART (in index 2): " + list_of_parts[2])
            sql_limit_clause = convert_hoffsql_limit_to_sql_limit_clause(print_debug, list_of_parts[2])
        elif list_of_parts[2][0] == 'O':
            # Could be orderby part, let's check!
            if len(list_of_parts[2]) == 1:
                if debug_print:
                    print("This is just an O, must be orderby part!")
                sql_orderby_clause = convert_hoffsql_orderby_to_sql_orderby_clause(print_debug, list_of_parts[2])
            #else

            if print_debug:
                print("ORDER BY PART: " + list_of_parts[2])
            sql_orderby_clause = convert_hoffsql_orderby_to_sql_orderby_clause(print_debug, list_of_parts[2])
        else:
            # This is the where part
            if print_debug:
                print("WHERE PART (in index 2): " + list_of_parts[2])
            sql_where_clause = convert_hoffsql_where_to_sql_where_clause(print_debug, list_of_parts[2], primary_table)
            if print_debug:
                print("WHERE clause: " + sql_where_clause)

    if len(list_of_parts) > 3:
        if list_of_parts[3][0] == 'O':
            # This is the orderby part
            if print_debug:
                print("ORDER BY PART: " + list_of_parts[3])

            raise Exception("Cannot have order by part in the 3rd index (fourth element)!")
        elif list_of_parts[3][0] == 'L':
            # This is the limit part

            if print_debug:
                print("LIMIT PART (in index 3): " + list_of_parts[3])
            sql_limit_clause = convert_hoffsql_limit_to_sql_limit_clause(print_debug, list_of_parts[3])
        else:
            # This is by default the where clause
            if print_debug:
                print("WHERE PART (in index 3): " + list_of_parts[3])
            if sql_where_clause != None:
                raise Exception("Where clause cannot be defined yet at this point!")

            sql_where_clause = convert_hoffsql_where_to_sql_where_clause(print_debug, list_of_parts[2], primary_table)
            if print_debug:
                print("WHERE clause: " + sql_where_clause)

    sql_query = "SELECT " + sql_select_clause + " FROM " + sql_from_clause
    if sql_where_clause != None:
        if sql_where_clause.strip() == '':
            raise Exception("sql where clause should not be empty string at this point!")

        sql_query = sql_query + " WHERE " + sql_where_clause

    if sql_orderby_clause != None:
        if sql_orderby_clause.strip() == '':
            raise Exception("sql limit clause should not be empty string at this point!")

        sql_query = sql_query + " ORDER BY " + sql_orderby_clause

    if sql_limit_clause != None:
        if sql_limit_clause.strip() == '':
            raise Exception("sql limit clause should not be empty string at this point!")

        sql_query = sql_query + " LIMIT " + sql_limit_clause

    sql_query = sql_query + ";"

    if print_debug:
        print("result query: " + sql_query)

    return sql_query

def test(actual, expected, throw):
    if (actual == None):
        raise Exception("FAIL, actual cannot be null")
        return

    if (expected == None):
        raise Exception("FAIL, expected cannot be null")
        return

    if (actual == expected):
        print("Test successful! \nActual was: " + actual + "\nWhich was expected!\n")
        return

    if throw:
        raise Exception("\n\nFAIL, expected:\n \"" + expected + "\" \nbut was:\n \"" + actual + "\"")
    else:
        print("FAIL, expected \"" + expected + "\" but was \"" + actual + "\"")

if __name__ == "__main__":

    print ("Test hoffsql to sql:\n\n")

    # These pass:
    # test(convert_to_sql(False, "! Orders"), "SELECT * FROM Orders;", True)
    # test(convert_to_sql(False, "! Orders L"), "SELECT * FROM Orders LIMIT 1;", True)
    # test(convert_to_sql(False, "! Orders L5"), "SELECT * FROM Orders LIMIT 5;", True)
    # test(convert_to_sql(False, "! Orders L1"), "SELECT * FROM Orders LIMIT 1;", True)
    # test(convert_to_sql(False, "! Orders 12345667"), "SELECT * FROM Orders WHERE OrderID = 12345667;", True)
    # test(convert_to_sql(False, "! Orders 12345667 L"), "SELECT * FROM Orders WHERE OrderID = 12345667 LIMIT 1;", True)
    # test(convert_to_sql(False, "! Orders 12345667 L4"), "SELECT * FROM Orders WHERE OrderID = 12345667 LIMIT 4;", True)
    #
    # test(convert_to_sql(False, "!ChainID Orders L2"), "SELECT ChainID FROM Orders LIMIT 2;", True)
    # test(convert_to_sql(False, "!ChainID,OrderID Orders L2"), "SELECT ChainID,OrderID FROM Orders LIMIT 2;", True)
    # test(convert_to_sql(False, "!ChainID Orders 18942810 L"), "SELECT ChainID FROM Orders WHERE OrderID = 18942810 LIMIT 1;", True)
    # test(convert_to_sql(False, "!ChainID Orders 18942810"), "SELECT ChainID FROM Orders WHERE OrderID = 18942810;", True)
    # test(convert_to_sql(False, "!ChainID Orders 18942810 L2"), "SELECT ChainID FROM Orders WHERE OrderID = 18942810 LIMIT 2;", True)
    # test(convert_to_sql(False, "!ChainID,OrderID Orders 1935091 L2"), "SELECT ChainID,OrderID FROM Orders WHERE OrderID = 1935091 LIMIT 2;", True)
    # test(convert_to_sql(False, "!ChainID,OrderID Orders orderstatusid=100 L2"), "SELECT ChainID,OrderID FROM Orders WHERE orderstatusid=100 LIMIT 2;", True)
    # test(convert_to_sql(False, "!chainID,OrderID Orders orderstatusid=100 L2"), "SELECT chainID,OrderID FROM Orders WHERE orderstatusid=100 LIMIT 2;", True)
    #
    # test(convert_to_sql(False, "! Orders orderstatusid=100 L2"), "SELECT * FROM Orders WHERE orderstatusid=100 LIMIT 2;", True)
    # test(convert_to_sql(False, "! Orders orderstatusid=100 L"), "SELECT * FROM Orders WHERE orderstatusid=100 LIMIT 1;", True)
    # test(convert_to_sql(False, "! BankLedger 193401513"), "SELECT * FROM BankLedger WHERE BankLedgerID = 193401513;", True)
    #
    # test(convert_to_sql(False, "!EventID,Processed,ProcessedAs BankLedger 193401513"), "SELECT EventID,Processed,ProcessedAs FROM BankLedger WHERE BankLedgerID = 193401513;", True)
    # test(convert_to_sql(False, "!EventID,Processed BankLedger 193401513 L"), "SELECT EventID,Processed FROM BankLedger WHERE BankLedgerID = 193401513 LIMIT 1;", True)
    # test(convert_to_sql(False, "!EventID,Processed,ProcessedAs BankLedger 193401513 L13"), "SELECT EventID,Processed,ProcessedAs FROM BankLedger WHERE BankLedgerID = 193401513 LIMIT 13;", True)
    #
    # test(convert_to_sql(False, "!EventID,Processed,ProcessedAs BankLedger L2"), "SELECT EventID,Processed,ProcessedAs FROM BankLedger LIMIT 2;", True)
    # test(convert_to_sql(False, "! BankLedger L2"), "SELECT * FROM BankLedger LIMIT 2;", True)
    #
    # test(convert_to_sql(False, "! Events O L"), "SELECT * FROM Events ORDER BY 1 DESC LIMIT 1;", True)
    # test(convert_to_sql(False, "! Events OA L"), "SELECT * FROM Events ORDER BY 1 ASC LIMIT 1;", True)
    # test(convert_to_sql(False, "! Events OD L"), "SELECT * FROM Events ORDER BY 1 DESC LIMIT 1;", True)
    # test(convert_to_sql(False, "! Events O L3"), "SELECT * FROM Events ORDER BY 1 DESC LIMIT 3;", True)
    # test(convert_to_sql(False, "! Events OA L4"), "SELECT * FROM Events ORDER BY 1 ASC LIMIT 4;", True)
    # test(convert_to_sql(False, "! Events OD L5"), "SELECT * FROM Events ORDER BY 1 DESC LIMIT 5;", True)
    #
    # test(convert_to_sql(False, "! Events OA_EventID L"), "SELECT * FROM Events ORDER BY EventID ASC LIMIT 1;", True)
    # test(convert_to_sql(False, "! Events OD_EventID L"), "SELECT * FROM Events ORDER BY EventID DESC LIMIT 1;", True)
    # test(convert_to_sql(False, "! Events OD_EventID L4"), "SELECT * FROM Events ORDER BY EventID DESC LIMIT 4;", True)
    # test(convert_to_sql(False, "!eventid,somecolumn Events OD_EventID L2"), "SELECT eventid,somecolumn FROM Events ORDER BY EventID DESC LIMIT 2;", True)
    # test(convert_to_sql(False, "!eventid,somecolumn Events OA_EventID L1"), "SELECT eventid,somecolumn FROM Events ORDER BY EventID ASC LIMIT 1;", True)
    # test(convert_to_sql(False, "!eventid,somecolumn Events OA_EventID L3"), "SELECT eventid,somecolumn FROM Events ORDER BY EventID ASC LIMIT 3;", True)

    # test(convert_to_sql(False, "! Orders OD_Datestamp L1"), "SELECT * FROM Orders ORDER BY Datestamp DESC LIMIT 1;", True)
    # test(convert_to_sql(False, "! Orders OrderStatusID=100 OD_Datestamp L1"), "SELECT * FROM Orders WHERE OrderStatusID=100 ORDER BY Datestamp DESC LIMIT 1;", True)
    # test(convert_to_sql(False, "!OrderID,chainid Orders OrderStatusID=100 OD_Datestamp L1"), "SELECT * FROM Orders WHERE OrderStatusID=100 ORDER BY Datestamp DESC LIMIT 1;", True)
    # test(convert_to_sql(False, "!OrderID,chainid Orders 12345 OD_Datestamp L1"), "SELECT * FROM Orders WHERE OrderStatusID=100 ORDER BY Datestamp DESC LIMIT 1;", True)
    # test(convert_to_sql(False, "!OrderID,chainid Orders 12345 OA_Datestamp L1"), "SELECT * FROM Orders WHERE OrderStatusID=100 ORDER BY Datestamp DESC LIMIT 1;", True)
    # test(convert_to_sql(False, "!OrderID,chainid Orders 12345 OA L1"), "SELECT * FROM Orders WHERE OrderStatusID=100 ORDER BY Datestamp DESC LIMIT 1;", True)
    # test(convert_to_sql(False, "!OrderID,chainid Orders 12345 OD L1"), "SELECT * FROM Orders WHERE OrderStatusID=100 ORDER BY Datestamp DESC LIMIT 1;", True)
    # test(convert_to_sql(False, "!OrderID,chainid Orders 12345 OA L1"), "SELECT * FROM Orders WHERE OrderStatusID=100 ORDER BY Datestamp DESC LIMIT 1;", True)
    # test(convert_to_sql(False, "!OrderID Orders 12345 OA L1"), "SELECT * FROM Orders WHERE OrderStatusID=100 ORDER BY Datestamp DESC LIMIT 1;", True)

    listOfQuickSQLParts = []
    quickSQLPart = QuickSQLPart()
    quickSQLPart.part = '!'
    quickSQLPart.partType = 'SELECT'
    quickSQLPart.sqlClause = '*'
    listOfQuickSQLParts.append(quickSQLPart)

    for part in listOfQuickSQLParts:
        print("Part: " + part.part + ", part type: " + part.partType + ", sql clause: " + part.sqlClause)
    #print("SQL from filter: " + filter(lambda part: part.partType == 'SELECT', listOfQuickSQLParts))

    print(first(part for part in listOfQuickSQLParts if part.partType == 'SELECT').sqlClause)


    # SELECT name, round(balance,2), currency FROM Accounts JOIN AccountBalances USING(AccountID) WHERE Name = 'CLIENT_BALANCES';
    # !name,round(balance,2),currency Accounts,AccountBalances name='CLIENT_BALANCES'
    #
    # SELECT * FROM Accounts JOIN AccountBalances USING(AccountID) WHERE Name = 'CLIENT_BALANCES';
    # ! Accounts,AccountBalances name='CLIENT_BALANCES'


#    SELECT * FROM BankWithdrawals JOIN BAnkWithrawalTYpes USING(BankWithdrawalTypeID) WHERE BankWithdrawalType = 'SETTLEMENT' ORDER BY datestamp desc LIMIT 1;
#    "! BankWithdrawals,BankWithdrawalTypes BankWithdrawalType='SETTLEMENT' OD L"

    # test(convert_to_sql("! Events OD L"), "SELECT * FROM Events ORDER BY EventID DESC LIMIT 1;", True)
    #
    # test(convert_to_sql("! Events O L30"), "SELECT * FROM Events ORDER BY EventID ASC LIMIT 30;", True)
    # test(convert_to_sql("! Events OD L30"), "SELECT * FROM Events ORDER BY EventID DESC LIMIT 30;", True)
    #
    # test(convert_to_sql("! BankLedger O_Datestamp L"), "SELECT * FROM BankLedger ORDER BY Datestamp ASC LIMIT 1;", True)
    # test(convert_to_sql("! BankLedger OD_Datestamp L"), "SELECT * FROM BankLedger ORDER BY Datestamp DESC LIMIT 1;", True)
    #
    # test(convert_to_sql("! BankLedger O_Datestamp L20"), "SELECT * FROM BankLedger ORDER BY Datestamp ASC LIMIT 20;", True)
    # test(convert_to_sql("! BankLedger OD_Datestamp L20"), "SELECT * FROM BankLedger ORDER BY Datestamp DESC LIMIT 20;", True)
