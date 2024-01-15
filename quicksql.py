from enum import Enum
import re

class PartType(Enum):
    WHERE = 3
    ORDERBY = 4
    LIMIT = 5

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

def strip_plural_endings(debug_print, tablename):
    tablename = tablename.strip()

    if debug_print:
        print(tablename)

    if tablename[-3:] == 'ies':

        return tablename[:-3] + 'y'

    elif tablename[-1] == 's':
        if debug_print:
            print("Plural style! Of table " + tablename)
        return tablename[:-1]

    else:
        if debug_print:
            print("Singular style! Of table " + tablename)

        return tablename

def convert_hoffsql_from_to_sql_from_clause(debug_print, frompart):
    frompart = frompart.strip()

    sql = frompart

    first_table = frompart

    if frompart == '' or frompart == None:
        raise Exception("From part needs to be defined!")

    # throw exception if contains spaces
    if ' ' in frompart:
        raise Exception("from part cannot contain spaces!")

    if ',' in frompart:
        tables = frompart.split(',')
        #raise Exception("from part cannot contain commas (yet)!")

        if debug_print:
            print(tables)

        sql = tables[0]
        first_table = tables[0]
        tables = tables[1:]

        for table in tables:
            sql = sql + ' JOIN ' + table + ' USING(' + strip_plural_endings(debug_print, table) + 'ID)'

    # Should split on comma here later, but for now only support one table
    #first_table = frompart

    if debug_print:
        print("RETUREND MAIN TABLE: "+first_table)
    return sql, first_table

def convert_hoffsql_where_to_sql_where_clause(debug_print, where, main_table):
    where = where.strip()
    main_table = main_table.strip()

    pk_name = strip_plural_endings(debug_print, main_table) + "ID"

    if debug_print:
        print("MAIN TABLE: " + main_table)

    if where == '' or where == None:
        raise Exception("where part needs to be defined!")

    if ' ' in where:
        raise Exception("where part cannot contain spaces!")

    if main_table == '' or main_table == None:
        raise Exception("main_table needs to be defined")

    if (re.search('^[0-9]+$', where) != None):
        # Only a number means, take the first table in fromclause and use the name + ID as query
        if debug_print:
            print("main_table : " + main_table)

        # if (main_table[-1:] == "s"):
        #     # table's last letter is s, this is the plural style
        #     if debug_print:
        #         print("Plural style of the main table " + main_table)
        #     pk_name = main_table[:-1] + "ID"
        #     if debug_print:
        #         print("Predicted name of PK: " + pk_name)
        # else:
        #     pk_name = main_table + "ID"
        #     if debug_print:
        #         print("Singular style of the main table " + main_table)
        #         print("Predicted name of PK: " + pk_name)

        return pk_name + " = " + where

    if (re.search('^[0-9,]+$', where) != None):
        if (debug_print):
            print("This is a list of ids, assume they are pks!")
        return pk_name + " IN (" + where + ")"
    else:
        print("Regex found no match!")

    # Just return where clause as is
    return where

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

    elif len(orderby) > 2:
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
            raise Exception("Quicksql does not allow multi column order by clauses (for now)!")

        if debug_print:
            print("Order by column list: " + orderby_columnlist)

    else:
        raise Exception("Impossible case in order by! Order by part: " + orderby)

    if orderby_columnlist == '' or orderby_columnlist == None:
        raise Exception("Order by column list is not defined!")

    if direction == '' or direction == None:
        raise Exception("Direction is not defined!")

    return orderby_columnlist + ' ' + direction


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
            raise Exception("Hoffsql limit part must be an integer! limit part: " + limit)

    return str(limit_rows)

class QuickSQLQuery:
    selectPart = None
    fromPart = None
    wherePart = None
    orderbyPart = None
    limitPart = None

    sqlSelect = None
    sqlFrom = None
    sqlWhere = None
    sqlOrderby = None
    sqlLimit = None

    primaryTable = None

def guess_part_type(print_debug, part_string):
    part_string = part_string.strip()

    if part_string == None or part_string == '':
        raise Exception('No orderby part defined!');

    first_character = part_string[0]

    if re.search("^[0-9]+$", part_string):
        # If the part only contains numbers it must be the where part as that only makes sense for it
        return PartType.WHERE
    elif '=' in part_string:
        # Some characters should indicate that this is where part since they indicate logical expression (which only works in where part)
        return PartType.WHERE
    elif first_character == 'O':
        return PartType.ORDERBY
    elif first_character == 'L':
        return PartType.LIMIT
    else:
        # Default to where
        return PartType.WHERE

def convert_to_sql(print_debug, hoffsql):
    if print_debug:
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

    quickSQLQuery = QuickSQLQuery()

    # STEP ONE is to figure out the part type each element is (or should be)



    # The first part always has to be (and is assumed to be) the select part
    quickSQLQuery.selectPart = list_of_parts[0]

    # The second part always has to be (and is assumed to be) the from part
    # (since it has no special initiating character, as the where, order by and limit parts do)
    quickSQLQuery.fromPart = list_of_parts[1]

    list_of_parts = list_of_parts[2:]

    for part in list_of_parts:
        part_type = guess_part_type(print_debug, part)
        if (part_type == PartType.WHERE):
            if quickSQLQuery.wherePart == None:
                quickSQLQuery.wherePart = part
            else:
                raise Exception("Where part is not null! Two parts seems to be wheres! Current where: " + quickSQLQuery.wherePart + ", new where: " + part)

        elif(part_type == PartType.ORDERBY):
            if quickSQLQuery.orderbyPart == None:
                quickSQLQuery.orderbyPart = part
            else:
                raise Exception("Orderby part is not null! Two parts seems to be orderbys! Current orderby: " + quickSQLQuery.orderbyPart + ", new orderby: " + part)

        elif(part_type == PartType.LIMIT):
            if quickSQLQuery.limitPart == None:
                quickSQLQuery.limitPart = part
            else:
                raise Exception("Limit part is not null! Two parts seems to be limits! Current limit: " + quickSQLQuery.limitPart + ", new limit: " + part)
        else:
            raise Exception("Unsupported part type " + part_type)

    # Now that we know the type of part (for each quicksqlpart) we have to parse them and translate them to sql

    if quickSQLQuery.selectPart.strip() == '' or quickSQLQuery.selectPart == None:
        raise Exception("sql select clause should not be empty string or null at this point!")

    if quickSQLQuery.fromPart.strip() == '' or quickSQLQuery.fromPart == None:
        raise Exception("sql from clause should not be empty string or null at this point!")

    if print_debug:
        print("SELECT PART: " + quickSQLQuery.selectPart)
    quickSQLQuery.sqlSelect = convert_hoffsql_select_to_sql_select_clause(print_debug, quickSQLQuery.selectPart)
    if print_debug:
        print("SELECT clause: " + quickSQLQuery.sqlSelect)



    if print_debug:
        print("FROM PART: " + quickSQLQuery.fromPart)
    quickSQLQuery.sqlFrom, quickSQLQuery.primaryTable = convert_hoffsql_from_to_sql_from_clause(print_debug, quickSQLQuery.fromPart)
    if print_debug:
        print("FROM clause: " + quickSQLQuery.sqlFrom + "\nprimary table " + quickSQLQuery.primaryTable)



    # The rest of the parts are all optional
    if quickSQLQuery.wherePart != None:
        if print_debug:
            print("WHERE PART: " + quickSQLQuery.wherePart)
        quickSQLQuery.sqlWhere = convert_hoffsql_where_to_sql_where_clause(print_debug, quickSQLQuery.wherePart, quickSQLQuery.primaryTable)
        if print_debug:
            print("WHERE clause: " + quickSQLQuery.sqlWhere)



    if quickSQLQuery.orderbyPart != None:
        if print_debug:
            print("ORDERBY PART: " + quickSQLQuery.orderbyPart)
        quickSQLQuery.sqlOrderby = convert_hoffsql_orderby_to_sql_orderby_clause(print_debug, quickSQLQuery.orderbyPart)
        if print_debug:
            print("ORDERBY clause: " + quickSQLQuery.sqlOrderby)



    if quickSQLQuery.limitPart != None:
        if print_debug:
            print("LIMIT PART: " + quickSQLQuery.limitPart)
        quickSQLQuery.sqlLimit = convert_hoffsql_limit_to_sql_limit_clause(print_debug, quickSQLQuery.limitPart)
        if print_debug:
            print("LIMIT clause: " + quickSQLQuery.sqlLimit)



    if quickSQLQuery.sqlSelect.strip() == '' or quickSQLQuery.sqlSelect == None:
        raise Exception("sql select clause should not be empty string at this point!")

    if quickSQLQuery.sqlFrom.strip() == '' or quickSQLQuery.sqlFrom == None:
        raise Exception("sql from clause should not be empty string at this point!")

    sql_query = "SELECT " + quickSQLQuery.sqlSelect + " FROM " + quickSQLQuery.sqlFrom
    if quickSQLQuery.sqlWhere != None:
        if quickSQLQuery.sqlWhere.strip() == '':
            raise Exception("sql where clause should not be empty string at this point!")

        sql_query = sql_query + " WHERE " + quickSQLQuery.sqlWhere

    if quickSQLQuery.sqlOrderby != None:
        if quickSQLQuery.sqlOrderby.strip() == '':
            raise Exception("sql orderby clause should not be empty string at this point!")

        sql_query = sql_query + " ORDER BY " + quickSQLQuery.sqlOrderby

    if quickSQLQuery.sqlLimit != None:
        if quickSQLQuery.sqlLimit.strip() == '':
            raise Exception("sql limit clause should not be empty string at this point!")

        sql_query = sql_query + " LIMIT " + quickSQLQuery.sqlLimit

    sql_query = sql_query + ";"

    if print_debug:
        print("result query: " + sql_query)

    return sql_query
