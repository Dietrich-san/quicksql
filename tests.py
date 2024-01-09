from enum import Enum
import re
from quicksql import convert_to_sql

def test_quicksql_convert(quicksql, sql, throw_on_fail):
    print("Attempting to convert quicksql: " + quicksql)

    test(convert_to_sql(False, quicksql), sql, throw_on_fail)

def test(actual, expected, throw):
    if (actual == None):
        raise Exception("FAIL, actual cannot be null")
        return

    if (expected == None):
        raise Exception("FAIL, expected cannot be null")
        return

    if (actual == expected):
        print("Test successful! \nActual was: " + actual + "\n")
        return

    if throw:
        raise Exception("\n\nFAIL, expected:\n \"" + expected + "\" \nbut was:\n \"" + actual + "\"")
    else:
        print("FAIL, expected \"" + expected + "\" but was \"" + actual + "\"")

if __name__ == "__main__":

    print ("Test hoffsql to sql:\n\n")

    # These pass:
    test(convert_to_sql(False, "! Orders"), "SELECT * FROM Orders;", True)
    test_quicksql_convert("! Orders", "SELECT * FROM Orders;", True)
    test_quicksql_convert("! Orders L", "SELECT * FROM Orders LIMIT 1;", True)
    test_quicksql_convert("! Orders L5", "SELECT * FROM Orders LIMIT 5;", True)
    test_quicksql_convert("! Orders L1", "SELECT * FROM Orders LIMIT 1;", True)
    test_quicksql_convert("! Orders 12345667", "SELECT * FROM Orders WHERE OrderID = 12345667;", True)
    test_quicksql_convert("! Orders 12345667 L", "SELECT * FROM Orders WHERE OrderID = 12345667 LIMIT 1;", True)
    test_quicksql_convert("! Orders 12345667 L4", "SELECT * FROM Orders WHERE OrderID = 12345667 LIMIT 4;", True)
    test_quicksql_convert("!ChainID Orders L2", "SELECT ChainID FROM Orders LIMIT 2;", True)
    test_quicksql_convert("!ChainID,OrderID Orders L2", "SELECT ChainID,OrderID FROM Orders LIMIT 2;", True)
    test_quicksql_convert("!ChainID Orders 18942810 L", "SELECT ChainID FROM Orders WHERE OrderID = 18942810 LIMIT 1;", True)
    test_quicksql_convert("!ChainID Orders 18942810", "SELECT ChainID FROM Orders WHERE OrderID = 18942810;", True)
    test_quicksql_convert("!ChainID Orders 18942810 L2", "SELECT ChainID FROM Orders WHERE OrderID = 18942810 LIMIT 2;", True)
    test_quicksql_convert("!ChainID,OrderID Orders 1935091 L2", "SELECT ChainID,OrderID FROM Orders WHERE OrderID = 1935091 LIMIT 2;", True)
    test_quicksql_convert("!ChainID,OrderID Orders orderstatusid=100 L2", "SELECT ChainID,OrderID FROM Orders WHERE orderstatusid=100 LIMIT 2;", True)
    test_quicksql_convert("!chainID,OrderID Orders orderstatusid=100 L2", "SELECT chainID,OrderID FROM Orders WHERE orderstatusid=100 LIMIT 2;", True)
    test_quicksql_convert("! Orders orderstatusid=100 L2", "SELECT * FROM Orders WHERE orderstatusid=100 LIMIT 2;", True)
    test_quicksql_convert("! Orders orderstatusid=100 L", "SELECT * FROM Orders WHERE orderstatusid=100 LIMIT 1;", True)
    test_quicksql_convert("! BankLedger 193401513", "SELECT * FROM BankLedger WHERE BankLedgerID = 193401513;", True)
    test_quicksql_convert("!EventID,Processed,ProcessedAs BankLedger 193401513", "SELECT EventID,Processed,ProcessedAs FROM BankLedger WHERE BankLedgerID = 193401513;", True)
    test_quicksql_convert("!EventID,Processed BankLedger 193401513 L", "SELECT EventID,Processed FROM BankLedger WHERE BankLedgerID = 193401513 LIMIT 1;", True)
    test_quicksql_convert("!EventID,Processed,ProcessedAs BankLedger 193401513 L13", "SELECT EventID,Processed,ProcessedAs FROM BankLedger WHERE BankLedgerID = 193401513 LIMIT 13;", True)
    test_quicksql_convert("!EventID,Processed,ProcessedAs BankLedger L2", "SELECT EventID,Processed,ProcessedAs FROM BankLedger LIMIT 2;", True)
    test_quicksql_convert("! BankLedger L2", "SELECT * FROM BankLedger LIMIT 2;", True)
    test_quicksql_convert("! Events O L", "SELECT * FROM Events ORDER BY 1 DESC LIMIT 1;", True)
    test_quicksql_convert("! Events OA L", "SELECT * FROM Events ORDER BY 1 ASC LIMIT 1;", True)
    test_quicksql_convert("! Events OD L", "SELECT * FROM Events ORDER BY 1 DESC LIMIT 1;", True)
    test_quicksql_convert("! Events O L3", "SELECT * FROM Events ORDER BY 1 DESC LIMIT 3;", True)
    test_quicksql_convert("! Events OA L4", "SELECT * FROM Events ORDER BY 1 ASC LIMIT 4;", True)
    test_quicksql_convert("! Events OD L5", "SELECT * FROM Events ORDER BY 1 DESC LIMIT 5;", True)
    test_quicksql_convert("! Events OA_EventID L", "SELECT * FROM Events ORDER BY EventID ASC LIMIT 1;", True)
    test_quicksql_convert("! Events OD_EventID L", "SELECT * FROM Events ORDER BY EventID DESC LIMIT 1;", True)
    test_quicksql_convert("! Events OD_EventID L4", "SELECT * FROM Events ORDER BY EventID DESC LIMIT 4;", True)
    test_quicksql_convert("!eventid,somecolumn Events OD_EventID L2", "SELECT eventid,somecolumn FROM Events ORDER BY EventID DESC LIMIT 2;", True)
    test_quicksql_convert("!eventid,somecolumn Events OA_EventID L1", "SELECT eventid,somecolumn FROM Events ORDER BY EventID ASC LIMIT 1;", True)
    test_quicksql_convert("!eventid,somecolumn Events OA_EventID L3", "SELECT eventid,somecolumn FROM Events ORDER BY EventID ASC LIMIT 3;", True)
    test_quicksql_convert("! Orders OD_Datestamp L1", "SELECT * FROM Orders ORDER BY Datestamp DESC LIMIT 1;", True)
    test_quicksql_convert("! Orders OrderStatusID=100 OD_Datestamp L1", "SELECT * FROM Orders WHERE OrderStatusID=100 ORDER BY Datestamp DESC LIMIT 1;", True)
    test_quicksql_convert("!OrderID,chainid Orders OrderStatusID=100 OD_Datestamp L1", "SELECT OrderID,chainid FROM Orders WHERE OrderStatusID=100 ORDER BY Datestamp DESC LIMIT 1;", True)
    test_quicksql_convert("! Events OD L", "SELECT * FROM Events ORDER BY 1 DESC LIMIT 1;", True)
    test_quicksql_convert("! Events O L30", "SELECT * FROM Events ORDER BY 1 DESC LIMIT 30;", True)
    test_quicksql_convert("! Events OD L30", "SELECT * FROM Events ORDER BY 1 DESC LIMIT 30;", True)
    test_quicksql_convert("! BankLedger OA_Datestamp L", "SELECT * FROM BankLedger ORDER BY Datestamp ASC LIMIT 1;", True)
    test_quicksql_convert("! BankLedger OD_Datestamp L", "SELECT * FROM BankLedger ORDER BY Datestamp DESC LIMIT 1;", True)
    test_quicksql_convert("! BankLedger OA_Datestamp L20", "SELECT * FROM BankLedger ORDER BY Datestamp ASC LIMIT 20;", True)
    test_quicksql_convert("! BankLedger OD_Datestamp L20", "SELECT * FROM BankLedger ORDER BY Datestamp DESC LIMIT 20;", True)
    test_quicksql_convert("!currency,bankwithdrawalid,bankwithdrawaltype,username bankwithdrawals,bankwithdrawaltypes,users bankwithdrawaltype='EXPRESS' OD_Datestamp L5", "SELECT currency,bankwithdrawalid,bankwithdrawaltype,username FROM bankwithdrawals JOIN bankwithdrawaltypes USING(bankwithdrawaltypeID) JOIN users USING(userID) WHERE bankwithdrawaltype='EXPRESS' ORDER BY Datestamp DESC LIMIT 5;", True)
    test_quicksql_convert("!name,round(balance,2),currency AccountBalances,Accounts", "SELECT name,round(balance,2),currency FROM AccountBalances JOIN Accounts USING(AccountID);", True)
    test_quicksql_convert("!round(balance,2),currency AccountBalances,Accounts name='CLIENT_BALANCES'", "SELECT round(balance,2),currency FROM AccountBalances JOIN Accounts USING(AccountID) WHERE name='CLIENT_BALANCES';", True)
    test_quicksql_convert("! accountbalances,accounts accounts.name='CLIENT_BALANCES'", "SELECT * FROM accountbalances JOIN accounts USING(accountID) WHERE accounts.name='CLIENT_BALANCES';", True)
    test_quicksql_convert("! accountbalances,accounts name='CLIENT_BALANCES'", "SELECT * FROM accountbalances JOIN accounts USING(accountID) WHERE name='CLIENT_BALANCES';", True)
    test_quicksql_convert("!currency,accountid,accounttypes.name accountbalances,accounts,accounttypes accounts.name='CLIENT_BALANCES'", "SELECT currency,accountid,accounttypes.name FROM accountbalances JOIN accounts USING(accountID) JOIN accounttypes USING(accounttypeID) WHERE accounts.name='CLIENT_BALANCES';", True)
    test_quicksql_convert("!accountbalances.* accountbalances,accounts,accounttypes accounts.name='CLIENT_BALANCES'", "SELECT accountbalances.* FROM accountbalances JOIN accounts USING(accountID) JOIN accounttypes USING(accounttypeID) WHERE accounts.name='CLIENT_BALANCES';", True)






    # test(convert_to_sql(False, "!OrderID,chainid Orders 12345 OD_Datestamp L1"), "SELECT * FROM Orders WHERE OrderStatusID=100 ORDER BY Datestamp DESC LIMIT 1;", True)
    # test(convert_to_sql(False, "!OrderID,chainid Orders 12345 OA_Datestamp L1"), "SELECT * FROM Orders WHERE OrderStatusID=100 ORDER BY Datestamp DESC LIMIT 1;", True)
    # test(convert_to_sql(False, "!OrderID,chainid Orders 12345 OA L1"), "SELECT * FROM Orders WHERE OrderStatusID=100 ORDER BY Datestamp DESC LIMIT 1;", True)
    # test(convert_to_sql(False, "!OrderID,chainid Orders 12345 OD L1"), "SELECT * FROM Orders WHERE OrderStatusID=100 ORDER BY Datestamp DESC LIMIT 1;", True)
    # test(convert_to_sql(False, "!OrderID,chainid Orders 12345 OA L1"), "SELECT * FROM Orders WHERE OrderStatusID=100 ORDER BY Datestamp DESC LIMIT 1;", True)

    # Not sure what this one was supposed to be
    #test(convert_to_sql(True, "!OrderID Orders 12345 OA L1"), "SELECT * FROM Orders WHERE OrderStatusID=100 ORDER BY Datestamp DESC LIMIT 1;", True)




    # SELECT name, round(balance,2), currency FROM Accounts JOIN AccountBalances USING(AccountID) WHERE Name = 'CLIENT_BALANCES';
    # !name,round(balance,2),currency Accounts,AccountBalances name='CLIENT_BALANCES'
    #
    # SELECT * FROM Accounts JOIN AccountBalances USING(AccountID) WHERE Name = 'CLIENT_BALANCES';
    # ! Accounts,AccountBalances name='CLIENT_BALANCES'


#    SELECT * FROM BankWithdrawals JOIN BAnkWithrawalTYpes USING(BankWithdrawalTypeID) WHERE BankWithdrawalType = 'SETTLEMENT' ORDER BY datestamp desc LIMIT 1;
#    "! BankWithdrawals,BankWithdrawalTypes BankWithdrawalType='SETTLEMENT' OD L"

    # test(convert_to_sql("! paypal.Statements (7078, 7090, 8001)"), "SELECT * FROM paypal.Statements WHERE StatementID IN (7078, 7090, 8001)", True)
