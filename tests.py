from enum import Enum
import re
from quicksql import convert_to_sql

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
    test(convert_to_sql(False, "! Orders"), "SELECT * FROM Orders;", True)
    test(convert_to_sql(False, "! Orders L"), "SELECT * FROM Orders LIMIT 1;", True)
    test(convert_to_sql(False, "! Orders L5"), "SELECT * FROM Orders LIMIT 5;", True)
    test(convert_to_sql(False, "! Orders L1"), "SELECT * FROM Orders LIMIT 1;", True)
    test(convert_to_sql(False, "! Orders 12345667"), "SELECT * FROM Orders WHERE OrderID = 12345667;", True)
    test(convert_to_sql(False, "! Orders 12345667 L"), "SELECT * FROM Orders WHERE OrderID = 12345667 LIMIT 1;", True)
    test(convert_to_sql(False, "! Orders 12345667 L4"), "SELECT * FROM Orders WHERE OrderID = 12345667 LIMIT 4;", True)
    test(convert_to_sql(False, "!ChainID Orders L2"), "SELECT ChainID FROM Orders LIMIT 2;", True)
    test(convert_to_sql(False, "!ChainID,OrderID Orders L2"), "SELECT ChainID,OrderID FROM Orders LIMIT 2;", True)
    test(convert_to_sql(False, "!ChainID Orders 18942810 L"), "SELECT ChainID FROM Orders WHERE OrderID = 18942810 LIMIT 1;", True)
    test(convert_to_sql(False, "!ChainID Orders 18942810"), "SELECT ChainID FROM Orders WHERE OrderID = 18942810;", True)
    test(convert_to_sql(False, "!ChainID Orders 18942810 L2"), "SELECT ChainID FROM Orders WHERE OrderID = 18942810 LIMIT 2;", True)
    test(convert_to_sql(False, "!ChainID,OrderID Orders 1935091 L2"), "SELECT ChainID,OrderID FROM Orders WHERE OrderID = 1935091 LIMIT 2;", True)
    test(convert_to_sql(False, "!ChainID,OrderID Orders orderstatusid=100 L2"), "SELECT ChainID,OrderID FROM Orders WHERE orderstatusid=100 LIMIT 2;", True)
    test(convert_to_sql(False, "!chainID,OrderID Orders orderstatusid=100 L2"), "SELECT chainID,OrderID FROM Orders WHERE orderstatusid=100 LIMIT 2;", True)
    test(convert_to_sql(False, "! Orders orderstatusid=100 L2"), "SELECT * FROM Orders WHERE orderstatusid=100 LIMIT 2;", True)
    test(convert_to_sql(False, "! Orders orderstatusid=100 L"), "SELECT * FROM Orders WHERE orderstatusid=100 LIMIT 1;", True)
    test(convert_to_sql(False, "! BankLedger 193401513"), "SELECT * FROM BankLedger WHERE BankLedgerID = 193401513;", True)
    test(convert_to_sql(False, "!EventID,Processed,ProcessedAs BankLedger 193401513"), "SELECT EventID,Processed,ProcessedAs FROM BankLedger WHERE BankLedgerID = 193401513;", True)
    test(convert_to_sql(False, "!EventID,Processed BankLedger 193401513 L"), "SELECT EventID,Processed FROM BankLedger WHERE BankLedgerID = 193401513 LIMIT 1;", True)
    test(convert_to_sql(False, "!EventID,Processed,ProcessedAs BankLedger 193401513 L13"), "SELECT EventID,Processed,ProcessedAs FROM BankLedger WHERE BankLedgerID = 193401513 LIMIT 13;", True)
    test(convert_to_sql(False, "!EventID,Processed,ProcessedAs BankLedger L2"), "SELECT EventID,Processed,ProcessedAs FROM BankLedger LIMIT 2;", True)
    test(convert_to_sql(False, "! BankLedger L2"), "SELECT * FROM BankLedger LIMIT 2;", True)
    test(convert_to_sql(False, "! Events O L"), "SELECT * FROM Events ORDER BY 1 DESC LIMIT 1;", True)
    test(convert_to_sql(False, "! Events OA L"), "SELECT * FROM Events ORDER BY 1 ASC LIMIT 1;", True)
    test(convert_to_sql(False, "! Events OD L"), "SELECT * FROM Events ORDER BY 1 DESC LIMIT 1;", True)
    test(convert_to_sql(False, "! Events O L3"), "SELECT * FROM Events ORDER BY 1 DESC LIMIT 3;", True)
    test(convert_to_sql(False, "! Events OA L4"), "SELECT * FROM Events ORDER BY 1 ASC LIMIT 4;", True)
    test(convert_to_sql(False, "! Events OD L5"), "SELECT * FROM Events ORDER BY 1 DESC LIMIT 5;", True)
    test(convert_to_sql(False, "! Events OA_EventID L"), "SELECT * FROM Events ORDER BY EventID ASC LIMIT 1;", True)
    test(convert_to_sql(False, "! Events OD_EventID L"), "SELECT * FROM Events ORDER BY EventID DESC LIMIT 1;", True)
    test(convert_to_sql(False, "! Events OD_EventID L4"), "SELECT * FROM Events ORDER BY EventID DESC LIMIT 4;", True)
    test(convert_to_sql(False, "!eventid,somecolumn Events OD_EventID L2"), "SELECT eventid,somecolumn FROM Events ORDER BY EventID DESC LIMIT 2;", True)
    test(convert_to_sql(False, "!eventid,somecolumn Events OA_EventID L1"), "SELECT eventid,somecolumn FROM Events ORDER BY EventID ASC LIMIT 1;", True)
    test(convert_to_sql(False, "!eventid,somecolumn Events OA_EventID L3"), "SELECT eventid,somecolumn FROM Events ORDER BY EventID ASC LIMIT 3;", True)
    test(convert_to_sql(False, "! Orders OD_Datestamp L1"), "SELECT * FROM Orders ORDER BY Datestamp DESC LIMIT 1;", True)
    test(convert_to_sql(False, "! Orders OrderStatusID=100 OD_Datestamp L1"), "SELECT * FROM Orders WHERE OrderStatusID=100 ORDER BY Datestamp DESC LIMIT 1;", True)
    test(convert_to_sql(False, "!OrderID,chainid Orders OrderStatusID=100 OD_Datestamp L1"), "SELECT OrderID,chainid FROM Orders WHERE OrderStatusID=100 ORDER BY Datestamp DESC LIMIT 1;", True)
    test(convert_to_sql(False, "! Events OD L"), "SELECT * FROM Events ORDER BY 1 DESC LIMIT 1;", True)
    test(convert_to_sql(False, "! Events O L30"), "SELECT * FROM Events ORDER BY 1 DESC LIMIT 30;", True)
    test(convert_to_sql(False, "! Events OD L30"), "SELECT * FROM Events ORDER BY 1 DESC LIMIT 30;", True)
    test(convert_to_sql(False, "! BankLedger OA_Datestamp L"), "SELECT * FROM BankLedger ORDER BY Datestamp ASC LIMIT 1;", True)
    test(convert_to_sql(False, "! BankLedger OD_Datestamp L"), "SELECT * FROM BankLedger ORDER BY Datestamp DESC LIMIT 1;", True)
    test(convert_to_sql(False, "! BankLedger OA_Datestamp L20"), "SELECT * FROM BankLedger ORDER BY Datestamp ASC LIMIT 20;", True)
    test(convert_to_sql(False, "! BankLedger OD_Datestamp L20"), "SELECT * FROM BankLedger ORDER BY Datestamp DESC LIMIT 20;", True)









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
