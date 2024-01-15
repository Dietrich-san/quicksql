from enum import Enum
import re
from quicksql import convert_to_sql

GLOBAL_test_id = 0
GLOBAL_number_of_tests_run = 0
GLOBAL_number_of_tests_failed = 0
GLOBAL_number_of_tests_succeeded = 0
GLOBAL_failed_tests = []

def test_quicksql_convert(quicksql, sql, throw_on_fail):
    global GLOBAL_test_id
    GLOBAL_test_id = GLOBAL_test_id + 1

    global GLOBAL_number_of_tests_run
    global GLOBAL_number_of_tests_failed
    global GLOBAL_number_of_tests_succeeded

    print("\n\nRunning test #" + str(GLOBAL_test_id) + "...")

    print("Attempting to convert quicksql: " + quicksql)

    GLOBAL_number_of_tests_run = GLOBAL_number_of_tests_run + 1

    try:
        test_success = test(convert_to_sql(True, quicksql), sql, throw_on_fail)
    except BaseException as error:
        print("Failed to translate to quicksql, error: {}".format(error))
        test_success = False

    if test_success:
        GLOBAL_number_of_tests_succeeded = GLOBAL_number_of_tests_succeeded + 1
    else:
        GLOBAL_number_of_tests_failed = GLOBAL_number_of_tests_failed + 1
        GLOBAL_failed_tests.append(GLOBAL_test_id)

def test(actual, expected, throw):
    if (actual == None):
        raise Exception("FAIL, actual cannot be null")
        return

    if (expected == None):
        raise Exception("FAIL, expected cannot be null")
        return

    if (actual == expected):
        print("Test successful! \nActual was: " + actual + "\n")
        return True

    if throw:
        raise Exception("\n\nFAIL, expected:\n \"" + expected + "\" \nbut was:\n \"" + actual + "\"")
    else:
        print("FAIL, expected \"" + expected + "\" but was \"" + actual + "\"")

    return False

if __name__ == "__main__":

    print ("Test hoffsql to sql:\n\n")

    # These pass:
    test_quicksql_convert("! Orders", "SELECT * FROM Orders;", False)
    test_quicksql_convert("! Orders", "SELECT * FROM Orders;", False)
    test_quicksql_convert("! Orders L", "SELECT * FROM Orders LIMIT 1;", False)
    test_quicksql_convert("! Orders L5", "SELECT * FROM Orders LIMIT 5;", False)
    test_quicksql_convert("! Orders L1", "SELECT * FROM Orders LIMIT 1;", False)
    test_quicksql_convert("! Orders 12345667", "SELECT * FROM Orders WHERE OrderID = 12345667;", False)
    test_quicksql_convert("! Orders 12345667 L", "SELECT * FROM Orders WHERE OrderID = 12345667 LIMIT 1;", False)
    test_quicksql_convert("! Orders 12345667 L4", "SELECT * FROM Orders WHERE OrderID = 12345667 LIMIT 4;", False)
    test_quicksql_convert("!ChainID Orders L2", "SELECT ChainID FROM Orders LIMIT 2;", False)
    test_quicksql_convert("!ChainID,OrderID Orders L2", "SELECT ChainID,OrderID FROM Orders LIMIT 2;", False)
    test_quicksql_convert("!ChainID Orders 18942810 L", "SELECT ChainID FROM Orders WHERE OrderID = 18942810 LIMIT 1;", False)
    test_quicksql_convert("!ChainID Orders 18942810", "SELECT ChainID FROM Orders WHERE OrderID = 18942810;", False)
    test_quicksql_convert("!ChainID Orders 18942810 L2", "SELECT ChainID FROM Orders WHERE OrderID = 18942810 LIMIT 2;", False)
    test_quicksql_convert("!ChainID,OrderID Orders 1935091 L2", "SELECT ChainID,OrderID FROM Orders WHERE OrderID = 1935091 LIMIT 2;", False)
    test_quicksql_convert("!ChainID,OrderID Orders orderstatusid=100 L2", "SELECT ChainID,OrderID FROM Orders WHERE orderstatusid=100 LIMIT 2;", False)
    test_quicksql_convert("!chainID,OrderID Orders orderstatusid=100 L2", "SELECT chainID,OrderID FROM Orders WHERE orderstatusid=100 LIMIT 2;", False)
    test_quicksql_convert("! Orders orderstatusid=100 L2", "SELECT * FROM Orders WHERE orderstatusid=100 LIMIT 2;", False)
    test_quicksql_convert("! Orders orderstatusid=100 L", "SELECT * FROM Orders WHERE orderstatusid=100 LIMIT 1;", False)
    test_quicksql_convert("! BankLedger 193401513", "SELECT * FROM BankLedger WHERE BankLedgerID = 193401513;", False)
    test_quicksql_convert("!EventID,Processed,ProcessedAs BankLedger 193401513", "SELECT EventID,Processed,ProcessedAs FROM BankLedger WHERE BankLedgerID = 193401513;", False)
    test_quicksql_convert("!EventID,Processed BankLedger 193401513 L", "SELECT EventID,Processed FROM BankLedger WHERE BankLedgerID = 193401513 LIMIT 1;", False)
    test_quicksql_convert("!EventID,Processed,ProcessedAs BankLedger 193401513 L13", "SELECT EventID,Processed,ProcessedAs FROM BankLedger WHERE BankLedgerID = 193401513 LIMIT 13;", False)
    test_quicksql_convert("!EventID,Processed,ProcessedAs BankLedger L2", "SELECT EventID,Processed,ProcessedAs FROM BankLedger LIMIT 2;", False)
    test_quicksql_convert("! BankLedger L2", "SELECT * FROM BankLedger LIMIT 2;", False)
    test_quicksql_convert("! Events O L", "SELECT * FROM Events ORDER BY 1 DESC LIMIT 1;", False)
    test_quicksql_convert("! Events OA L", "SELECT * FROM Events ORDER BY 1 ASC LIMIT 1;", False)
    test_quicksql_convert("! Events OD L", "SELECT * FROM Events ORDER BY 1 DESC LIMIT 1;", False)
    test_quicksql_convert("! Events O L3", "SELECT * FROM Events ORDER BY 1 DESC LIMIT 3;", False)
    test_quicksql_convert("! Events OA L4", "SELECT * FROM Events ORDER BY 1 ASC LIMIT 4;", False)
    test_quicksql_convert("! Events OD L5", "SELECT * FROM Events ORDER BY 1 DESC LIMIT 5;", False)
    test_quicksql_convert("! Events OA_EventID L", "SELECT * FROM Events ORDER BY EventID ASC LIMIT 1;", False)
    test_quicksql_convert("! Events OD_EventID L", "SELECT * FROM Events ORDER BY EventID DESC LIMIT 1;", False)
    test_quicksql_convert("! Events OD_EventID L4", "SELECT * FROM Events ORDER BY EventID DESC LIMIT 4;", False)
    test_quicksql_convert("!eventid,somecolumn Events OD_EventID L2", "SELECT eventid,somecolumn FROM Events ORDER BY EventID DESC LIMIT 2;", False)
    test_quicksql_convert("!eventid,somecolumn Events OA_EventID L1", "SELECT eventid,somecolumn FROM Events ORDER BY EventID ASC LIMIT 1;", False)
    test_quicksql_convert("!eventid,somecolumn Events OA_EventID L3", "SELECT eventid,somecolumn FROM Events ORDER BY EventID ASC LIMIT 3;", False)
    test_quicksql_convert("! Orders OD_Datestamp L1", "SELECT * FROM Orders ORDER BY Datestamp DESC LIMIT 1;", False)
    test_quicksql_convert("! Orders OrderStatusID=100 OD_Datestamp L1", "SELECT * FROM Orders WHERE OrderStatusID=100 ORDER BY Datestamp DESC LIMIT 1;", False)
    test_quicksql_convert("!OrderID,chainid Orders OrderStatusID=100 OD_Datestamp L1", "SELECT OrderID,chainid FROM Orders WHERE OrderStatusID=100 ORDER BY Datestamp DESC LIMIT 1;", False)
    test_quicksql_convert("! Events OD L", "SELECT * FROM Events ORDER BY 1 DESC LIMIT 1;", False)
    test_quicksql_convert("! Events O L30", "SELECT * FROM Events ORDER BY 1 DESC LIMIT 30;", False)
    test_quicksql_convert("! Events OD L30", "SELECT * FROM Events ORDER BY 1 DESC LIMIT 30;", False)
    test_quicksql_convert("! BankLedger OA_Datestamp L", "SELECT * FROM BankLedger ORDER BY Datestamp ASC LIMIT 1;", False)
    test_quicksql_convert("! BankLedger OD_Datestamp L", "SELECT * FROM BankLedger ORDER BY Datestamp DESC LIMIT 1;", False)
    test_quicksql_convert("! BankLedger OA_Datestamp L20", "SELECT * FROM BankLedger ORDER BY Datestamp ASC LIMIT 20;", False)
    test_quicksql_convert("! BankLedger OD_Datestamp L20", "SELECT * FROM BankLedger ORDER BY Datestamp DESC LIMIT 20;", False)
    test_quicksql_convert("!currency,bankwithdrawalid,bankwithdrawaltype,username bankwithdrawals,bankwithdrawaltypes,users bankwithdrawaltype='EXPRESS' OD_Datestamp L5", "SELECT currency,bankwithdrawalid,bankwithdrawaltype,username FROM bankwithdrawals JOIN bankwithdrawaltypes USING(bankwithdrawaltypeID) JOIN users USING(userID) WHERE bankwithdrawaltype='EXPRESS' ORDER BY Datestamp DESC LIMIT 5;", False)
    test_quicksql_convert("!name,round(balance,2),currency AccountBalances,Accounts", "SELECT name,round(balance,2),currency FROM AccountBalances JOIN Accounts USING(AccountID);", False)
    test_quicksql_convert("!round(balance,2),currency AccountBalances,Accounts name='CLIENT_BALANCES'", "SELECT round(balance,2),currency FROM AccountBalances JOIN Accounts USING(AccountID) WHERE name='CLIENT_BALANCES';", False)
    test_quicksql_convert("! accountbalances,accounts accounts.name='CLIENT_BALANCES'", "SELECT * FROM accountbalances JOIN accounts USING(accountID) WHERE accounts.name='CLIENT_BALANCES';", False)
    test_quicksql_convert("! accountbalances,accounts name='CLIENT_BALANCES'", "SELECT * FROM accountbalances JOIN accounts USING(accountID) WHERE name='CLIENT_BALANCES';", False)
    test_quicksql_convert("!currency,accountid,accounttypes.name accountbalances,accounts,accounttypes accounts.name='CLIENT_BALANCES'", "SELECT currency,accountid,accounttypes.name FROM accountbalances JOIN accounts USING(accountID) JOIN accounttypes USING(accounttypeID) WHERE accounts.name='CLIENT_BALANCES';", False)
    test_quicksql_convert("!accountbalances.* accountbalances,accounts,accounttypes accounts.name='CLIENT_BALANCES'", "SELECT accountbalances.* FROM accountbalances JOIN accounts USING(accountID) JOIN accounttypes USING(accounttypeID) WHERE accounts.name='CLIENT_BALANCES';", False)
    test_quicksql_convert("! Users,UserCategories", "SELECT * FROM Users JOIN UserCategories USING(UserCategoryID);", False)
    test_quicksql_convert("!userid,usercategories.name Users,UserCategories", "SELECT userid,usercategories.name FROM Users JOIN UserCategories USING(UserCategoryID);", False)
    test_quicksql_convert("! Users,UserCategories 123", "SELECT * FROM Users JOIN UserCategories USING(UserCategoryID) WHERE UserID = 123;", False)
    test_quicksql_convert("! Users,UserCategories name='Gaming'", "SELECT * FROM Users JOIN UserCategories USING(UserCategoryID) WHERE name='Gaming';", False)
    test_quicksql_convert("! Users,UserCategories UserCategoryID=1234", "SELECT * FROM Users JOIN UserCategories USING(UserCategoryID) WHERE UserCategoryID=1234;", False)
    test_quicksql_convert("! UserCategories", "SELECT * FROM UserCategories;", False)
    test_quicksql_convert("!usercategories.name UserCategories", "SELECT usercategories.name FROM UserCategories;", False)
    test_quicksql_convert("!name UserCategories", "SELECT name FROM UserCategories;", False)
    test_quicksql_convert("! UserCategories 123491509", "SELECT * FROM UserCategories WHERE UserCategoryID = 123491509;", False)
    test_quicksql_convert("! UserCategories,Users 123491509", "SELECT * FROM UserCategories JOIN Users USING(UserID) WHERE UserCategoryID = 123491509;", False)
    test_quicksql_convert("! UserCategories,Users UserCategories.Name='Gaming'", "SELECT * FROM UserCategories JOIN Users USING(UserID) WHERE UserCategories.Name='Gaming';", False)
    test_quicksql_convert("! UserCategories,Users Name='Gaming'", "SELECT * FROM UserCategories JOIN Users USING(UserID) WHERE Name='Gaming';", False)
    test_quicksql_convert("! UserCategories,Users 51", "SELECT * FROM UserCategories JOIN Users USING(UserID) WHERE UserCategoryID = 51;", False)
    test_quicksql_convert("! BankLedger 1234,5678,35858", "SELECT * FROM BankLedger WHERE BankLedgerID IN (1234,5678,35858);", False)
    test_quicksql_convert("! BankLedger,BankAccounts 1234,5678,35858", "SELECT * FROM BankLedger JOIN BankAccounts USING(BankAccountID) WHERE BankLedgerID IN (1234,5678,35858);", False)
    test_quicksql_convert("!bankledgerid,ecosysaccount,amount,currency BankLedger,BankAccounts 1234,5678,35858", "SELECT bankledgerid,ecosysaccount,amount,currency FROM BankLedger JOIN BankAccounts USING(BankAccountID) WHERE BankLedgerID IN (1234,5678,35858);", False)
    test_quicksql_convert("! BankLedger,BankAccounts BankAccounts.EcosysAccount='CLIENT_FUNDS_SWEDEN_SWED'", "SELECT * FROM BankLedger JOIN BankAccounts USING(BankAccountID) WHERE BankAccounts.EcosysAccount='CLIENT_FUNDS_SWEDEN_SWED';", False)
    test_quicksql_convert("! BankLedger,BankAccounts BankAccounts.EcosysAccount='CLIENT_FUNDS_SWEDEN_SWED' O", "SELECT * FROM BankLedger JOIN BankAccounts USING(BankAccountID) WHERE BankAccounts.EcosysAccount='CLIENT_FUNDS_SWEDEN_SWED' ORDER BY 1 DESC;", False)
    test_quicksql_convert("! BankLedger,BankAccounts BankAccounts.EcosysAccount='CLIENT_FUNDS_SWEDEN_SWED' OD_Datestamp", "SELECT * FROM BankLedger JOIN BankAccounts USING(BankAccountID) WHERE BankAccounts.EcosysAccount='CLIENT_FUNDS_SWEDEN_SWED' ORDER BY Datestamp DESC;", False)
    test_quicksql_convert("! BankLedger,BankAccounts BankAccounts.EcosysAccount='CLIENT_FUNDS_SWEDEN_SWED' OA_Datestamp", "SELECT * FROM BankLedger JOIN BankAccounts USING(BankAccountID) WHERE BankAccounts.EcosysAccount='CLIENT_FUNDS_SWEDEN_SWED' ORDER BY Datestamp ASC;", False)
    test_quicksql_convert("! BankLedger,BankAccounts BankAccounts.EcosysAccount='CLIENT_FUNDS_SWEDEN_SWED' OD_Datestamp L", "SELECT * FROM BankLedger JOIN BankAccounts USING(BankAccountID) WHERE BankAccounts.EcosysAccount='CLIENT_FUNDS_SWEDEN_SWED' ORDER BY Datestamp DESC LIMIT 1;", False)
    test_quicksql_convert("! BankLedger,BankAccounts BankAccounts.EcosysAccount='CLIENT_FUNDS_SWEDEN_SWED' OD_Datestamp L1", "SELECT * FROM BankLedger JOIN BankAccounts USING(BankAccountID) WHERE BankAccounts.EcosysAccount='CLIENT_FUNDS_SWEDEN_SWED' ORDER BY Datestamp DESC LIMIT 1;", False)
    test_quicksql_convert("! BankLedger,BankAccounts BankAccounts.EcosysAccount='CLIENT_FUNDS_SWEDEN_SWED' OD_Datestamp L4", "SELECT * FROM BankLedger JOIN BankAccounts USING(BankAccountID) WHERE BankAccounts.EcosysAccount='CLIENT_FUNDS_SWEDEN_SWED' ORDER BY Datestamp DESC LIMIT 4;", False)
    #
    #
    #
    #
    test_quicksql_convert("! Users,UserCategories 51,34 (Enabled IS TRUE AND (Yolo = 5 OR Polo <> 3))", "SELECT * FROM Users JOIN UserCategories USING(UserCategoryID) WHERE UserID IN (51,34) AND (Enabled IS TRUE AND (Yolo = 5 OR Polo <> 3));", True)

    test_quicksql_convert("! Users,UserCategories 51,34 (Enabled IS TRUE)", "SELECT * FROM Users JOIN UserCategories USING(UserCategoryID) WHERE UserID IN (51,34) AND (Enabled IS TRUE);", True)

    #
    test_quicksql_convert("! Users,UserCategories 51 (Enabled IS TRUE)", "SELECT * FROM Users JOIN UserCategories USING(UserCategoryID) WHERE UserID = 51 AND (Enabled IS TRUE);", True)


    test_quicksql_convert("! Users,UserCategories 51,34 (Enabled IS TRUE)", "SELECT * FROM Users JOIN UserCategories USING(UserCategoryID) WHERE UserID IN (51,34) AND (Enabled IS TRUE);", True)
    # test_quicksql_convert("! UserCategories,Users (UserCategoryID IN (51, 34) AND Username ilike '%fred%' OR Username = 'apitest')", "SELECT * FROM UserCategories JOIN Users USING(UserID) WHERE UserCategoryID IN (51, 34) AND Username ilike '%fred%' OR Username = 'apitest';", True)
    #
    # test_quicksql_convert("! orders 12357", "SELECT * FROM orders WHERE order_id;", True)
    # test_quicksql_convert("! bank_withdrawals 12357", "SELECT * FROM orders WHERE bank_withdrawal_id;", True)



    test_quicksql_convert("! UserCategories,Users 51 (Username ilike '%fred%')", "SELECT * FROM UserCategories JOIN Users USING(UserID) WHERE UserCategoryID = 51 AND (Username ilike '%fred%');", True)


    test_quicksql_convert("! UserCategories,Users 51,34 (Username ilike '%fred%')", "SELECT * FROM UserCategories JOIN Users USING(UserID) WHERE UserCategoryID IN (51,34) AND (Username ilike '%fred%');", True)








    # ideas
    #test_quicksql_convert("!orderid,workertype,datestamp,orderstatusid Orders,WorkerTypes(WorkerTypeID, OrderTypeID) OD_Datestamp L10", "SELECT orderid,workertype,datestamp,orderstatusid FROM Orders JOIN WorkerTypes ON WorkerTypes.WorkerTypeID = Orders.OrderTypeID ORDER BY Datestamp DESC LIMIT 10", True)







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



    # Final presentation:

    print("Tests passed: " + str(GLOBAL_number_of_tests_succeeded))
    print("Tests failed: " + str(GLOBAL_number_of_tests_failed))
    print("Tests run: " + str(GLOBAL_number_of_tests_run))
    print("Failed tests(#): " + str(GLOBAL_failed_tests))
    if GLOBAL_number_of_tests_succeeded == GLOBAL_number_of_tests_run:
        print("ALL TESTS SUCCESSFUL!")
