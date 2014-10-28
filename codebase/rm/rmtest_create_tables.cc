#include "test_util.h"

int main()
{
    // Remove files that might be created by previous test run
	remove("tbl_employee");
	remove("tbl_employee2");
	remove("tbl_employee3");
	remove("tbl_employee4");

    // Basic Functions
    cout << endl << "Create Tables ..." << endl;

    // Create Table tbl_employee
    createTable("tbl_employee");
    rm->printTable(CATALOG_TABLE_NAME);
    rm->printTable(CATALOG_COLUMN_NAME);

    // Create Table tbl_employee2
    createTable("tbl_employee2");
    rm->printTable(CATALOG_TABLE_NAME);
    rm->printTable(CATALOG_COLUMN_NAME);

    // Create Table tbl_employee3
    createTable("tbl_employee3");
    rm->printTable(CATALOG_TABLE_NAME);
    rm->printTable(CATALOG_COLUMN_NAME);
    
    // Create Table tbl_employee4
    createLargeTable("tbl_employee4");
    rm->printTable(CATALOG_TABLE_NAME);
    rm->printTable(CATALOG_COLUMN_NAME);

    return 0;
}

