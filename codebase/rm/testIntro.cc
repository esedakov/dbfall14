#include "test_util.h"

void TEST_INTRO()
{
	RC rc;
	//Tables
	RM_ScanIterator rmsi;
	string tableAttrs[] = {"table-id", "table-name", "file-name"};
	vector<string> attributes;
	attributes.push_back(tableAttrs[0]);
	attributes.push_back(tableAttrs[1]);
	attributes.push_back(tableAttrs[2]);
	std::vector<Attribute> table;
	table.push_back(
			(Attribute) {tableAttrs[0], AttrType(0), sizeof(unsigned int)});
	table.push_back(
			(Attribute) {tableAttrs[1], AttrType(2), MAX_SIZE_OF_NAME_IN_DB});
	table.push_back(
			(Attribute) {tableAttrs[2], AttrType(2), MAX_SIZE_OF_NAME_IN_DB});
	rc = rm->scan(CATALOG_TABLE_NAME, "", NO_OP, NULL, attributes, rmsi);
	assert(rc == success);
	RID rid;
	void* returnedData = malloc(PAGE_SIZE);
	memset(returnedData, 0, PAGE_SIZE);
	while(rmsi.getNextTuple(rid, returnedData) != RM_EOF)
	{
		cout << "tuple from Tables: " << endl;
		RecordBasedFileManager::instance()->printRecord(table, returnedData);
		memset(returnedData, 0, PAGE_SIZE);
	}
	rmsi.close();
	//Columns
	std::vector<Attribute> column;
	string columnAttrs[] = { "table-id", "column-name", "column-type",
			"column-length" };
	attributes.clear();
	attributes.push_back(columnAttrs[0]);
	attributes.push_back(columnAttrs[1]);
	attributes.push_back(columnAttrs[2]);
	attributes.push_back(columnAttrs[3]);
	column.push_back(
			(Attribute) {columnAttrs[0], AttrType(0), sizeof(unsigned int)});
	column.push_back(
			(Attribute) {columnAttrs[1], AttrType(2), MAX_SIZE_OF_NAME_IN_DB});
	column.push_back(
			(Attribute) {columnAttrs[2], AttrType(0), sizeof(unsigned int)});
	column.push_back(
			(Attribute) {columnAttrs[3], AttrType(0), sizeof(unsigned int)});
	rc = rm->scan(CATALOG_COLUMN_NAME, "", NO_OP, NULL, attributes, rmsi);
	assert(rc == success);
	memset(returnedData, 0, PAGE_SIZE);
	while(rmsi.getNextTuple(rid, returnedData) != RM_EOF)
	{
		cout << "tuple from Columns: " << endl;
		RecordBasedFileManager::instance()->printRecord(column, returnedData);
		memset(returnedData, 0, PAGE_SIZE);
	}
	rmsi.close();
}

int main()
{
    cout << endl << "Test Simple Scan .." << endl;

     // Simple Scan
    TEST_INTRO();

    return 0;
}
