#include "rm.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <iostream>

//error codes for project 2, component RM:
//	-30 = attempt to re-create existing table
//	-31 = accessing/modifying/deleting table that does not exists
//	-32 = Table of tables corrupted
//	-33 = invalid write access to system table
//	-34 = destructive operation on the catalog
//	-35 = specified column does not exist
//	-36 = attempting to drop non-existing OR add existing field inside the record
//	-37 = wrong table arguments

//	-38 = index already exists

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance() {
	if (!_rm)
		_rm = new RelationManager();

	return _rm;
}

void RelationManager::cleanup() {
	remove(CATALOG_TABLE_NAME);
	remove(CATALOG_COLUMN_NAME);
}

RC RelationManager::createRecordInTables(FileHandle& tableHandle,
		const std::vector<Attribute>& table, const char* tableName, int tableId,
		RID& rid)
		{
	RC errCode = 0;

	//determine lengths of VarChar fields
	unsigned int lenOfTableNameField = strlen(tableName);

	//create buffer for storing record
	void* recordData = malloc(sizeof(unsigned int) + //table_id
			(sizeof(unsigned int) + lenOfTableNameField) + //size + table_name
			(sizeof(unsigned int) + lenOfTableNameField)); //size + file_name

	//copy fields into record's buffer
	char* p = (char*) recordData;
	//table id
	((unsigned int*) p)[0] = tableId;
	p += sizeof(unsigned int);
	//table name's length
	((unsigned int*) p)[0] = lenOfTableNameField;
	p += sizeof(unsigned int);
	//table name
	memcpy(p, tableName, lenOfTableNameField);
	p += lenOfTableNameField;
	//file name's length
	((unsigned int*) p)[0] = lenOfTableNameField;
	p += sizeof(unsigned int);
	//file name
	memcpy(p, tableName, lenOfTableNameField);

	//insert record
	if ((errCode = _rbfm->insertRecord(tableHandle, table, recordData, rid))
			!= 0) {
		//deallocate record's buffer
		free(recordData);

		//return error code
		return errCode;
	}

	//deallocate record's buffer
	free(recordData);

	//return success
	return errCode;
}

RC RelationManager::createRecordInColumns(FileHandle& columnHandle,
		const std::vector<Attribute>& desc, unsigned int tableId,
		const char * columnName, AttrType columnType, unsigned int columnLength,
		bool isDropped, RID& rid) //NOT TESTED
		{
	RC errCode = 0;

	//determine lengths of VarChar fields
	unsigned int lenOfColumnNameField = strlen(columnName);

	//create buffer for storing record
	void* recordData = malloc(sizeof(unsigned int) + //table_id
			(sizeof(unsigned int) + lenOfColumnNameField) + //size + column_name
			sizeof(unsigned int) +	//column type
			sizeof(unsigned int) +	//column length
			sizeof(unsigned int));	//column status

	//copy fields into record's buffer
	char* p = (char*) recordData;
	//table id
	((unsigned int*) p)[0] = tableId;
	p += sizeof(unsigned int);
	//column name's length
	((unsigned int*) p)[0] = lenOfColumnNameField;
	p += sizeof(unsigned int);
	//column name
	memcpy(p, columnName, lenOfColumnNameField);
	p += lenOfColumnNameField;
	//column type
	((unsigned int*) p)[0] = columnType;
	p += sizeof(unsigned int);
	//column length
	((unsigned int*) p)[0] = columnLength;
	p += sizeof(unsigned int);
	//column status: is this field has been dropped (integer: 1=true, 0=false)
	((unsigned int*) p)[0] = isDropped ? 1 : 0;

	//insert record
	if ((errCode = _rbfm->insertRecord(columnHandle, desc, recordData, rid))
			!= 0) {
		//deallocate record's buffer
		free(recordData);

		//return error code
		return errCode;
	}

	//deallocate record's buffer
	free(recordData);

	//return success
	return errCode;
}

RC RelationManager::createRecordInIndexes(FileHandle& columnHandle,
		const std::vector<Attribute>& desc, unsigned int tableId,
		const char* colName, const char* fileName, RID& rid) //NOT TESTED
		{
	RC errCode = 0;

	//determine lengths of VarChar fields
	unsigned int lenOfColNameField = strlen(colName);
	unsigned int lenOfFileNameField = strlen(fileName);

	//create buffer for storing record
	void* recordData = malloc(
			sizeof(unsigned int) + //table_id
			sizeof(unsigned int) + lenOfColNameField + //column_name
			sizeof(unsigned int) + lenOfFileNameField);	//file_index_name

	//copy fields into record's buffer
	char* p = (char*) recordData;
	//table id
	((unsigned int*) p)[0] = tableId;
	p += sizeof(unsigned int);
	//length of column name
	((unsigned int*) p)[0] = lenOfColNameField;
	p += sizeof(unsigned int);
	//copy column name
	memcpy(p, colName, lenOfColNameField);
	p += lenOfColNameField;
	//length of file name
	((unsigned int*) p)[0] = lenOfFileNameField;
	p += sizeof(unsigned int);
	//copy file name
	memcpy(p, fileName, lenOfFileNameField);
	p += lenOfFileNameField;

	//insert record
	//if ((errCode = _rbfm->insertRecord(columnHandle, desc, recordData, rid)) != 0)
	if( (errCode = insertTuple(columnHandle._info->_name, recordData, rid)) != 0 )
	{
		//deallocate record's buffer
		free(recordData);

		//return error code
		return errCode;
	}

	//deallocate record's buffer
	free(recordData);

	//return success
	return errCode;
}

RC RelationManager::printIndexFile(const string& fileName, const Attribute& attrOfKey)
{
	IndexManager* indexManager = IndexManager::instance();

	RC rc = 0;

	IXFileHandle ixfileHandle;
	if( (rc = indexManager->openFile(fileName, ixfileHandle)) != 0 )
	{
		return rc;
	}

	unsigned int numberOfPagesFromFunction = 0;
	// Get number of primary pages
	rc = indexManager->getNumberOfPrimaryPages(ixfileHandle, numberOfPagesFromFunction);
	if(rc != 0)
	{
		cout << "getNumberOfPrimaryPages() failed." << endl;
		//indexManager->closeFile(ixfileHandle);
		return -1;
	}

	// Print Entries in each page
	for (unsigned i = 0; i < numberOfPagesFromFunction; i++) {
		rc = indexManager->printIndexEntriesInAPage(ixfileHandle, attrOfKey, i);
		if (rc != 0) {
			cout << "printIndexEntriesInAPage() failed." << endl;
			//indexManager->closeFile(ixfileHandle);
			return -1;
		}
	}

	//success
	return rc;
}

void RelationManager::createCatalog()
{
	//from project description:
	//	It is mandatory to store the catalog information by using the RBF layer functions.
	//	In other words, you should manually create the catalog's tables and populate them
	//	the first time your database is initialized. Once the catalog's tables (such as the
	//	Tables table with the following attributes (table-id, table-name, file-name) and
	//	the Columns table with the following attributes (table-id, column-name,
	//	column-type, column-length)) have been created, they should be persisted to disk.

	//create new files for table and column
	if (_rbfm->createFile(CATALOG_TABLE_NAME) != 0
			|| _rbfm->createFile(CATALOG_COLUMN_NAME) != 0
			|| _rbfm->createFile(CATALOG_INDEX_NAME)) {
		//abort
		exit(-1);
	}

	//open both files
	FileHandle tableHandle, columnHandle, indexHandle;
	if (_rbfm->openFile(CATALOG_TABLE_NAME, tableHandle) != 0
			|| _rbfm->openFile(CATALOG_COLUMN_NAME, columnHandle) != 0
			|| _rbfm->openFile(CATALOG_INDEX_NAME, indexHandle)) {
		//abort
		exit(-1);
	}

	//set both files to be modifiable solely by the DB (and not by users)
	bool success = true;
	tableHandle.setAccess(only_system_can_modify, success);
	columnHandle.setAccess(only_system_can_modify, success);
	indexHandle.setAccess(only_system_can_modify, success);
	if (success == false) {
		//abort
		exit(-1);
	}

	//1. insert record about table Tables into Tables => {TABLE_ID=1, TABLE_NAME="Tables", FILE_NAME="Tables"}
	//Tables table with the following attributes (table-id, table-name, file-name)
	std::vector<Attribute> table;
	const char* tableDescFields[] = { "table_id", "table_name", "file_name" };

	table.push_back(
			(Attribute) {tableDescFields[0], AttrType(0), sizeof(unsigned int)});
	table.push_back(
			(Attribute) {tableDescFields[1], AttrType(2), MAX_SIZE_OF_NAME_IN_DB});
	table.push_back(
			(Attribute) {tableDescFields[2], AttrType(2), MAX_SIZE_OF_NAME_IN_DB});

	RID ridTable, ridColumn, ridIndex;

	//compose and insert record describing the TABLE
	if (createRecordInTables(tableHandle, table, CATALOG_TABLE_NAME,
			CATALOG_TABLE_ID, ridTable) != 0) {
		//abort
		cleanup();
		exit(-1);
	}

	//2. insert record about table Columns into Tables => {TABLE_ID=2, TABLE_NAME="Columns", FILE_NAME="Columns"}
	//compose and insert record describing the COLUMN
	if (createRecordInTables(tableHandle, table, CATALOG_COLUMN_NAME,
			CATALOG_COLUMN_ID, ridColumn) != 0) {
		//abort
		cleanup();
		exit(-1);
	}

	//insert record about Indexes table into Tables table
	if( createRecordInTables(tableHandle, table, CATALOG_INDEX_NAME,
			CATALOG_INDEX_ID, ridIndex) != 0 )
	{
		//abort
		cleanup();
		exit(-1);
	}

	//3. Insert information about Tables and Columns into Table's map => {TABLE_ID, TABLE_NAME, RID}
	//for fast lookup -> insert <table name, table info> INTO _catalogTable
	_catalogTable.insert(
			std::pair<string, TableInfo>(CATALOG_TABLE_NAME,
					(TableInfo) {CATALOG_TABLE_ID, CATALOG_TABLE_NAME, ridTable}));
	_catalogTable.insert(
			std::pair<string, TableInfo>(CATALOG_COLUMN_NAME,
					(TableInfo) {CATALOG_COLUMN_ID, CATALOG_COLUMN_NAME, ridColumn}));
	_catalogTable.insert(
			std::pair<string, TableInfo>(CATALOG_INDEX_NAME,
					(TableInfo) {CATALOG_INDEX_ID, CATALOG_INDEX_NAME, ridIndex}));


	//4. Insert record about table Tables into Columns =>
	//		{TABLE_ID=CATALOG_TABLE_ID, COLUMN_NAME=tableDescFields[#], COLUMN_TYPE=table[#].type, COLUMN_LENGTH=table[#].length}
	//compose and insert COLUMN records for TABLES table
	//Columns table with the following attributes (table-id, column-name, column-type, column-length)
	std::vector<Attribute> column;
	const char* columnDescFields[] = { "table_id", "column_name", "column_type",
			"column_length", "column_status" };
	column.push_back(
			(Attribute) {columnDescFields[0], AttrType(0), sizeof(unsigned int)});
	column.push_back(
			(Attribute) {columnDescFields[1], AttrType(2), MAX_SIZE_OF_NAME_IN_DB});
	column.push_back(
			(Attribute) {columnDescFields[2], AttrType(0), sizeof(unsigned int)});
	column.push_back(
			(Attribute) {columnDescFields[3], AttrType(0), sizeof(unsigned int)});
	column.push_back(
			(Attribute) {columnDescFields[4], AttrType(0), sizeof(unsigned int)});
	RID list_of_tableColumn_rids[3];

	if (createRecordInColumns(columnHandle, column, CATALOG_TABLE_ID,
			table[0].name.c_str(), table[0].type, table[0].length, false,
			list_of_tableColumn_rids[0]) != 0
			|| createRecordInColumns(columnHandle, column, CATALOG_TABLE_ID,
					table[1].name.c_str(), table[1].type, table[1].length, false,
					list_of_tableColumn_rids[1]) != 0
			|| createRecordInColumns(columnHandle, column, CATALOG_TABLE_ID,
					table[2].name.c_str(), table[2].type, table[2].length, false,
					list_of_tableColumn_rids[2]) != 0) {
		//abort
		cleanup();
		exit(-1);
	}

	//5. insert information about Table into Column's map => {COLUMN_NAME, TYPE_OF_FIELD, SIZE_OF_FIELD}
	//compose list of column information
	std::vector<ColumnInfo> tableInfo;
	tableInfo.push_back(
			(ColumnInfo) {tableDescFields[0], TypeInt, sizeof(unsigned int), 0, list_of_tableColumn_rids[0]});
	tableInfo.push_back(
			(ColumnInfo) {tableDescFields[1], TypeVarChar, MAX_SIZE_OF_NAME_IN_DB, 0, list_of_tableColumn_rids[1]});
	tableInfo.push_back(
			(ColumnInfo) {tableDescFields[2], TypeVarChar, MAX_SIZE_OF_NAME_IN_DB, 0, list_of_tableColumn_rids[2]});

	//for fast lookup -> insert <table id, list of column info> for TABLE INTO _catalogColumn
	_catalogColumn.insert(
			std::pair<int, std::vector<ColumnInfo> >(CATALOG_TABLE_ID,
					tableInfo));

	//6. insert record about table Columns into Columns =>
	//		{TABLE_ID=CATALOG_COLUMN_ID, COLUMN_NAME=columnDescFields[#], COLUMN_TYPE=column[#].type, COLUMN_LENGTH=column[#].length}
	RID list_of_columnColumn_rids[5];

	//compose and insert COLUMN records for COLUMNS table
	if (createRecordInColumns(columnHandle, column, CATALOG_COLUMN_ID,
			column[0].name.c_str(), column[0].type, column[0].length, false,
			list_of_columnColumn_rids[0]) != 0
			|| createRecordInColumns(columnHandle, column, CATALOG_COLUMN_ID,
					column[1].name.c_str(), column[1].type, column[1].length,
					false, list_of_columnColumn_rids[1]) != 0
			|| createRecordInColumns(columnHandle, column, CATALOG_COLUMN_ID,
					column[2].name.c_str(), column[2].type, column[2].length,
					false, list_of_columnColumn_rids[2]) != 0
			|| createRecordInColumns(columnHandle, column, CATALOG_COLUMN_ID,
					column[3].name.c_str(), column[3].type, column[3].length,
					false, list_of_columnColumn_rids[3]) != 0
			|| createRecordInColumns(columnHandle, column, CATALOG_COLUMN_ID,
					column[4].name.c_str(), column[4].type, column[4].length,
					false, list_of_columnColumn_rids[4]) != 0) {
		//abort
		cleanup();
		exit(-1);
	}

	//7. insert information about Columns into Column's map
	//compose list of column information
	std::vector<ColumnInfo> columnInfo;
	columnInfo.push_back(
			(ColumnInfo) {columnDescFields[0], AttrType(0), sizeof(unsigned int), 0, list_of_columnColumn_rids[0]});
	columnInfo.push_back(
			(ColumnInfo) {columnDescFields[1], AttrType(2), MAX_SIZE_OF_NAME_IN_DB, 0, list_of_columnColumn_rids[1]});
	columnInfo.push_back(
			(ColumnInfo) {columnDescFields[2], AttrType(0), sizeof(unsigned int), 0, list_of_columnColumn_rids[2]});
	columnInfo.push_back(
			(ColumnInfo) {columnDescFields[3], AttrType(0), sizeof(unsigned int), 0, list_of_columnColumn_rids[3]});
	columnInfo.push_back(
			(ColumnInfo) {columnDescFields[4], AttrType(0), sizeof(unsigned int), 0, list_of_columnColumn_rids[4]});

	//for fast lookup -> insert <table id, list of column info> for TABLE INTO _catalogColumn
	_catalogColumn.insert(
			std::pair<int, std::vector<ColumnInfo> >(CATALOG_COLUMN_ID,
					columnInfo));

	//8. Indexes table with the following attributes (table-id, column-name, index-file-name)
	std::vector<Attribute> index;
	const char* indexDescFields[] = { "table_id", "column_name", "index_file_name" };
	index.push_back(
			(Attribute) {indexDescFields[0], AttrType(0), sizeof(unsigned int)});
	index.push_back(
			(Attribute) {indexDescFields[1], AttrType(2), MAX_SIZE_OF_NAME_IN_DB});
	index.push_back(
			(Attribute) {indexDescFields[2], AttrType(2), MAX_SIZE_OF_NAME_IN_DB});
	RID list_of_indexColumn_rids[3];

	// createRecordInColumns(columnHandle, column, CATALOG_TABLE_ID,
	//		table[0].name.c_str(), table[0].type, table[0].length, false,
	//		list_of_tableColumn_rids[0]

	//9. insert record about table Indexes into Columns =>
	//		{TABLE_ID=CATALOG_INDEX_ID, COLUMN_NAME=indexDescFields[#], COLUMN_TYPE=index[#].type, COLUMN_LENGTH=index[#].length}
	RC errCode =
		createRecordInColumns(columnHandle, column, CATALOG_INDEX_ID,
			index[0].name.c_str(), index[0].type, index[0].length, false,
			list_of_indexColumn_rids[0]);

	errCode =
		createRecordInColumns(columnHandle, column, CATALOG_INDEX_ID,
					index[1].name.c_str(), index[1].type, index[1].length, false,
					list_of_indexColumn_rids[1]);
	errCode =
		createRecordInColumns(columnHandle, column, CATALOG_INDEX_ID,
					index[2].name.c_str(), index[2].type, index[2].length, false,
					list_of_indexColumn_rids[2]);
	if( errCode != 0 )
	{
		//abort
		cleanup();
		exit(-1);
	}

	//10. insert index information into local column map
	std::vector<ColumnInfo> indexInfo;
	indexInfo.push_back(
			(ColumnInfo) {indexDescFields[0], TypeInt, sizeof(unsigned int), 0, list_of_indexColumn_rids[0]});
	indexInfo.push_back(
			(ColumnInfo) {indexDescFields[1], TypeVarChar, MAX_SIZE_OF_NAME_IN_DB, 0, list_of_indexColumn_rids[1]});
	indexInfo.push_back(
			(ColumnInfo) {indexDescFields[2], TypeVarChar, MAX_SIZE_OF_NAME_IN_DB, 0, list_of_indexColumn_rids[2]});

	//for fast lookup -> insert <table id, list of column info> for TABLE INTO _catalogColumn
	_catalogColumn.insert(
			std::pair<int, std::vector<ColumnInfo> >(CATALOG_INDEX_ID,
					indexInfo));

	//11. close all catalog files
	if (_rbfm->closeFile(tableHandle) != 0
			|| _rbfm->closeFile(columnHandle) != 0
			|| _rbfm->closeFile(indexHandle) != 0 ) {
		//abort
		cleanup();
		exit(-1);
	}
}

void RelationManager::insertElementsFromTableIntoMap(FileHandle tableHandle,
		const std::vector<Attribute>& tableDesc) {
	std::string tableName = tableHandle._info->_name;

	//quick way of determining whether the given system table a table of Columns or Tables
	//bool isColumnsTable = (tableName == CATALOG_COLUMN_NAME);
	//whichTable = {Tables => 1, Columns => 2, Indexes => 3, other => 0)
	int whichTable =
			( tableName == CATALOG_TABLE_NAME ? 1 : ( tableName == CATALOG_COLUMN_NAME ? 2 : (tableName == CATALOG_INDEX_NAME ? 3 : 0) ) );

	//make sure that the given table name refers to one of the system tables
	if (whichTable == 0) {
		//destroy system files
		cleanup();

		//fail
		exit(-1);
	}

	//setup iterator to loop thru table Tables to insert each found record into the local copy
	//RM_ScanIterator iterator;	//cannot use RM iterator, since it requires an information from catalog that has not yet been setup
	RBFM_ScanIterator iterator;

	//allocate buffer for storing iterated elements, i.e. table and column records
	void* data = malloc(PAGE_SIZE);
	memset(data, 0, PAGE_SIZE);

	//setup list of attributes for scanning
	std::vector<string> tableAttr;

	//loop through elements of attributes to create list of strings
	std::vector<Attribute>::const_iterator i = tableDesc.begin();

	//setup tableAttr
	for (; i != tableDesc.end(); i++) {
		tableAttr.push_back(i->name);
	}

	//set the iterator to the beginning of the given table
	if (_rbfm->scan(tableHandle, tableDesc, "", NO_OP, NULL, tableAttr,
			iterator) != 0) {
		//delete system files
		cleanup();

		//abort
		exit(-1);
	}

	//record identifier for table records
	RID rid;

	//loop through Table's records and insert them into one of the catalog local maps
	while (iterator.getNextRecord(rid, data) != RM_EOF) {
		//insert iterated element into the proper map
		switch(whichTable)
		{
		case 1:
			processTableRecordAndInsertIntoMap((char*) data, rid);
			break;
		case 2:
			processColumnRecordAndInsertIntoMap((char*) data, rid);
			break;
		case 3:
			processIndexRecordAndInsertIntoMap((char*) data, rid);
			break;
		}
	}

}

void RelationManager::processTableRecordAndInsertIntoMap(const void* buffer,
		const RID& rid) {
	//cast buffer to char-pointer
	char* data = (char*) buffer;

	//get integer that represents table-id
	unsigned int table_id = *((unsigned int*) data);

	//update data pointer
	data += sizeof(unsigned int);

	//get table-name
	//get integer that describes the size of the char-array
	unsigned int sz_of_name = *((unsigned int*) data);

	//update data pointer
	data += sizeof(unsigned int);

	//get string for table name

	//get character array
	char* carr_table_name = (char*) malloc(sz_of_name + 1);
	memset(carr_table_name, 0, sz_of_name + 1);
	memcpy(carr_table_name, data, sz_of_name);

	//convert to string
	string str_table_name = string(carr_table_name);

	//free space used by the table name
	free(carr_table_name);
	//update data pointer
	data += sz_of_name;

	//get file-name
	//get integer that describes the size of the char-array
	sz_of_name = *((unsigned int*) data);

	//update data pointer
	data += sizeof(unsigned int);

	//get string for table name

	//get character array
	char* carr_file_name = (char*) malloc(sz_of_name + 1);
	memset(carr_file_name, 0, sz_of_name + 1);
	memcpy(carr_file_name, data, sz_of_name);

	//convert to string
	string str_file_name = string(carr_file_name);

	//free space used by the table name
	free(carr_file_name);

	//insert record elements inside _catalogTable
	_catalogTable.insert(
			std::pair<string, TableInfo>(str_table_name,
					(TableInfo) {table_id, str_table_name, rid}));
}

void RelationManager::processIndexRecordAndInsertIntoMap(const void* buffer,
		const RID& rid) {

	//record format: <table-id:Integer, column-name:VarChar, index-file-name:VarChar>

	//cast buffer to char-array
	char* data = (char*) buffer;

	//get integer that represents table-id
	unsigned int table_id = *((unsigned int*) data);

	//update data pointer
	data += sizeof(unsigned int);

	//get file-index-name
	//get integer that describes the size of the char-array
	unsigned int sz_of_col_name = *((unsigned int*) data);

	//update data pointer
	data += sizeof(unsigned int);

	//get string for column name

	//get character array
	char* carr_col_name = (char*) malloc(sz_of_col_name + 1);
	memset(carr_col_name, 0, sz_of_col_name + 1);
	memcpy(carr_col_name, data, sz_of_col_name);
	data += sz_of_col_name;

	//convert to string
	string str_col_name = string(carr_col_name);

	//get file-index-name
	//get integer that describes the size of the char-array
	unsigned int sz_of_file_name = *((unsigned int*) data);

	//update data pointer
	data += sizeof(unsigned int);

	//get string for column name

	//get character array
	char* carr_file_name = (char*) malloc(sz_of_file_name + 1);
	memset(carr_file_name, 0, sz_of_file_name + 1);
	memcpy(carr_file_name, data, sz_of_file_name);

	//convert to string
	string str_file_name = string(carr_file_name);

	//free space used by the table name
	free(carr_file_name);

	//iterator that points at the key-value pair of interest
	std::map<int, std::map<std::string, IndexInfo> >::iterator iterator =
			_catalogIndex.find(table_id);

	//make sure that the pair with the given table-id exists inside the map
	if (iterator == _catalogIndex.end()) {
		_catalogIndex.insert(
				std::pair<int, std::map<std::string, IndexInfo> >(table_id, std::map<std::string, IndexInfo>() )
		);
		iterator = _catalogIndex.find(table_id);
	}

	IndexInfo info;
	info._indexName = str_file_name;
	info._rid.pageNum = rid.pageNum;
	info._rid.slotNum = rid.slotNum;

	//insert record elements inside _catalogTable
	(*iterator).second.insert( std::pair<std::string, IndexInfo>(str_col_name, info) );
}

void RelationManager::processColumnRecordAndInsertIntoMap(const void* buffer,
		const RID& rid) {
	//cast buffer to char-array
	char* data = (char*) buffer;

	//get integer that represents table-id
	unsigned int table_id = *((unsigned int*) data);

	//update data pointer
	data += sizeof(unsigned int);

	//get column-name
	//get integer that describes the size of the char-array
	unsigned int sz_of_name = *((unsigned int*) data);

	//update data pointer
	data += sizeof(unsigned int);

	//get string for column name

	//get character array
	char* carr_column_name = (char*) malloc(sz_of_name + 1);
	memset(carr_column_name, 0, sz_of_name + 1);
	memcpy(carr_column_name, data, sz_of_name);

	//convert to string
	string str_column_name = string(carr_column_name);

	//free space used by the table name
	free(carr_column_name);
	//update data pointer
	data += sz_of_name;

	//get integer that represents column-type
	unsigned int column_type = *((unsigned int*) data);

	//update data pointer
	data += sizeof(unsigned int);

	//get integer that represents column-length
	unsigned int column_length = *((unsigned int*) data);

	//update data pointer
	data += sizeof(unsigned int);

	//get integer that represents column-status
	unsigned int column_status = *((unsigned int*) data);

	//iterator that points at the key-value pair of interest
	std::map<int, std::vector<ColumnInfo> >::iterator iterator =
			_catalogColumn.find(table_id);

	//make sure that the pair with the given table-id exists inside the map
	if (iterator == _catalogColumn.end()) {
		_catalogColumn.insert(
				std::pair<int, std::vector<ColumnInfo> >(table_id,
						std::vector<ColumnInfo>()));
		iterator = _catalogColumn.find(table_id);
	}

	//insert record elements inside _catalogTable
	ColumnInfo info(str_column_name, (AttrType) column_type, column_length,
			column_status, rid);
	(*iterator).second.push_back(info);

}

RelationManager::RelationManager()
{
	_rbfm = RecordBasedFileManager::instance();

	//setup the other class data-member(s)
	_nextTableId = 4;

	//check if catalog from prior execution already exists.
	FileHandle catalogOfTables, catalogOfColumns, catalogOfIndexes;

	std::string catalogTableFileName = string(CATALOG_TABLE_NAME),
			catalogColumnFileName = string(CATALOG_COLUMN_NAME),
			catalogIndexFileName = string(CATALOG_INDEX_NAME);

	if (_rbfm->openFile(catalogTableFileName, catalogOfTables) != 0
			|| _rbfm->openFile(catalogColumnFileName, catalogOfColumns) != 0
			|| _rbfm->openFile(catalogIndexFileName, catalogOfIndexes) != 0 ) {
		//if some of the system files do not exist, clean up
		cleanup();

		//and compile new catalog files
		createCatalog();
	} else //the DB was in existence beforehand
	{
		//insert elements from the table Tables into _catalogTable
		//create list of attributes for table Tables
		std::vector<Attribute> table;
		const char* tableDescFields[] =
				{ "table_id", "table_name", "file_name" };

		table.push_back(
				(Attribute) {tableDescFields[0], AttrType(0), sizeof(unsigned int)});
		table.push_back(
				(Attribute) {tableDescFields[1], AttrType(2), MAX_SIZE_OF_NAME_IN_DB});
		table.push_back(
				(Attribute) {tableDescFields[2], AttrType(2), MAX_SIZE_OF_NAME_IN_DB});

		insertElementsFromTableIntoMap(catalogOfTables, table);

		//debugging
		/*map<string, TableInfo>::iterator tableMapIter = _catalogTable.begin();
		 for( ; tableMapIter != _catalogTable.end(); tableMapIter++ )
		 {
		 std::cout << "table name: " << (*tableMapIter).first;
		 std::cout << ", id: " << (*tableMapIter).second._id << ", name = " << (*tableMapIter).second._name << ", rid.pageNum: " << (*tableMapIter).second._rid.pageNum << ", rid.slotNum: " << (*tableMapIter).second._rid.slotNum;
		 std::cout << std::endl;
		 std::flush(std::cout);
		 }*/

		//insert elements from the table Columns into _catalogColumn
		//create list of attributes for table Columns
		std::vector<Attribute> column;
		const char* columnDescFields[] = { "table_id", "column_name",
				"column_type", "column_length", "column_status" };
		column.push_back(
				(Attribute) {columnDescFields[0], AttrType(0), sizeof(unsigned int)});
		column.push_back(
				(Attribute) {columnDescFields[1], AttrType(2), MAX_SIZE_OF_NAME_IN_DB});
		column.push_back(
				(Attribute) {columnDescFields[2], AttrType(0), sizeof(unsigned int)});
		column.push_back(
				(Attribute) {columnDescFields[3], AttrType(0), sizeof(unsigned int)});
		column.push_back(
				(Attribute) {columnDescFields[4], AttrType(0), sizeof(unsigned int)});

		insertElementsFromTableIntoMap(catalogOfColumns, column);

		_nextTableId = _catalogTable.size() + 1;

		//debugging
		/*map<int, vector<ColumnInfo> >::iterator columnMapIter = _catalogColumn.begin();
		 for( ; columnMapIter != _catalogColumn.end(); columnMapIter++ )
		 {
		 std::cout << "table id: " << (*columnMapIter).first;
		 vector<ColumnInfo>::iterator vIter = (*columnMapIter).second.begin(), vMax = (*columnMapIter).second.end();
		 for( ; vIter != vMax; vIter++ )
		 {
		 std::cout << "    type: " << (*vIter)._type << ", length: " << (*vIter)._length << ", name = " << (*vIter)._name << ", rid.pageNum: " << (*vIter)._rid.pageNum << ", rid.slotNum: " << (*vIter)._rid.slotNum << std::endl;
		 std::flush(std::cout);
		 }
		 std::flush(std::cout);
		 }*/
		//insert elements from the table Indexes into _catalogIndexes
		//create list of attributes for table Indexes
		std::vector<Attribute> index;
		const char* indexDescFields[] =
				{ "table_id", "column_name", "index_file_name" };

		index.push_back(
				(Attribute) {indexDescFields[0], AttrType(0), sizeof(unsigned int)});
		index.push_back(
				(Attribute) {indexDescFields[1], AttrType(2), MAX_SIZE_OF_NAME_IN_DB});
		index.push_back(
				(Attribute) {indexDescFields[2], AttrType(2), MAX_SIZE_OF_NAME_IN_DB});

		insertElementsFromTableIntoMap(catalogOfIndexes, index);

	}

	//debugging
	//std::cout << "Tables:" << endl;
	//printTable(CATALOG_TABLE_NAME);
	//std::cout << "\nColumns:" << endl;
	//printTable(CATALOG_COLUMN_NAME);
	//std::cout << endl;
}

RC RelationManager::printTable(const string& tableName) {
	RC errCode = 0;

	FileHandle tableHandle;

	//open file handle
	if ((errCode = _rbfm->openFile(tableName, tableHandle)) != 0) {
		//fail
		return errCode;
	}

	//get descriptor
	std::vector<Attribute> desc;
	if ((errCode = getAttributes(tableName, desc)) != 0) {
		//fail
		_rbfm->closeFile(tableHandle);
		return errCode;
	}

	//setup selected attribute names
	std::vector<std::string> attrNames;
	std::vector<Attribute>::iterator attrIter = desc.begin(), attrMax =
			desc.end();
	for (; attrIter != attrMax; attrIter++) {
		attrNames.push_back(attrIter->name);
	}

	//setup iterator for looping inside this table
	RM_ScanIterator iterator;
	if ((errCode = scan(tableName, "", NO_OP, NULL, attrNames, iterator))
			!= 0) {
		//fail
		_rbfm->closeFile(tableHandle);
		return errCode;
	}

	//setup arguments for iterating
	RID rid;
	void* data = malloc(PAGE_SIZE);
	memset(data, 0, PAGE_SIZE);

	//loop thru table file
	while (iterator.getNextTuple(rid, data) != RM_EOF) {
		RecordBasedFileManager::instance()->printRecord(desc, data);
		memset(data, 0, PAGE_SIZE);
	}

	//print entries from catalog maps
	//table catalog map
	std::map<string, TableInfo>::iterator catTableIter = _catalogTable.begin(), catTableEnd = _catalogTable.end();

	cout << "catalog Table" << endl;
	for( ; catTableIter != catTableEnd; catTableIter++ )
	{
		cout << "id: " << catTableIter->second._id << " => name: " << catTableIter->first << "\trid: " << catTableIter->second._rid.pageNum << " , " << catTableIter->second._rid.slotNum << endl;
	}
	cout << endl;

	///check that table entry is inside table map
	/*if (catTableIter == _catalogTable.end()) {
		//fail
		_rbfm->closeFile(tableHandle);
		free(data);
		exit(-1);
	}

	//get table id
	int id = catTableIter->second._id;

	//print content of entry
	std::cout << "from catalog table map: " << std::endl;
	std::cout << "    id= " << id << ", table_name= "
			<< catTableIter->second._name << ", rid = { "
			<< catTableIter->second._rid.pageNum << ", "
			<< catTableIter->second._rid.slotNum << " }" << std::endl;
*/
	//column catalog map
	std::map<int, std::vector<ColumnInfo> >::iterator catColumnIter = _catalogColumn.begin(), catColumnEnd = _catalogColumn.end();

	cout << "catalog Column" << endl;
	for( ; catColumnIter != catColumnEnd; catColumnIter++ )
	{
		cout << "id: " << catColumnIter->first << endl;
		std::vector<ColumnInfo>::iterator k = catColumnIter->second.begin(), k_max = catColumnIter->second.end();
		for( ; k != k_max; k++ )
		{
			cout << "\tname: " << k->_name << " ;length: " << k->_length << " ;type: " << k->_type << " ;rid: " << k->_rid.pageNum << " , " << k->_rid.slotNum << endl;
		}
	}
	cout << endl;
/*
	//check that table entry is inside column map
	if (catColumnIter == _catalogColumn.end()) {
		//fail
		_rbfm->closeFile(tableHandle);
		free(data);
		return -32;
	}

	std::vector<ColumnInfo> vecColInfo = catColumnIter->second;

	//loop thru column entries
	std::cout << "from catalog column map: ";
	std::vector<ColumnInfo>::iterator vecIter = vecColInfo.begin(), vecMax =
			vecColInfo.end();
	for (; vecIter != vecMax; vecIter++) {
		std::cout << "    name:" << vecIter->_name << ", length: "
				<< vecIter->_length << ", type: " << vecIter->_type
				<< ", rid: {" << vecIter->_rid.pageNum << ", "
				<< vecIter->_rid.slotNum << "}";
	}*/

	//deallocate buffer
	free(data);

	//close file
	if ((errCode = _rbfm->closeFile(tableHandle)) != 0) {
		//fail
		return errCode;
	}

	//success
	return errCode;
}

RelationManager::~RelationManager() {
}

bool RelationManager::isTableExisiting(const std::string& tableName) {
	return _catalogTable.find(tableName) != _catalogTable.end();
}

RC RelationManager::createTable(const string &tableName,
		const vector<Attribute> &attrs) {
	RC errCode = 0;

	//checking the input arguments
	if (tableName.length() == 0 || attrs.size() == 0) {
		return -37; //wrong table arguments
	}

	//make sure that the table does not exist, otherwise fail
	if (isTableExisiting(tableName)) {
		return -30; //attempt to re-create existing table
	}

	//create file for the table
	if ((errCode = _rbfm->createFile(tableName)) != 0) {
		//return error code
		return errCode;
	}

	//open catalog table's TABLE
	FileHandle tableHandle;

	if ((errCode = _rbfm->openFile(CATALOG_TABLE_NAME, tableHandle)) != 0) {
		//return error code
		_rbfm->destroyFile(tableName);
		return errCode;
	}

	unsigned int id = 0;

	//if there are unoccupied table IDs, then
	if (_freeTableIds.size() > 0) {
		//assign one from the top of the stack
		id = _freeTableIds.top();

		//remove it from the stack
		_freeTableIds.pop();
	}

	//if there were no unoccupied table IDs, then
	if (id == 0) {
		//assign using the next counter
		id = _nextTableId++;
	}

	//insert record (tuple) into catalog Table's table

	//setup record descriptor for catalog table
	std::vector<Attribute> desc;
	if ((errCode = getAttributes(CATALOG_TABLE_NAME, desc)) != 0) {
		//fail
		_rbfm->destroyFile(tableName);
		_rbfm->closeFile(tableHandle);
		return errCode;
	}

	//maintain record identifier for the element inserted into table Tables
	RID tableRid;

	//insert new record into table Tables
	if ((errCode = createRecordInTables(tableHandle, desc, tableName.c_str(),
			id, tableRid)) != 0) {
		//return error code
		_rbfm->destroyFile(tableName);
		_rbfm->closeFile(tableHandle);
		return errCode;
	}

	//insert an entry into _catalogTable
	_catalogTable.insert(
			std::pair<string, TableInfo>(tableName,
					(TableInfo) {id, tableName, tableRid}));

	//close table handle
	if ((errCode = _rbfm->closeFile(tableHandle)) != 0) {
		//return error code
		return errCode;
	}

	//open catalog table Columns
	FileHandle columnHandle;

	if ((errCode = _rbfm->openFile(CATALOG_COLUMN_NAME, columnHandle)) != 0) {
		//return error code
		return errCode;
	}

	//setup record descriptor for catalog column
	desc.clear();
	if ((errCode = getAttributes(CATALOG_COLUMN_NAME, desc)) != 0) {
		//fail
		_rbfm->destroyFile(tableName);
		_rbfm->closeFile(columnHandle);
		return errCode;
	}

	std::vector<ColumnInfo> tableInfo;
	std::vector<Attribute>::const_iterator i = attrs.begin(), max = attrs.end();
	for (; i != max; i++) {
		RID columnRid;

		//create record in table Columns
		if ((errCode = createRecordInColumns(columnHandle, desc, id,
				(*i).name.c_str(), (*i).type, (*i).length, false, columnRid)) != 0) {
			//fail
			_rbfm->destroyFile(tableName);
			_rbfm->closeFile(columnHandle);
			return errCode;
		}

		//create column information entry
		tableInfo.push_back(
				(ColumnInfo) {(*i).name, (*i).type, (*i).length, 0, columnRid});
	}

	//close column handle
	if ((errCode = _rbfm->closeFile(columnHandle)) != 0) {
		//fail
		_rbfm->destroyFile(tableName);
		_rbfm->closeFile(columnHandle);
		return errCode;
	}

	//for fast lookup -> insert <table id, list of column info> for TABLE INTO _catalogColumn
	_catalogColumn.insert(
			std::pair<int, std::vector<ColumnInfo> >(id, tableInfo));

	//success
	return 0;
}

std::string RelationManager::composeIndexName(const string& tableName, const string& columnName)
{
	return tableName + "_" + columnName + "_";
}

bool RelationManager::isCatalogTable(const string& name)
{
	const char* ptrName = name.c_str();

	return strcmp(ptrName, CATALOG_COLUMN_NAME) == 0 ||
		   strcmp(ptrName, CATALOG_TABLE_NAME) == 0 ||
		   strcmp(ptrName, CATALOG_INDEX_NAME) == 0;
}

RC RelationManager::createIndex(const string& tableName, const string& attributeName)
{
	RC errCode = 0;

	IndexManager* ix = IndexManager::instance();

	//check if table exists
	std::map<string, TableInfo>::iterator tableIter = _catalogTable.find(
			tableName);
	if (tableIter == _catalogTable.end())
	{
		//fail
		return -31; //accessing table that does not exists
	}

	//determine table id
	unsigned int id = tableIter->second._id;

	//if entry with this table id does not exist, then create one
	if( _catalogIndex.find(id) == _catalogIndex.end() )
	{
		_catalogIndex.insert( std::pair<int, std::map<std::string, IndexInfo> >(id, std::map<std::string, IndexInfo>() ) );
	}

	//check if index already exists
	if( _catalogIndex[id].find(attributeName) != _catalogIndex[id].end() )
	{
		//index already exists
		return -38;
	}

	FileHandle indexesHandle;
	if( (errCode = _rbfm->openFile(CATALOG_INDEX_NAME, indexesHandle)) != 0 )
	{
		//return error code
		return errCode;
	}

	RID indexesRid;

	//create index file
	//string indexName = tableName + "_" + attributeName + "_";
	string indexName = composeIndexName(tableName, attributeName);

	//but only if this is not the case of catalog table, since all files for them already been created
	if( isCatalogTable(tableName) == false )
	{
		if( (errCode = ix->createFile(indexName, INDEX_DEFAULT_NUM_PAGES)) != 0 )
		{
			_rbfm->closeFile(indexesHandle);
			return errCode;
		}
	}
	else
	{
		return -100;
	}

	//setup record descriptor for catalog index
	std::vector<Attribute> desc;
	desc.clear();
	if ((errCode = getAttributes(CATALOG_INDEX_NAME, desc)) != 0) {
		//fail
		_rbfm->closeFile(indexesHandle);
		return errCode;
	}

	//insert record into Indexes table
	if( (errCode = createRecordInIndexes(indexesHandle, desc, id, attributeName.c_str(), indexName.c_str(), indexesRid)) != 0 )
	{
		_rbfm->closeFile(indexesHandle);
		return errCode;
	}

	//insert entry into Indexes table map
	IndexInfo info;
	info._indexName = indexName;
	info._rid.pageNum = indexesRid.pageNum;
	info._rid.slotNum = indexesRid.slotNum;

	//insert index info for the specified attribute name into index catalog
	_catalogIndex[id].insert( std::pair<std::string, IndexInfo>(attributeName, info) );

	//close column handle
	if ((errCode = _rbfm->closeFile(indexesHandle)) != 0) {
		//fail
		return errCode;
	}

	//open actual index file
	IXFileHandle ixFileHandle;
	if( (errCode = ix->openFile(indexName, ixFileHandle)) != 0 )
	{
		_rbfm->closeFile(indexesHandle);
		return errCode;
	}

	//find the attribute that we need to index
	desc.clear();
	if ((errCode = getAttributes(tableName, desc)) != 0) {
		//fail
		_rbfm->closeFile(indexesHandle);
		return errCode;
	}
	Attribute attribute;
	int indexAttr = 0;
	for( ; indexAttr < (int)desc.size(); indexAttr++ )
	{
		if( desc[indexAttr].name == attributeName )
		{
			attribute.length = desc[indexAttr].length;
			attribute.name = desc[indexAttr].name;
			attribute.type = desc[indexAttr].type;
			break;
		}
	}

	//scan thru existing table and insert elements into the index
	RM_ScanIterator iterator;
	string condAttribute;
	vector<string> selAttr;
	selAttr.push_back(attributeName);
	if( (errCode = scan(tableName, condAttribute, NO_OP, NULL, selAttr, iterator)) != 0 )
	{
		_rbfm->closeFile(indexesHandle);
		return errCode;
	}

	//allocate buffer for scanning
	void* dataBuf = malloc(PAGE_SIZE);
	memset(dataBuf, 0, PAGE_SIZE);
	RID rid = {0, 0};

	while (iterator.getNextTuple(rid, dataBuf) == 0)
	{
		//dataBuf is entire record => but we need a key (one of fields in this record)
		//since index does not store whole record but only <key, RID>

		//find attribute in the record
		//unsigned int offsetToCurrentAttrValue = getOffset(dataBuf, desc, indexAttr);	//error: no need to offset, since only a
																						//		 required (indexing) attribute is returned by an iterator
		char* key = (char*)dataBuf;

		//go ahead and insert index entry
		if ( (errCode = ix->insertEntry(ixFileHandle, attribute, key, rid)) != 0)
		{
			iterator.close();
			_rbfm->closeFile(indexesHandle);
			return errCode;
		}
	}

	iterator.close();
	free(dataBuf);

	if( (errCode = ix->closeFile(ixFileHandle)) != 0 )
	{
		_rbfm->closeFile(indexesHandle);
		return errCode;
	}

	//success
	return errCode;
}

RC RelationManager::deleteTable(const string &tableName) {
	RC errCode = 0;

	//checking that input argument makes sense
	if (tableName.size() == 0) {
		//fail
		return -37; //wrong table arguments
	}

	if( tableName == CATALOG_COLUMN_NAME || tableName == CATALOG_TABLE_NAME || tableName == CATALOG_INDEX_NAME )
	{
		//cannot delete catalog
		return -34;
	}

	//check if table exists
	std::map<string, TableInfo>::iterator tableIter = _catalogTable.find(
			tableName);
	if (tableIter == _catalogTable.end()) {
		//fail
		return -31; //accessing table that does not exists
	}

	//fast access for table information
	TableInfo info = (*tableIter).second;

	//1, remove record inside Tables
	//find and remove record inside the table Tables for this table
	if ((errCode = deleteTuple(CATALOG_TABLE_NAME, info._rid)) != 0) {
		//fail
		return errCode; //most likely record does not exist
	}

	//2. remove file representing the table
	//remove file corresponding to this table (using the information from the record from Table of tables, i.e. 3rd field = file-name)
	if ((errCode = _rbfm->destroyFile(info._name)) != 0) {
		//fail
		return errCode; //file does not exist
	}

	//get list of column information
	std::map<int, std::vector<ColumnInfo> >::iterator columnIter =
			_catalogColumn.find((int) info._id);

	if (columnIter == _catalogColumn.end()) {
		//fail
		return -31; //accessing column table that does not exist
	}

	std::vector<ColumnInfo> vecColInfo = (*columnIter).second;

	//3. remove records from Columns
	//find and remove records inside the table Columns, representing separate fields of this table
	std::vector<ColumnInfo>::iterator colInfoIter = vecColInfo.begin(), max =
			vecColInfo.end();
	for (; colInfoIter != max; colInfoIter++) {
		if ((errCode = deleteTuple(CATALOG_COLUMN_NAME, (*colInfoIter)._rid))
				!= 0) {
			//fail
			return errCode; //most likely record does not exist
		}
	}

	//include unoccupied rid into the free list
	_freeTableIds.push(info._id);

	//4. remove entries from both maps corresponding to tables Tables and Columns
	_catalogTable.erase(tableIter);
	_catalogColumn.erase(columnIter);

	//success
	return errCode;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	RC errCode = 0;

	//get table id
	std::map<string, TableInfo>::iterator tableIter = _catalogTable.find(tableName);
	unsigned id = tableIter->second._id;

	//determine RID of deleted record inside catalog INDEX

	//for that get list of tuples <attributeName, indexInfo> for this table
	std::map<int, std::map<std::string, IndexInfo> >::iterator
		indexesIter = _catalogIndex.find(id);

	IndexManager* ix = IndexManager::instance();

	//loop thru all attributes of the table to find the proper one => to get indexInfo for that attrubute
	std::map<std::string, IndexInfo>::iterator i = indexesIter->second.find(attributeName);

	if( i != indexesIter->second.end() )
	{
		return -38;
	}

	//remove index file representing current attribute
	if( (errCode = ix->destroyFile(i->second._indexName)) != 0 )
	{
		return errCode;
	}

	//remove record from Indexes table
	if( (errCode = deleteTuple(CATALOG_INDEX_NAME, i->second._rid)) )
	{
		return errCode;
	}

	//success
	return errCode;
}

RC RelationManager::getAttributes(const string &tableName,
		vector<Attribute> &attrs)
		{
	RC errCode = 0;
	if (tableName.empty())
		return -37; //tableName no valid

	//check if the table exist in the map
	if (_catalogTable.find(tableName) == _catalogTable.end()) {
		return -31;
	}

	//get the tableId
	int tableId = _catalogTable[tableName]._id;

	//get the handle for columns file
	FileHandle fileHandle;
	if ((errCode = _rbfm->openFile(CATALOG_COLUMN_NAME, fileHandle)) != 0)
		return errCode;

	//get the vector of ColumnInfo
	vector<ColumnInfo> columnsInfo = _catalogColumn[tableId];

	//create buffer for storing record
	//void* recordData = malloc(sizeof(unsigned int) + //table_id
	//		(sizeof(unsigned int) + MAX_SIZE_OF_NAME_IN_DB) + //size + column_name
	//		sizeof(unsigned int) + //column type
	//		sizeof(unsigned int)); //column length
	//possible issue - allocated size would be larger than the PAGE_SIZE
	void* recordData = malloc(MAX_SIZE_OF_RECORD);
	vector<Attribute> recordDescriptor;

	for (unsigned int i = 0; i < columnsInfo.size(); i++) {
		ColumnInfo column = columnsInfo[i];
		/*
		 * Obs: Here the correct way is checking the ColumnInfo on Disk, but
		 * there is no RID to access to it.
		 *
		 */
		// set the attribute and put into the vector
		Attribute attr;
		attr.name = column._name;
		attr.type = column._type;
		attr.length = (column._isDropped == 0 ? column._length : 0);
		attrs.push_back(attr);

	}

	free(recordData);
	errCode = _rbfm->closeFile(fileHandle);

	return errCode;
}

//get offset inside "data" corresponding to the specified attribute by "attrIndex"
unsigned int RelationManager::getOffset(const void* data, const std::vector<Attribute> desc, int attrIndex)
{
	unsigned int offset = 0;

    //declare constant iterator for descriptor values
	vector<Attribute>::const_iterator i = desc.begin(), max = desc.end();

	//current pointer for iterating within data
	const void* ptr = data;

	//loop through all descriptors using aforementioned iterator
	for(; i != max && attrIndex > 0; i++, attrIndex--)
	{
		//depending on the type of element, do a separate conversion and printing actions
		switch(i->type)
		{
		case TypeInt:

			offset += sizeof(int);

			break;
		case TypeReal:

			offset += sizeof(float);

			break;
		case TypeVarChar:

			offset += ((int*)ptr)[0] + sizeof(unsigned int);

			break;
		}

		//update pointer
		ptr = (void*)((char*)data + offset);

	}

	return offset;
}

RC RelationManager::insertIXEntry(const string& tableName, const std::vector<Attribute> attrs, const void* data, const RID& rid)
{
	RC errCode = 0;

	//get table id (since catalog Index has such outer key
	int tableId = _catalogTable[tableName]._id;

	IndexManager* ix = IndexManager::instance();

	//iterate over ONLY those attributes that were indexed

	//setup iterators for looping thru indexed attributes
	std::map<int, std::map<std::string, IndexInfo> >::iterator
		indexIter = _catalogIndex.find(tableId);

	if( indexIter == _catalogIndex.end() )
	{
		return 0;	//no indexes to update, so just return
	}

	std::map<std::string, IndexInfo>::iterator
		i = indexIter->second.begin(), max = indexIter->second.end();

	for( ; i != max; i++ )
	{
		//determine current attribute
		Attribute curAttr;
		int indexAttr = 0;
		for(; indexAttr < (int)attrs.size(); indexAttr++)
		{
			if( attrs[indexAttr].name == i->first )
			{
				curAttr.length = attrs[indexAttr].length;
				curAttr.name = attrs[indexAttr].name;
				curAttr.type = attrs[indexAttr].type;
				break;
			}
		}

		string name = composeIndexName(tableName, curAttr.name);
		IXFileHandle ixHandle;

		//open IX files
		if( (errCode = ix->openFile(name, ixHandle)) != 0 )
		{
			//fail
			return errCode;
		}

		//find attribute in the record
		unsigned int offsetToCurrentAttrValue = getOffset(data, attrs, indexAttr);
		char* key = (char*)data + offsetToCurrentAttrValue;

		//insert entry into IX component
		if( (errCode = ix->insertEntry(ixHandle, curAttr, (void*)key, rid)) != 0 )
		{
			//fail
			ix->closeFile(ixHandle);
			return errCode;
		}

		//close IX files
		if( (errCode = ix->closeFile(ixHandle)) != 0 )
		{
			//fail
			return errCode;
		}
	}

	//success
	return errCode;
}

RC RelationManager::insertTuple(const string &tableName, const void *data,
		RID &rid) {

	RC errCode = 0;

	//check if there is inconsistent data
	if (tableName.empty() || data == NULL)
		return -37;

	vector<Attribute> attrs;
	//get the attributes for this table
	if ((errCode = getAttributes(tableName, attrs)) != 0) {
		//fail
		return errCode;
	}

	//create the handle and open the table
	FileHandle fileHandle;
	if ((errCode = _rbfm->openFile(tableName, fileHandle)) != 0)
		return errCode;

	//insert the record
	if ((errCode = _rbfm->insertRecord(fileHandle, attrs, data, rid)) != 0)
	{
		//close file
		_rbfm->closeFile(fileHandle);

		//return error code
		return errCode;
	}

	//insert record into IX component - into all indexes associated with this table
	if( (errCode = insertIXEntry(tableName, attrs, data, rid)) != 0 )
	{
		//fail
		_rbfm->closeFile(fileHandle);
		return errCode;
	}

	errCode = _rbfm->closeFile(fileHandle);

	return errCode;

}

RC RelationManager::deleteTuples(const string &tableName) {
	RC errCode = 0;

	//checking that table name is not empty string
	if (tableName.empty())
		return -37;

	FileHandle tableHandle;

	//open file that contains table tuples
	if ((errCode = _rbfm->openFile(tableName, tableHandle)) != 0) {
		//fail
		return errCode;
	}

	//delete records from the opened file
	if ((errCode = _rbfm->deleteRecords(tableHandle)) != 0) {
		//fail
		_rbfm->closeFile(tableHandle);
		return errCode;
	}

	//close file
	if ((errCode = _rbfm->closeFile(tableHandle)) != 0) {
		//fail
		return errCode;
	}

	IndexManager* ix = IndexManager::instance();

	//get table id (since catalog Index has such outer key
	int tableId = _catalogTable[tableName]._id;

	//find index corresponding to this table
	std::map<int, std::map<std::string, IndexInfo> >::iterator
		indexIter = _catalogIndex.find(tableId);

	if( indexIter == _catalogIndex.end() )
	{
		return errCode;	//if this table has no indexes, then just leave
	}

	vector<Attribute> attrs;
	//get the attributes for this table
	if ((errCode = getAttributes(tableName, attrs)) != 0)
	{
		//fail
		return errCode;
	}

	std::map<std::string, IndexInfo>::iterator
		k = indexIter->second.begin(), kmax = indexIter->second.end();

	//iterate over the list of indexed attributes
	for( ; k != kmax; k++ )
	{
		//determine current attribute
		Attribute curAttr;
		int indexAttr = 0;
		for(; indexAttr < (int)attrs.size(); indexAttr++)
		{
			if( attrs[indexAttr].name == k->first )
			{
				curAttr.length = attrs[indexAttr].length;
				curAttr.name = attrs[indexAttr].name;
				curAttr.type = attrs[indexAttr].type;
				break;
			}
		}

		//find tuple <attributeName, IndexInfo> inside the catalog Indexes corresponding to this attribute
		//std::map<std::string, IndexInfo>::iterator attrIter = indexIter->second.find( curAttr.name );
		//string name = attrIter->second._indexName;
		string name = composeIndexName(tableName, curAttr.name);
		IXFileHandle ixHandle;

		//open IX files
		if( (errCode = ix->openFile(name, ixHandle)) != 0 )
		{
			//fail
			return errCode;
		}

		//scan thru the index corresponding to the current attribute
		IX_ScanIterator ix_ScanIterator;
		if( (errCode = ix->scan(ixHandle, curAttr, NULL, NULL, true, true, ix_ScanIterator)) != 0 )
		{
			//fail
			ix->closeFile(ixHandle);
			return errCode;
		}

		//loop thru the index files for current attribute and delete all entries sequentially
		void* key = malloc(PAGE_SIZE);
		memset(key, 0, PAGE_SIZE);
		RID rid;
	    while(ix_ScanIterator.getNextEntry(rid, key) == 0)
	    {
	    	//delete this entry
	    	if( (errCode = ix->deleteEntry(ixHandle, curAttr, key, rid)) != 0 )
	    	{
	    		//fail
	    		ix_ScanIterator.close();
				ix->closeFile(ixHandle);
				free(key);
	    		return errCode;
	    	}
	    }

	    ix_ScanIterator.close();
	    free(key);

		//close IX files
		if( (errCode = ix->closeFile(ixHandle)) != 0 )
		{
			//fail
			free(key);
			return errCode;
		}
	}

	//success
	return errCode;
}

RC RelationManager::deleteIXEntry(const string& tableName, const std::vector<Attribute> attrs, const void* data, const RID& rid)
{
	RC errCode = 0;

	//get table id (since catalog Index has such outer key
	int tableId = _catalogTable[tableName]._id;

	IndexManager* ix = IndexManager::instance();

	//iterate over ONLY those attributes that were indexed

	//setup iterators for looping thru indexed attributes
	std::map<int, std::map<std::string, IndexInfo> >::iterator
		indexIter = _catalogIndex.find(tableId);

	if( indexIter == _catalogIndex.end() )
	{
		return 0;	//no indexes to update, so just return
	}

	std::map<std::string, IndexInfo>::iterator
		i = indexIter->second.begin(), max = indexIter->second.end();

	for( ; i != max; i++ )
	{
		//determine current attribute
		Attribute curAttr;
		int indexAttr = 0;
		for(; indexAttr < (int)attrs.size(); indexAttr++)
		{
			if( attrs[indexAttr].name == i->first )
			{
				curAttr.length = attrs[indexAttr].length;
				curAttr.name = attrs[indexAttr].name;
				curAttr.type = attrs[indexAttr].type;
				break;
			}
		}

		//find tuple <attributeName, IndexInfo> inside the catalog Indexes corresponding to this attribute
		//std::map<std::string, IndexInfo>::iterator attrIter = indexIter->second.find( curAttr.name );
		//string name = attrIter->second._indexName;
		string name = composeIndexName(tableName, curAttr.name);
		IXFileHandle ixHandle;

		//open IX files
		if( (errCode = ix->openFile(name, ixHandle)) != 0 )
		{
			//fail
			return errCode;
		}

		//find attribute in the record
		unsigned int offsetToCurrentAttrValue = getOffset(data, attrs, indexAttr);
		char* key = (char*)data + offsetToCurrentAttrValue;

		//insert entry into IX component
		if( (errCode = ix->deleteEntry(ixHandle, curAttr, (void*)key, rid)) != 0 )
		{
			//fail
			ix->closeFile(ixHandle);
			return errCode;
		}

		//close IX files
		if( (errCode = ix->closeFile(ixHandle)) != 0 )
		{
			//fail
			return errCode;
		}
	}

	//success
	return errCode;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {

	RC errCode = 0;
	//check if there is inconsistent data
	if (tableName.empty() || rid.pageNum == 0)
		return -37;

	vector<Attribute> attrs;
	//get the attributes for this table
	if ((errCode = getAttributes(tableName, attrs)) != 0) {
		//fail
		return errCode;
	}

	//create the handle and open the table
	FileHandle fileHandle;
	if ((errCode = _rbfm->openFile(tableName, fileHandle)) != 0)
		return errCode;

	//delete record in IX component (before deleting it in RBFM)
	//but to do that, IX needs a key using which it can find the record inside its files organization

	//so first need to read tuple
	void* data = malloc(PAGE_SIZE);
	memset(data, 0, PAGE_SIZE);
	if ((errCode = _rbfm->readRecord(fileHandle, attrs, rid, data)) != 0)
	{
		_rbfm->closeFile(fileHandle);
		free(data);
		return errCode;
	}

	if( (errCode = deleteIXEntry(tableName, attrs, data, rid)) != 0 )
	{
		_rbfm->closeFile(fileHandle);
		free(data);
		return errCode;
	}
	free(data);

	//delete the record
	if ((errCode = _rbfm->deleteRecord(fileHandle, attrs, rid)) != 0)
	{
		_rbfm->closeFile(fileHandle);
		return errCode;
	}

	_rbfm->closeFile(fileHandle);

	return errCode;
}

RC RelationManager::updateTuple(const string &tableName, const void *data,
		const RID &rid) {
	RC errCode = 0;
	//check if there is inconsistent data
	if (tableName.empty() || data == NULL || rid.pageNum == 0)
		return -37;

	vector<Attribute> attrs;
	//get the attributes for this table
	if ((errCode = getAttributes(tableName, attrs)) != 0) {
		//fail
		return errCode;
	}

	//create the handle and open the table
	FileHandle fileHandle;
	if ((errCode = _rbfm->openFile(tableName, fileHandle)) != 0)
		return errCode;

	//data stores new instance of entry, with field(s) updated already
	//first, however, we need an copy of original entry, so that using
	//it we could find and delete its records in all index files

	//allocate buffer for storing "original" record
	void* readInBuffer = malloc(PAGE_SIZE);
	memset(readInBuffer, 0, PAGE_SIZE);

	//read the original record
	if ((errCode = _rbfm->readRecord(fileHandle, attrs, rid, readInBuffer)) != 0)
	{
		free(readInBuffer);
		_rbfm->closeFile(fileHandle);
		return errCode;
	}

	//remove record from IX component
	if( (errCode = deleteIXEntry(tableName, attrs, readInBuffer, rid)) != 0 )
	{
		free(readInBuffer);
		return errCode;
	}

	//deallocate buffer for "original" record
	free(readInBuffer);

	//update the record
	if ((errCode = _rbfm->updateRecord(fileHandle, attrs, data, rid)) != 0)
	{
		_rbfm->closeFile(fileHandle);
		return errCode;
	}

	if( (errCode = _rbfm->closeFile(fileHandle)) != 0 )
	{
		return errCode;
	}

	//now insert new item (i.e. updated) into IX component, since IX interface does not support update routine
	if( (errCode = insertIXEntry(tableName, attrs, data, rid)) != 0 )
	{
		return errCode;
	}

	return errCode;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid,
		void *data) {
	RC errCode = 0;
	//check if there is inconsistent data
	if (tableName.empty() || data == NULL || rid.pageNum == 0)
		return -37;

	vector<Attribute> attrs;
	//get the attributes for this table
	if ((errCode = getAttributes(tableName, attrs)) != 0) {
		//fail
		return errCode;
	}

	//create the handle and open the table
	FileHandle fileHandle;
	if ((errCode = _rbfm->openFile(tableName, fileHandle)) != 0)
		return errCode;

	//read the tuple
	if ((errCode = _rbfm->readRecord(fileHandle, attrs, rid, data)) != 0)
	{
		_rbfm->closeFile(fileHandle);
		return errCode;
	}

	if ((errCode = _rbfm->closeFile(fileHandle)) != 0)
		return errCode;

	return errCode;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid,
		const string &attributeName, void *data) {
	RC errCode = 0;
	//check if there is inconsistent data
	if (tableName.empty() || data == NULL || rid.pageNum == 0)
		return -37;

	vector<Attribute> attrs;
	//get the attributes for this table
	if ((errCode = getAttributes(tableName, attrs)) != 0) {
		//fail
		return errCode;
	}

	//create the handle and open the table
	FileHandle fileHandle;
	if ((errCode = _rbfm->openFile(tableName, fileHandle)) != 0)
		return errCode;

	//read attribute
	if ((errCode = _rbfm->readAttribute(fileHandle, attrs, rid, attributeName,
			data)) != 0)
	{
		_rbfm->closeFile(fileHandle);
		return errCode;
	}

	_rbfm->closeFile(fileHandle);

	return errCode;
}

RC RelationManager::reorganizePage(const string &tableName,
		const unsigned pageNumber) {
	RC errCode = 0;
	//check if there is inconsistent data
	if (tableName.empty())
		return -37;

	vector<Attribute> attrs;
	//get the attributes for this table
	if ((errCode = getAttributes(tableName, attrs)) != 0) {
		//fail
		return errCode;
	}

	//create the handle and open the table
	FileHandle fileHandle;
	if ((errCode = _rbfm->openFile(tableName, fileHandle)) != 0)
		return errCode;

	//reorganize Page
	if ((errCode = _rbfm->reorganizePage(fileHandle, attrs, pageNumber)) != 0) {
		//fail
		_rbfm->closeFile(fileHandle);
		return errCode;
	}

	if ((errCode = _rbfm->closeFile(fileHandle)) != 0) {
		//fail
		return errCode;
	}

	//success
	return errCode;
}

RM_ScanIterator::RM_ScanIterator() :
		_iterator() {
	//nothing
}

RM_ScanIterator::~RM_ScanIterator() {
	//nothing
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
	return _iterator.getNextRecord(rid, data);
}

RC RM_ScanIterator::close() {
	RC errCode = 0;

	//attempt to close RBFM iterator
	if ((errCode = _iterator.close()) != 0) {
		//fail
		return errCode;
	}

	//success
	return errCode;
}

RBFM_ScanIterator& RM_ScanIterator::getIterator() {
	return _iterator;
}

RC RelationManager::scan(const string &tableName,
		const string &conditionAttribute, const CompOp compOp,
		const void *value, const vector<string> &attributeNames,
		RM_ScanIterator &rm_ScanIterator) {

	RC errCode = 0;
	//check if there is inconsistent data
	if (tableName.empty())
		return -37;

	//create the handle and open the table
	FileHandle fileHandle;
	if ((errCode = _rbfm->openFile(tableName, fileHandle)) != 0)
		return errCode;

	vector<Attribute> attrs;
	//get the attributes for this table
	if ((errCode = getAttributes(tableName, attrs)) != 0) {
		//fail
		_rbfm->closeFile(fileHandle);
		return errCode;
	}

	//set up scan RBFM
	if ((errCode = _rbfm->scan(fileHandle, attrs, conditionAttribute, compOp,
			value, attributeNames, rm_ScanIterator.getIterator())) != 0) //check this part
		return errCode;

	return errCode;
}

// Extra credit
RC RelationManager::dropAttribute(const string &tableName,
		const string &attributeName) {
	int errCode = 0;
	//find the tableID from catalog
	unsigned int tableId;
	if ((tableId = _catalogTable.find(tableName)->second._id) == 0) //get the table id)
		return -31;	//accessing table that does not exist

	//get columnInfo vector from catalog in memory
	std::vector<ColumnInfo> columnsInfo = _catalogColumn[tableId];

	unsigned int index;
	for (index = 0; index < columnsInfo.size(); index++) { //fin the column
		if (columnsInfo[index]._name.compare(attributeName.c_str()) == 0) {
			break;
		}
	}

	//if the column does not exist, error
	if (index == columnsInfo.size())
		return -35; //specified column does not exist

	//delete the attribute on disk
	//REASON WHY NOT TO DELETE:
	//INSTEAD, a record inside the catalog COLUMNS will be marked as deleted with use of one extra field called isDropped
	//if isDropped = 0 => field exists, if isDropped = 1 => field is deleted!
	//NO actual deletion takes place, since the information about the deleted item is necessary for accessing records.
	//Keep in mind that the actual records are not updated, so they contain data in the original way, i.e. with deleted field.
	//So after attribute is dropped, this field will be by-passed when record is read or written to...
	//if ((errCode = deleteTuple(CATALOG_COLUMN_NAME, columnsInfo[index]._rid))
	//		!= 0) {
	//	//fail
	//	return errCode; //most likely record does not exist
	//}

	//instead, declare this attributed to be deleted
	//allocate buffer for record to be updated inside the column
	void* dataPtr = malloc(PAGE_SIZE);
	memset(dataPtr, 0, PAGE_SIZE);

	//first read the column into the dataPtr
	if((errCode = readTuple(CATALOG_COLUMN_NAME, columnsInfo[index]._rid, dataPtr)) != 0)
	{
		//deallocate buffer
		free(dataPtr);
		//return error code
		return errCode;
	}

	char* recordPtr = (char*)dataPtr;
	//this is a decoded record, so there is no directory inside it, which means that a manual loop over the fields is required
	recordPtr += sizeof(unsigned int);
	recordPtr += *((unsigned int*)recordPtr);
	recordPtr += 3 * sizeof(unsigned int);

	//has this field has already been dropped
	if( *((unsigned int*)recordPtr) == 1 || columnsInfo[index]._isDropped == 1 )
	{
		//deallocate buffer
		free(dataPtr);

		//return error code
		return -36;
	}

	//change the field <column-status> inside the retrieved record to true, which would mean that the represented column is dropped
	*((unsigned int*)recordPtr) = 1;

	//update record
	if((errCode = updateTuple(CATALOG_COLUMN_NAME, dataPtr, columnsInfo[index]._rid)) != 0)
	{
		//free buffer
		free(dataPtr);
		//return error code
		return errCode;
	}

	//finally delete the attribute from catalog in memory
	//similar argument applies here!!! (for reason see above => "REASON WHY NOT TO DELETE")
	//columnsInfo.erase(columnsInfo.begin() + index);

	//instead reset the isDropped to 1 inside map column catalog
	_catalogColumn[tableId][index]._isDropped = 1;

	return errCode;
}

// Extra credit
RC RelationManager::addAttribute(const string &tableName,
		const Attribute &attr) {
	int errCode = 0;
	//find the tableID from catalog
	unsigned int tableId;
	if ((tableId = _catalogTable.find(tableName)->second._id) == 0) //get the table id)
		return -32; // set error number

	//open file and set handle
	FileHandle columnHandle;
	if ((errCode = _rbfm->openFile(CATALOG_COLUMN_NAME, columnHandle)) != 0) {
		//abort
		return errCode;
	}

	//set file to be modifiable solely by the DB (and not by users)
	bool success = true;
	columnHandle.setAccess(only_system_can_modify, success);
	if (success == false) {
		//abort
		exit(-1);
	}

	//setup the descriptor for Columns (my bad, not exactly the smartest parameter for the function that inserts record into table Columns...)
	std::vector<Attribute> column;
	const char* columnDescFields[] = { "table_id", "column_name", "column_type",
			"column_length", "column_status" };
	column.push_back(
			(Attribute) {columnDescFields[0], AttrType(0), sizeof(unsigned int)});
	column.push_back(
			(Attribute) {columnDescFields[1], AttrType(2), MAX_SIZE_OF_NAME_IN_DB});
	column.push_back(
			(Attribute) {columnDescFields[2], AttrType(0), sizeof(unsigned int)});
	column.push_back(
			(Attribute) {columnDescFields[3], AttrType(0), sizeof(unsigned int)});
	column.push_back(
			(Attribute) {columnDescFields[4], AttrType(0), sizeof(unsigned int)});

	RID ridColumn;
	//add the new attribute on catalog in disk
	if ((errCode = createRecordInColumns(columnHandle, column, tableId,
			attr.name.c_str(), attr.type, attr.length, false, ridColumn)) != 0)
	{
		_rbfm->closeFile(columnHandle);
		return errCode; // set error number
	}

	//add the new attribute to catalog column in memory
	_catalogColumn[tableId].push_back(
			(ColumnInfo) {attr.name.c_str(), attr.type, attr.length, 0, ridColumn});

	if ((errCode = _rbfm->closeFile(columnHandle)) != 0)
		return errCode; // set error number

	return errCode;
}

// Extra credit
RC RelationManager::reorganizeTable(const string &tableName) {
	//I guess it is just call reorganizeFile from RBFM
	int errCode = 0;
	FileHandle tableHandle;
	vector<Attribute> attrs;

	if ((errCode = _rbfm->openFile(tableName, tableHandle)) != 0) {
		//abort
		return errCode;
	}

	if ((errCode = getAttributes(tableName, attrs)) != 0) {
		//abort
		_rbfm->closeFile(tableHandle);
		return errCode;
	}

	if ((errCode = _rbfm->reorganizeFile(tableHandle, attrs)) != 0) {
		//abort
		_rbfm->closeFile(tableHandle);
		return errCode;
	}

	if ((errCode = _rbfm->closeFile(tableHandle)) != 0)
		return errCode; // set error number

	return errCode;
}

RC RelationManager::indexScan(const string &tableName,
	  const string &attributeName,
	  const void *lowKey,
	  const void *highKey,
	  bool lowKeyInclusive,
	  bool highKeyInclusive,
	  RM_IndexScanIterator &rm_IndexScanIterator)
{
	RC errCode = 0;

	//check whether given table exists
	if( isTableExisiting(tableName) == false )
	{
		//if not, then fail
		return -31;
	}

	//get table id (since catalog Index has such outer key
	int tableId = _catalogTable[tableName]._id;

	//find index corresponding to this table
	std::map<int, std::map<std::string, IndexInfo> >::iterator indexIter = _catalogIndex.find(tableId);

	vector<Attribute> attrs;
	//get the attributes for this table
	if ((errCode = getAttributes(tableName, attrs)) != 0)
	{
		//fail
		return errCode;
	}

	int i = 0, max = (int)attributeName.size();

	//CLI:IndexScan has all of its attribute names appended with table name, like "TableName.AttributeName"
	//but here only attribute name is needed, so loop thru the string and take only the relevant part
	string attrName;
	bool foundDot = false;
	for( i = 0; i < (int)attributeName.size(); i++ )
	{
		if( foundDot )
		{
			attrName.push_back( attributeName[i] );
		}
		if( attributeName[i] == '.' )
		{
			foundDot = true;
		}
	}
	if( foundDot == false )
	{
		attrName = attributeName;
	}

	//find attribute
	Attribute attr;
	i = 0; max = (int)attrs.size();
	for( ; i < max; i++ )
	{
		if( attrs[i].name == attrName )
		{
			attr.length = attrs[i].length;
			attr.name = attrs[i].name;
			attr.type = attrs[i].type;
			break;
		}
	}

	//if such attribute does not exist, fail
	if( i > max )
	{
		return -39;
	}

	//determine index name
	//std::map<std::string, IndexInfo>::iterator attrIter = indexIter->second.find( attributeName );

	string name = composeIndexName(tableName, attrName);
	IndexManager* ix = IndexManager::instance();

	//open IX files
	if( (errCode = ix->openFile(name, rm_IndexScanIterator._fileHandle)) != 0 )
	{
		//fail
		return errCode;
	}

	//setup scan
	if( (errCode = ix->scan(
			rm_IndexScanIterator._fileHandle, attr, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator._iterator)) != 0 )
	{
		ix->closeFile(rm_IndexScanIterator._fileHandle);
		return errCode;
	}

	//close file	(error: should not close file handler that is used in scanning the file)
	//if( (errCode = ix->closeFile(ixHandle)) != 0 )
	//{
	//	return errCode;
	//}

	//success
	return errCode;
}

RM_IndexScanIterator::RM_IndexScanIterator()
: _iterator()
{
}

RM_IndexScanIterator::~RM_IndexScanIterator()
{
	_iterator.reset();

}

RC RM_IndexScanIterator::getNextEntry(RID & rid, void* key)
{
	return _iterator.getNextEntry(rid, key);
}

RC RM_IndexScanIterator::close()
{
	return _iterator.close();
}

