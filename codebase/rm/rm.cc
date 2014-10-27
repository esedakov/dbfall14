#include "rm.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

void RelationManager::cleanup()
{
	remove(CATALOG_TABLE_NAME);
	remove(CATALOG_COLUMN_NAME);
}

RC RelationManager::createRecordInTables(FileHandle& tableHandle,
		const std::vector<Attribute>& table, const char* tableName, int tableId, RID& rid)	//NOT TESTED
{
	RC errCode = 0;

	//determine lengths of VarChar fields
	unsigned int lenOfTableNameField = strlen(tableName) - 1;	//-1 accounts for removing the null-character from the size of table's name

	//create buffer for storing record
	void* recordData = malloc(sizeof(unsigned int) + //table_id
				(sizeof(unsigned int) + lenOfTableNameField) + //size + table_name
				(sizeof(unsigned int) + lenOfTableNameField)); //size + file_name

	//copy fields into record's buffer
	char* p = (char*) recordData;
	//table id
	((unsigned int*)p)[0] = tableId;
	p += sizeof(unsigned int);
	//table name's length
	((unsigned int*)p)[0] = lenOfTableNameField;
	p += sizeof(unsigned int);
	//table name
	memcpy(p, tableName, lenOfTableNameField);
	p += lenOfTableNameField;
	//file name's length
	((unsigned int*)p)[0] = lenOfTableNameField;
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
		const std::vector<Attribute>& column, int index, unsigned int tableId, const char * columnName, RID& rid) //NOT TESTED
{
	RC errCode = 0;

	//determine lengths of VarChar fields
	unsigned int lenOfColumnNameField = strlen(columnName) - 1;	//-1 accounts for removing the null-character from the size of column's name

	//create buffer for storing record
	void* recordData = malloc(sizeof(unsigned int) + //table_id
			(sizeof(unsigned int) + lenOfColumnNameField) + //size + column_name
			sizeof(unsigned int) + //column type
			sizeof(unsigned int)); //column length

	//copy fields into record's buffer
	char* p = (char*) recordData;
	//table id
	((unsigned int*)p)[0] = tableId;
	p += sizeof(unsigned int);
	//column name's length
	((unsigned int*)p)[0] = lenOfColumnNameField;
	p += sizeof(unsigned int);
	//column name
	memcpy(p, columnName, lenOfColumnNameField);
	p += lenOfColumnNameField;
	//column type
	((unsigned int*)p)[0] = column[index].type;
	p += sizeof(unsigned int);
	//column length
	((unsigned int*)p)[0] = column[index].length;

	//insert record
	if ((errCode = _rbfm->insertRecord(columnHandle, column, recordData, rid))
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

void RelationManager::createCatalog()	//NOT TESTED
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
			|| _rbfm->createFile(CATALOG_COLUMN_NAME) != 0) {
		//abort
		exit(-1);
	}

	//open both files
	FileHandle tableHandle, columnHandle;
	if (_rbfm->openFile(CATALOG_TABLE_NAME, tableHandle) != 0
			|| _rbfm->openFile(CATALOG_COLUMN_NAME, columnHandle) != 0) {
		//abort
		exit(-1);
	}

	//set both files to be modifiable solely by the DB (and not by users)
	bool success = true;
	tableHandle.setAccess(only_system_can_modify, success);
	columnHandle.setAccess(only_system_can_modify, success);
	if (success == false) {
		//abort
		exit(-1);
	}

	//1. insert record about table Tables into Tables => {TABLE_ID=1, TABLE_NAME="Tables", FILE_NAME="Tables"}
	//Tables table with the following attributes (table-id, table-name, file-name)
	std::vector<Attribute> table;
	const char* tableDescFields[] = {"table-id", "table-name", "file-name"};

	table.push_back(
			(Attribute) {tableDescFields[0], AttrType(0), sizeof(unsigned int)});
	table.push_back(
			(Attribute) {tableDescFields[1], AttrType(2), MAX_SIZE_OF_NAME_IN_DB});
	table.push_back(
			(Attribute) {tableDescFields[2], AttrType(2), MAX_SIZE_OF_NAME_IN_DB});

	RID ridTable, ridColumn;

	//compose and insert record describing the TABLE
	if( createRecordInTables(tableHandle, table, CATALOG_TABLE_NAME,
			CATALOG_TABLE_ID, ridTable) != 0 )
	{
		//abort
		cleanup();
		exit(-1);
	}

	//2. insert record about table Columns into Tables => {TABLE_ID=2, TABLE_NAME="Columns", FILE_NAME="Columns"}
	//compose and insert record describing the COLUMN
	if( createRecordInTables(tableHandle, table, CATALOG_COLUMN_NAME,
			CATALOG_COLUMN_ID, ridColumn) != 0 )
	{
		//abort
		cleanup();
		exit(-1);
	}

	//3. Insert information about Tables and Columns into Table's map => {TABLE_ID, TABLE_NAME, RID}
	//for fast lookup -> insert <table name, table info> INTO _catalogTable
	_catalogTable.insert(std::pair<string, TableInfo>(CATALOG_TABLE_NAME, (TableInfo){CATALOG_TABLE_ID, CATALOG_TABLE_NAME, ridTable}));
	_catalogTable.insert(std::pair<string, TableInfo>(CATALOG_COLUMN_NAME, (TableInfo){CATALOG_COLUMN_ID, CATALOG_COLUMN_NAME, ridColumn}));

	//4. Insert record about table Tables into Columns =>
	//		{TABLE_ID=CATALOG_TABLE_ID, COLUMN_NAME=tableDescFields[#], COLUMN_TYPE=table[#].type, COLUMN_LENGTH=table[#].length}
	//compose and insert COLUMN records for TABLES table
	RID list_of_tableColumn_rids[3];

	if( createRecordInColumns(columnHandle, table, 0, CATALOG_TABLE_ID, tableDescFields[0], list_of_tableColumn_rids[0]) != 0 ||
		createRecordInColumns(columnHandle, table, 1, CATALOG_TABLE_ID, tableDescFields[1], list_of_tableColumn_rids[1]) != 0 ||
		createRecordInColumns(columnHandle, table, 2, CATALOG_TABLE_ID, tableDescFields[2], list_of_tableColumn_rids[2]) != 0 )
	{
		//abort
		cleanup();
		exit(-1);
	}

	//5. insert information about Table into Column's map => {COLUMN_NAME, TYPE_OF_FIELD, SIZE_OF_FIELD}
	//compose list of column information
	std::vector<ColumnInfo> tableInfo;
	tableInfo.push_back((ColumnInfo){tableDescFields[0], TypeInt, sizeof(unsigned int), list_of_tableColumn_rids[0]});
	tableInfo.push_back((ColumnInfo){tableDescFields[1], TypeVarChar, MAX_SIZE_OF_NAME_IN_DB, list_of_tableColumn_rids[1]});
	tableInfo.push_back((ColumnInfo){tableDescFields[2], TypeVarChar, MAX_SIZE_OF_NAME_IN_DB, list_of_tableColumn_rids[2]});

	//for fast lookup -> insert <table id, list of column info> for TABLE INTO _catalogColumn
	_catalogColumn.insert(std::pair<int, std::vector<ColumnInfo> >(CATALOG_TABLE_ID, tableInfo));

	//6. insert record about table Columns into Columns =>
	//		{TABLE_ID=CATALOG_COLUMN_ID, COLUMN_NAME=columnDescFields[#], COLUMN_TYPE=column[#].type, COLUMN_LENGTH=column[#].length}
	//Columns table with the following attributes (table-id, column-name, column-type, column-length)
	std::vector<Attribute> column;
	const char* columnDescFields[] = { "table-id", "column-name", "column-type",
			"column-length" };
	column.push_back(
			(Attribute) {columnDescFields[0], AttrType(0), sizeof(unsigned int)});
	column.push_back(
			(Attribute) {columnDescFields[1], AttrType(2), MAX_SIZE_OF_NAME_IN_DB});
	column.push_back(
			(Attribute) {columnDescFields[2], AttrType(0), sizeof(unsigned int)});
	column.push_back(
			(Attribute) {columnDescFields[3], AttrType(0), sizeof(unsigned int)});

	RID list_of_columnColumn_rids[4];

	//compose and insert COLUMN records for COLUMNS table
	if( createRecordInColumns(columnHandle, column, 0, CATALOG_COLUMN_ID, columnDescFields[0], list_of_columnColumn_rids[0]) != 0 ||
		createRecordInColumns(columnHandle, column, 1, CATALOG_COLUMN_ID, columnDescFields[1], list_of_columnColumn_rids[1]) != 0 ||
		createRecordInColumns(columnHandle, column, 2, CATALOG_COLUMN_ID, columnDescFields[2], list_of_columnColumn_rids[2]) != 0 ||
		createRecordInColumns(columnHandle, column, 3, CATALOG_COLUMN_ID, columnDescFields[3], list_of_columnColumn_rids[3]) != 0 )
	{
		//abort
		cleanup();
		exit(-1);
	}

	//7. insert information about Columns into Column's map
	//compose list of column information
	std::vector<ColumnInfo> columnInfo;
	columnInfo.push_back(
			(ColumnInfo) {columnDescFields[0], AttrType(0), sizeof(unsigned int), list_of_columnColumn_rids[0]});
	columnInfo.push_back(
			(ColumnInfo) {columnDescFields[1], AttrType(2), MAX_SIZE_OF_NAME_IN_DB, list_of_columnColumn_rids[1]});
	columnInfo.push_back(
			(ColumnInfo) {columnDescFields[2], AttrType(0), sizeof(unsigned int), list_of_columnColumn_rids[2]});
	columnInfo.push_back(
			(ColumnInfo) {columnDescFields[3], AttrType(0), sizeof(unsigned int), list_of_columnColumn_rids[3]});

	//for fast lookup -> insert <table id, list of column info> for TABLE INTO _catalogColumn
	_catalogColumn.insert(
			std::pair<int, std::vector<ColumnInfo> >(CATALOG_COLUMN_ID,
					columnInfo));

	//close both catalog files
	if( _rbfm->closeFile(tableHandle) != 0 || _rbfm->closeFile(columnHandle) != 0 )
	{
		//abort
		cleanup();
		exit(-1);
	}
}

void RelationManager::insertElementsFromTableIntoMap(const std::string& tableName, const std::vector<Attribute>& tableDesc)
{
	//quick way of determining whether the given system table a table of Columns or Tables
	bool isColumnsTable = (tableName == CATALOG_COLUMN_NAME);

	//make sure that the given table name refers to one of the system tables
	if( tableName != CATALOG_TABLE_NAME && !isColumnsTable )
	{
		//destroy system files
		cleanup();

		//fail
		exit(-1);
	}

	//setup iterator to loop thru table Tables to insert each found record into the local copy
	RM_ScanIterator iterator;

	//allocate buffer for storing iterated elements, i.e. table and column records
	void* data = malloc(PAGE_SIZE);
	memset(data, 0, PAGE_SIZE);

	//setup list of attributes for scanning
	std::vector<string> tableAttr;

	//loop through elements of attributes to create list of strings
	std::vector<Attribute>::const_iterator i = tableDesc.begin();

	//setup tableAttr
	for( ; i != tableDesc.end(); i++ )
	{
		tableAttr.push_back( i->name );
	}

	//set the iterator to the beginning of the given table
	if( scan(tableName, "", NO_OP, NULL, tableAttr, iterator) != 0 )
	{
		//delete system files
		cleanup();

		//abort
		exit(-1);
	}

	//record identifier for table records
	RID rid;

	//loop through Table's records and insert them into _catalogTable
	while( iterator.getNextTuple(rid, data) != RM_EOF )
	{
		//insert iterated element into the proper map
		if( isColumnsTable )
			processColumnRecordAndInsertIntoMap((char*) data);
		else
			processTableRecordAndInsertIntoMap((char*) data, rid);
	}

}

void RelationManager::processTableRecordAndInsertIntoMap(const void* buffer, const RID& rid)
{
	//cast buffer to char-pointer
	char* data = (char*)buffer;

	//get integer that represents table-id
	unsigned int table_id = *((unsigned int*)data);

	//update data pointer
	data += sizeof(unsigned int);

	//get table-name
		//get integer that describes the size of the char-array
		unsigned int sz_of_name = *((unsigned int*)data);

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
		sz_of_name = *((unsigned int*)data);

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
	_catalogTable.insert(std::pair<string, TableInfo>( str_table_name, (TableInfo){ table_id, str_table_name, rid } ));
}

void RelationManager::processColumnRecordAndInsertIntoMap(const void* buffer)
{
	//cast buffer to char-array
	char* data = (char*)buffer;

	//get integer that represents table-id
	unsigned int table_id = *((unsigned int*)data);

	//update data pointer
	data += sizeof(unsigned int);

	//get column-name
		//get integer that describes the size of the char-array
		unsigned int sz_of_name = *((unsigned int*)data);

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
	unsigned int column_type = *((unsigned int*)data);

	//update data pointer
	data += sizeof(unsigned int);

	//get integer that represents column-length
	unsigned int column_length = *((unsigned int*)data);

	//iterator that points at the key-value pair of interest
	std::map< int, std::vector< ColumnInfo > >::iterator iterator = _catalogColumn.find(table_id);

	//make sure that the pair with the given table-id exists inside the map
	if( iterator == _catalogColumn.end() )
	{
		iterator = _catalogColumn.insert( std::pair<int, std::vector<ColumnInfo> >() ).first;
	}

	//insert record elements inside _catalogTable
	(*iterator).second.push_back( (ColumnInfo) { str_column_name, (AttrType)column_type, column_length } );
}

RelationManager::RelationManager()	//NOT TESTED
{
	_rbfm = RecordBasedFileManager::instance();

	//check if catalog from prior execution already exists.
	FileHandle catalogOfTables, catalogOfColumns;

	std::string catalogTableFileName = string(CATALOG_TABLE_NAME),
			catalogColumnFileName = string(CATALOG_COLUMN_NAME);


	if( _rbfm->openFile(catalogTableFileName, catalogOfTables) != 0
			|| _rbfm->openFile(catalogColumnFileName, catalogOfColumns) != 0 ) {
		//if some of the system files do not exist, clean up
		cleanup();

		//and compile new catalog files
		createCatalog();
	} else	//the DB was in existence beforehand
	{


		//TODO:
		//loop thru table Tables (that stores records for each table in the DB) and insert them into _catalogTable
		//	check whether system tables are in presence (i.e. Tables and Columns), if not abort
		//loop thru table Columns (that inserts types of columns in the DB) and insert them into _catalogColumn

		//insert elements from the table Tables into _catalogTable
			//create list of attributes for table Tables
			std::vector<Attribute> table;
			const char* tableDescFields[] = {"table-id", "table-name", "file-name"};

			table.push_back(
					(Attribute) {tableDescFields[0], AttrType(0), sizeof(unsigned int)});
			table.push_back(
					(Attribute) {tableDescFields[1], AttrType(2), MAX_SIZE_OF_NAME_IN_DB});
			table.push_back(
					(Attribute) {tableDescFields[2], AttrType(2), MAX_SIZE_OF_NAME_IN_DB});

		insertElementsFromTableIntoMap(CATALOG_TABLE_NAME, table);

		//insert elements from the table Columns into _catalogColumn
			//create list of attributes for table Columns
			std::vector<Attribute> column;
			const char* columnDescFields[] = { "table-id", "column-name", "column-type",
					"column-length" };
			column.push_back(
					(Attribute) {columnDescFields[0], AttrType(0), sizeof(unsigned int)});
			column.push_back(
					(Attribute) {columnDescFields[1], AttrType(2), MAX_SIZE_OF_NAME_IN_DB});
			column.push_back(
					(Attribute) {columnDescFields[2], AttrType(0), sizeof(unsigned int)});
			column.push_back(
					(Attribute) {columnDescFields[3], AttrType(0), sizeof(unsigned int)});

		insertElementsFromTableIntoMap(CATALOG_COLUMN_NAME, column);
	}

	//setup the other class data-member(s)
	_nextTableId = (CATALOG_COLUMN_ID > CATALOG_TABLE_ID ? CATALOG_COLUMN_ID : CATALOG_TABLE_ID) + 1;
}

RelationManager::~RelationManager()
{
	//not sure
}

//error codes for project 2, component RM:
//	-29 = wrong table arguments
//	-30 = attempt to re-create existing table
//	-31 = accessing/modifying/deleting table that does not exists
//	-32 = Table of tables corrupted

bool RelationManager::isTableExisiting(const std::string& tableName)
{
	return _catalogTable.find(tableName) != _catalogTable.end();
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	RC errCode = 0;

	//checking the input arguments
	if( tableName.length() == 0 || attrs.size() == 0 )
	{
		return -29;	//wrong table arguments
	}

	//make sure that the table does not exist, otherwise fail
	if( isTableExisiting(tableName) )
	{
		return -30;	//attempt to re-create existing table
	}

	//create file for the table
	if( (errCode = _rbfm->createFile(tableName)) != 0 )
	{
		//return error code
		return errCode;
	}

	//open catalog table's TABLE
	FileHandle tableHandle;

	if( (errCode = _rbfm->openFile(CATALOG_TABLE_NAME, tableHandle)) != 0 )
	{
		//return error code
		return errCode;
	}

	unsigned int id = 0;

	//if there are unoccupied table IDs, then
	if( _freeTableIds.size() > 0 )
	{
		//assign one from the top of the stack
		id = _freeTableIds.top();

		//remove it from the stack
		_freeTableIds.pop();
	}

	//if there were no unoccupied table IDs, then
	if( id == 0 )
	{
		//assign using the next counter
		id = _nextTableId++;
	}

	//insert record (tuple) into catalog Table's table

	//maintain record identifier for the element inserted into table Tables
	RID tableRid;

	//insert new record into table Tables
	if( (errCode = createRecordInTables(tableHandle, attrs, tableName.c_str(), id, tableRid)) != 0 )
	{
		//return error code
		return errCode;
	}

	//insert an entry into _catalogTable
	_catalogTable.insert(std::pair<string, TableInfo>(tableName, (TableInfo){id, tableName, tableRid} ));

	//close table handle
	if( (errCode = _rbfm->closeFile(tableHandle)) != 0 )
	{
		//return error code
		return errCode;
	}

	//open catalog table Columns
	FileHandle columnHandle;

	if( (errCode = _rbfm->openFile(CATALOG_COLUMN_NAME, columnHandle)) != 0 )
	{
		//return error code
		return errCode;
	}

	std::vector<ColumnInfo> tableInfo;
	std::vector<Attribute>::const_iterator i = attrs.begin(), max = attrs.end();
	for( ; i != max; i++ )
	{
		RID columnRid;

		//create record in table Columns
		if( (errCode = createRecordInColumns( columnHandle, attrs, 0, id, (*i).name.c_str(), columnRid )) != 0 )
		{
			//close the column handle
			_rbfm->closeFile(columnHandle);

			//fail
			return errCode;
		}

		//create column information entry
		tableInfo.push_back( (ColumnInfo){ (*i).name, (*i).type, (*i).length, columnRid } );
	}

	//for fast lookup -> insert <table id, list of column info> for TABLE INTO _catalogColumn
	_catalogColumn.insert(std::pair<int, std::vector<ColumnInfo> >(id, tableInfo));

	//success
	return 0;
}

RC RelationManager::deleteTable(const string &tableName) {
	RC errCode = 0;

	//checking that input argument makes sense
	if( tableName.size() == 0 )
	{
		//fail
		return -29;	//wrong table arguments
	}

	//check if table exists
	std::map<string, TableInfo>::iterator tableIter = _catalogTable.find(tableName);
	if( iterator == _catalogTable.end() )
	{
		//fail
		return -31;	//accessing table that does not exists
	}

	//fast access for table information
	TableInfo info = (*tableIter).second;

	//1, remove record inside Tables
	//find and remove record inside the table Tables for this table
	if( (errCode = deleteTuple(CATALOG_TABLE_NAME, info._rid)) != 0 )
	{
		//fail
		return errCode;	//most likely record does not exist
	}

	//2. remove file representing the table
	//remove file corresponding to this table (using the information from the record from Table of tables, i.e. 3rd field = file-name)
	if( (errCode = _rbfm->destroyFile(info._name)) != 0 )
	{
		//fail
		return errCode;	//file does not exist
	}

	//get list of column information
	std::map<int, std::vector<ColumnInfo> >::iterator columnIter = _catalogColumn.find((int)info._id);

	if( columnIter == _catalogColumn.end() )
	{
		//fail
		return -31;	//accessing column table that does not exist
	}

	std::vector<ColumnInfo> vecColInfo = (*columnIter).second;

	//3. remove records from Columns
	//find and remove records inside the table Columns, representing separate fields of this table
	std::vector<ColumnInfo>::iterator colInfoIter = vecColInfo.begin(), max = vecColInfo.end();
	for( ; colInfoIter != max; colInfoIter++ )
	{
		if( (errCode = deleteTuple(CATALOG_COLUMN_NAME, (*colInfoIter)._rid)) != 0)
		{
			//fail
			return errCode;	//most likely record does not exist
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

RC RelationManager::getAttributes(const string &tableName,
		vector<Attribute> &attrs) //NOT TESTED
{
	//TODO
	//checking that the input arguments make sense - empty string => fail
	//check if table exists, if not => fail(-31:accessing/modifying/deleting table that does not exists)
	//from _catatlogColumn get a vector of ColumnInfo describing the list of columns and their corresponding types
	//	* if there is no record => fail(-33:local map catalog is corrupted)
	//loop thru the list of columns and insert fields into attrs

	RC errCode = 0;
	if (tableName.empty())
		return -1; //tableName no valid

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
	void* recordData = malloc(sizeof(unsigned int) + //table_id
			(sizeof(unsigned int) + MAX_SIZE_OF_NAME_IN_DB) + //size + column_name
			sizeof(unsigned int) + //column type
			sizeof(unsigned int)); //column length
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
		attr.length = column._length;
		attrs.push_back(attr);

	}

	free(recordData);
	_rbfm->closeFile(fileHandle);

	return errCode;
}

RC RelationManager::insertTuple(const string &tableName, const void *data,
		RID &rid) {
	//TODO
	//checking that the input arguments make sense - empty table name, or NULL-ed data, or rid with inconsistent data:
	//	=> page number eq 0 => fail (the first page is always reserved for first header page)
	//		=> if also slot number is 0, then the rid points to a deleted record (by the rules of RBFM)
	//	=> slot number eq (unsigned int)-1 => may be fail? => means a TombStone record
	//get description of record (call getAttributes OR simply lookup into _catalogColumns)
	//insert record with use of RBFM class

	RC errCode = 0;

	//check if there is inconsistent data
	if (tableName.empty() || data == NULL || rid.pageNum == 0
			|| rid.slotNum == 0)
		return -1;

	vector<Attribute> attrs;
	//get the attributes for this table
	getAttributes(tableName, attrs);

	//create the handle and open the table
	FileHandle fileHandle;
	if ((errCode = _rbfm->openFile(tableName, fileHandle)) != 0)
		return errCode;

	//insert the record
	if ((errCode = _rbfm->insertRecord(fileHandle, attrs, data, rid)) != 0)
		return errCode;

	_rbfm->closeFile(fileHandle);

	return errCode;

}

RC RelationManager::deleteTuples(const string &tableName) {
	//TODO
	//checking that table name is not empty string
	//simply delete all records with use of RBFM
	return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {
	//TODO
	//checking (table name, rid != <0,0> AND rid.slot != (unsigned)-1)
	//simply delete record with use of RBFM (record description from getAttributes)

	RC errCode = 0;
	//check if there is inconsistent data
	if (tableName.empty() || rid.pageNum == 0 || rid.slotNum == 0)
		return -1;

	vector<Attribute> attrs;
	//get the attributes for this table
	getAttributes(tableName, attrs);

	//create the handle and open the table
	FileHandle fileHandle;
	if ((errCode = _rbfm->openFile(tableName, fileHandle)) != 0)
		return errCode;

	//delete the record
	if ((errCode = _rbfm->deleteRecord(fileHandle, attrs, rid)) != 0)
		return errCode;

	_rbfm->closeFile(fileHandle);

	return errCode;
}

RC RelationManager::updateTuple(const string &tableName, const void *data,
		const RID &rid) {
	//TODO
	//checking (table name.size != 0, data != NULL, rid != <0,0> AND rid.slot != (unsigned)-1)
	//simply update record with use of RBFM (record description from getAttributes)
	RC errCode = 0;
	//check if there is inconsistent data
	if (tableName.empty() || data == NULL || rid.pageNum == 0
			|| rid.slotNum == 0)
		return -1;

	vector<Attribute> attrs;
	//get the attributes for this table
	getAttributes(tableName, attrs);

	//create the handle and open the table
	FileHandle fileHandle;
	if ((errCode = _rbfm->openFile(tableName, fileHandle)) != 0)
		return errCode;

	//update the record
	if ((errCode = _rbfm->updateRecord(fileHandle, attrs, data, rid)) != 0)
		return errCode;

	_rbfm->closeFile(fileHandle);

	return errCode;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid,
		void *data) {
	//TODO
	//checking (table name.size != 0, rid != <0,0> AND rid.slot != (unsigned)-1, data != NULL)
	//simple read record with use of RBFM (record description from getAttributes)
	RC errCode = 0;
	//check if there is inconsistent data
	if (tableName.empty() || data == NULL || rid.pageNum == 0
			|| rid.slotNum == 0)
		return -1;

	vector<Attribute> attrs;
	//get the attributes for this table
	getAttributes(tableName, attrs);

	//create the handle and open the table
	FileHandle fileHandle;
	if ((errCode = _rbfm->openFile(tableName, fileHandle)) != 0)
		return errCode;

	//read the tuple
	if ((errCode = _rbfm->readRecord(fileHandle, attrs, rid, data)) != 0)
		return errCode;

	_rbfm->closeFile(fileHandle);

	return errCode;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid,
		const string &attributeName, void *data) {
	//TODO
	//checking (table name.size != 0, rid != <0,0> AND rid.slot != (unsigned)-1, data != NULL)
	//simple read attribute with use of RBFM (record description from getAttributes)
	RC errCode = 0;
	//check if there is inconsistent data
	if (tableName.empty() || data == NULL || rid.pageNum == 0
			|| rid.slotNum == 0)
		return -1;

	vector<Attribute> attrs;
	//get the attributes for this table
	getAttributes(tableName, attrs);

	//create the handle and open the table
	FileHandle fileHandle;
	if ((errCode = _rbfm->openFile(tableName, fileHandle)) != 0)
		return errCode;

	//read attribute
	if ((errCode = _rbfm->readAttribute(fileHandle, attrs, rid, attributeName,
			data)) != 0)
		return errCode;

	_rbfm->closeFile(fileHandle);

	return errCode;
}

RC RelationManager::reorganizePage(const string &tableName,
		const unsigned pageNumber) {
	//TODO
	//checking (table name.size != 0)
	//simple reorganize page with use of RBFM (record description from getAttributes)
	RC errCode = 0;
	//check if there is inconsistent data
	if (tableName.empty())
		return -1;

	vector<Attribute> attrs;
	//get the attributes for this table
	getAttributes(tableName, attrs);

	//create the handle and open the table
	FileHandle fileHandle;
	if ((errCode = _rbfm->openFile(tableName, fileHandle)) != 0)
		return errCode;

	//reorganize Page
	if ((errCode = _rbfm->reorganizePage(fileHandle, attrs, pageNumber)) != 0)
		return errCode;

	_rbfm->closeFile(fileHandle);

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
	return _iterator.close();
}

RBFM_ScanIterator RM_ScanIterator::getIterator() {
	return _iterator;
}

RC RelationManager::scan(const string &tableName,
		const string &conditionAttribute, const CompOp compOp,
		const void *value, const vector<string> &attributeNames,
		RM_ScanIterator &rm_ScanIterator) {

	RC errCode = 0;
	//check if there is inconsistent data
	if (tableName.empty())
		return -1;

	//create the handle and open the table
	FileHandle fileHandle;
	if ((errCode = _rbfm->openFile(tableName, fileHandle)) != 0)
		return errCode;

	vector<Attribute> attrs;
	//get the attributes for this table
	getAttributes(tableName, attrs);

	//set up scan RBFM
	//if ((errCode = _rbfm->scan(fileHandle, attrs, conditionAttribute, compOp,value, attributeNames, rm_ScanIterator.getIterator())) != 0) //check this part
		return errCode;

	return errCode;
}

// Extra credit (for later)
RC RelationManager::dropAttribute(const string &tableName,
		const string &attributeName) {
	return -1;
}

// Extra credit
RC RelationManager::addAttribute(const string &tableName,
		const Attribute &attr) {
	return -1;
}

// Extra credit
RC RelationManager::reorganizeTable(const string &tableName) {
	return -1;
}
