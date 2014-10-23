
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

RC RelationManager::createRecordInTables(FileHandle tableHandle, std::vector<Attribute> table, const char* tableName, int tableId)	//NOT TESTED
{
	RC errCode = 0;

	//determine lengths of VarChar fields
	unsigned int lenOfTableNameField = strlen(tableName) - 1;	//-1 accounts for removing the null-character from the size of table's name

	//create buffer for storing record
	void* recordData = malloc( sizeof(unsigned int) +							//table_id
							   (sizeof(unsigned int) + lenOfTableNameField) +	//size + table_name
							   (sizeof(unsigned int) + lenOfTableNameField) );	//size + file_name

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

	//variable to store table's record id
	RID tableRid;

	//insert record
	if( (errCode = _rbfm->insertRecord(tableHandle, table, recordData, tableRid)) != 0 )
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

RC RelationManager::createRecordInColumns(
		FileHandle columnHandle, std::vector<Attribute> column, int index, unsigned int tableId, const char * columnName)	//NOT TESTED
{
	RC errCode = 0;

	//determine lengths of VarChar fields
	unsigned int lenOfColumnNameField = strlen(columnName) - 1;	//-1 accounts for removing the null-character from the size of column's name

	//create buffer for storing record
	void* recordData = malloc( sizeof(unsigned int) +							//table_id
							   (sizeof(unsigned int) + lenOfColumnNameField) +	//size + column_name
							   sizeof(unsigned int) +							//column type
							   sizeof(unsigned int));							//column length

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

	//variable to store table's record id
	RID tableRid;

	//insert record
	if( (errCode = _rbfm->insertRecord(columnHandle, column, recordData, tableRid)) != 0 )
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
	if( _rbfm->createFile(CATALOG_TABLE_NAME) != 0 || _rbfm->createFile(CATALOG_COLUMN_NAME) != 0 )
	{
		//abort
		exit(-1);
	}

	//open both files
	FileHandle tableHandle, columnHandle;
	if( _rbfm->openFile(CATALOG_TABLE_NAME, tableHandle) != 0 || _rbfm->openFile(CATALOG_COLUMN_NAME, columnHandle) != 0 )
	{
		//abort
		exit(-1);
	}

	//set both files to be modifiable solely by the DB (and not by users)
	bool success = true;
	tableHandle.setAccess(only_system_can_modify, success);
	columnHandle.setAccess(only_system_can_modify, success);
	if( success == false )
	{
		//abort
		exit(-1);
	}

	//Tables table with the following attributes (table-id, table-name, file-name)
	std::vector<Attribute> table;
	const char* tableDescFields[] = {"table-id", "table-name", "file-name"};

	table.push_back((Attribute){tableDescFields[0], TypeInt, sizeof(unsigned int)});
	table.push_back((Attribute){tableDescFields[1], TypeVarChar, MAX_SIZE_OF_NAME_IN_DB});
	table.push_back((Attribute){tableDescFields[2], TypeVarChar, MAX_SIZE_OF_NAME_IN_DB});

	//compose and insert record describing the TABLE
	createRecordInTables(tableHandle, table, CATALOG_TABLE_NAME, CATALOG_TABLE_ID);

	//compose and insert record describing the COLUMN
	createRecordInTables(tableHandle, table, CATALOG_COLUMN_NAME, CATALOG_COLUMN_ID);

	//for fast lookup -> insert <table name, table id> INTO _catalogTable
	_catalogTable.insert(std::pair<string, int>(CATALOG_TABLE_NAME, CATALOG_TABLE_ID));
	_catalogTable.insert(std::pair<string, int>(CATALOG_COLUMN_NAME, CATALOG_COLUMN_ID));

	//Columns table with the following attributes (table-id, column-name, column-type, column-length)
	std::vector<Attribute> column;
	const char* columnDescFields[] = {"table-id", "column-name", "column-type", "column-length"};
	table.push_back((Attribute){columnDescFields[0], TypeInt, sizeof(unsigned int)});
	table.push_back((Attribute){columnDescFields[1], TypeVarChar, MAX_SIZE_OF_NAME_IN_DB});
	table.push_back((Attribute){columnDescFields[2], TypeInt, sizeof(unsigned int)});
	table.push_back((Attribute){columnDescFields[3], TypeInt, sizeof(unsigned int)});

	//compose and insert COLUMN records for COLUMN table
	createRecordInColumns(columnHandle, table, 0, CATALOG_TABLE_ID, tableDescFields[0]);
	createRecordInColumns(columnHandle, table, 1, CATALOG_TABLE_ID, tableDescFields[1]);
	createRecordInColumns(columnHandle, table, 2, CATALOG_TABLE_ID, tableDescFields[2]);

	//compose list of column information
	std::vector<ColumnInfo> tableInfo;
	tableInfo.push_back((ColumnInfo){tableDescFields[0], TypeInt, sizeof(unsigned int)});
	tableInfo.push_back((ColumnInfo){tableDescFields[1], TypeVarChar, MAX_SIZE_OF_NAME_IN_DB});
	tableInfo.push_back((ColumnInfo){tableDescFields[2], TypeVarChar, MAX_SIZE_OF_NAME_IN_DB});

	//for fast lookup -> insert <table id, list of column info> for TABLE INTO _catalogColumn
	_catatlogColumn.insert(std::pair<int, std::vector<ColumnInfo> >(CATALOG_TABLE_ID, tableInfo));

	//compose and insert COLUMN records for COLUMN table
	createRecordInColumns(columnHandle, column, 0, CATALOG_COLUMN_ID, columnDescFields[0]);
	createRecordInColumns(columnHandle, column, 1, CATALOG_COLUMN_ID, columnDescFields[1]);
	createRecordInColumns(columnHandle, column, 2, CATALOG_COLUMN_ID, columnDescFields[2]);
	createRecordInColumns(columnHandle, column, 3, CATALOG_COLUMN_ID, columnDescFields[3]);

	//compose list of column information
	std::vector<ColumnInfo> columnInfo;
	columnInfo.push_back((ColumnInfo){columnDescFields[0], TypeInt, sizeof(unsigned int)});
	columnInfo.push_back((ColumnInfo){columnDescFields[1], TypeVarChar, MAX_SIZE_OF_NAME_IN_DB});
	columnInfo.push_back((ColumnInfo){columnDescFields[2], TypeInt, sizeof(unsigned int)});
	columnInfo.push_back((ColumnInfo){columnDescFields[3], TypeInt, sizeof(unsigned int)});

	//for fast lookup -> insert <table id, list of column info> for TABLE INTO _catalogColumn
	_catatlogColumn.insert(std::pair<int, std::vector<ColumnInfo> >(CATALOG_COLUMN_ID, columnInfo));
}

RelationManager::RelationManager()	//NOT TESTED
{
	_rbfm = RecordBasedFileManager::instance();

	//check if catalog from prior execution already exists.
	FileHandle catalogOfTables, catalogOfColumns;

	std::string catalogTableFileName = string(CATALOG_TABLE_NAME), catalogColumnFileName = string(CATALOG_COLUMN_NAME);

	if( _rbfm->openFile(catalogTableFileName, catalogOfTables) != 0 || _rbfm->openFile(catalogColumnFileName, catalogOfColumns) != 0 )
	{
		//if some of the system files do not exist, clean up
		cleanup();

		//and compile new catalog files
		createCatalog();
	}
	else
	{
		//TODO: check that content of the catalog is consistent
	}
}

RelationManager::~RelationManager()
{
	//not sure
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	//TODO
	//checking the input arguments: empty tableName, zero-size attr vector => fail(-29:wrong table arguments)
    //make sure that table does not exists, if it does => fail(-30:attempt to re-create a table)
	//create file for the table (From project 2 description: "A table maps to a single file, and a single file contains only one table.")
	//assign table's id
	//	* in case there were deleted tables, then need to use their IDs for the next created table (may need to store vector of free table IDs)
	//	* in case there were NO deleted tables, may need to store _nextTableId inside rm.h, for fast unique id assignment
	//insert record (tuple) into catalog Table's table AND _catalogTable (see rm.h)
	//add attributes (i.e. columns to catalog Table of columns) AND _catatlogColumn (see rm.h)
	return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
	//TODO
	//checking that input argument makes sense -- empty string => fail(-29:wrong table arguments)
	//check if table exists, if it does not => fail(-31:accessing/modifying/deleting table that does not exists)
	//find a record inside the Table of tables for this table
	//	* no record found => fail(-32:Table of tables corrupted)
	//remove file corresponding to this table (using the information from the record from Table of tables, i.e. 3rd field = file-name)
	//remove record (tuple) from both _catatlogTable AND catalog Table's table
	//	* no record inside the _catalogTable => fail(-33:local map catalog is corrupted)
	//remove records (tuples) corresponding to descriptions of columns for this table
	//	* no record inside the table of columns or _catalogColumn => fail(-33:local map catalog is corrupted)
	//if we use the vector of free IDs (for fast assignment of id to newly created table), then insert id into that vector
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	//TODO
	//checking that the input arguments make sense - empty strong and empty vector => fail
	//check if table exists, if not => fail(-31:accessing/modifying/deleting table that does not exists)
	//from _catatlogColumn get a vector of ColumnInfo describing the list of columns and their corresponding types
	//	* if there is no record => fail(-33:local map catalog is corrupted)
	//loop thru the list of columns and insert fields into attrs
    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	//TODO
	//checking that the input arguments make sense - empty table name, or NULL-ed data, or rid with inconsistent data:
	//	=> page number eq 0 => fail (the first page is always reserved for first header page)
	//		=> if also slot number is 0, then the rid points to a deleted record (by the rules of RBFM)
	//	=> slot number eq (unsigned int)-1 => may be fail? => means a TombStone record
	//get description of record (call getAttributes OR simply lookup into _catalogColumns)
	//insert record with use of RBFM class
    return -1;
}

RC RelationManager::deleteTuples(const string &tableName)
{
	//TODO
	//checking that table name is not empty string
	//simply delete all records with use of RBFM
    return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	//TODO
	//checking (table name, rid != <0,0> AND rid.slot != (unsigned)-1)
	//simply delete record with use of RBFM (record description from getAttributes)
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	//TODO
	//checking (table name.size != 0, data != NULL, rid != <0,0> AND rid.slot != (unsigned)-1)
	//simply update record with use of RBFM (record description from getAttributes)
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	//TODO
	//checking (table name.size != 0, rid != <0,0> AND rid.slot != (unsigned)-1, data != NULL)
	//simple read record with use of RBFM (record description from getAttributes)
    return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	//TODO
	//checking (table name.size != 0, rid != <0,0> AND rid.slot != (unsigned)-1, data != NULL)
	//simple read attribute with use of RBFM (record description from getAttributes)
    return -1;
}

RC RelationManager::reorganizePage(const string &tableName, const unsigned pageNumber)
{
	//TODO
	//checking (table name.size != 0, rid != <0,0> AND rid.slot != (unsigned)-1, data != NULL)
	//simple reorganize page with use of RBFM (record description from getAttributes)
    return -1;
}

RM_ScanIterator::RM_ScanIterator()
: _iterator()
{
	//nothing
}

RM_ScanIterator::~RM_ScanIterator()
{
	//nothing
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
	//TODO
	//simply call getNextRecord of the _iterator AND handle error case(s)
	return -1;
}

RC RM_ScanIterator::close()
{
	//TODO
	//simply call close of the _iterator AND handle error case(s)
	return -1;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
	//TODO
	//checking (...)
	//if i am correct, then it is sufficient to simply call scan
    return -1;
}

// Extra credit (for later)
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}

// Extra credit
RC RelationManager::reorganizeTable(const string &tableName)
{
    return -1;
}
