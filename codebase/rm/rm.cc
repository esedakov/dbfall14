
#include "rm.h"

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
	//TODO
	//from project description:
	//	It is mandatory to store the catalog information by using the RBF layer functions.
	//	In other words, you should manually create the catalog's tables and populate them
	//	the first time your database is initialized. Once the catalog's tables (such as the
	//	Tables table with the following attributes (table-id, table-name, file-name) and
	//	the Columns table with the following attributes (table-id, column-name,
	//	column-type, column-length)) have been created, they should be persisted to disk.

	//check if catalog from prior execution already exists.
	//if files for table and column did NOT existed priorly
	//	* insert record into file for tables describing the "Tables table": {table-id:Integer, table-name:VarChar, file-name:VarChar}
	//	* insert record into file for tables describing the "Columns table": {table-id:Integer, column-name:VarChar, column-type:???, column-length:Integer}
	//			=> Not sure what type to assign to field "column-type" - VarChar or Integer? What is your opinion???
	// initialize _catalogTable and insert 2 records corresponding to these two tables - Table of tables, Table of columns
	//-------------------
	//	* insert records into file for columns describing each of the columns of two aforementioned tables (e.g. table-id, column-type, ...)
	// initialize _catatlogColumn and insert 7 records corresponding to 7 different columns from the 2 aforementioned tables

	//else (these files existed)
	//	* May be it is good idea to make sure that each such record stores correct information (optional???)
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
