
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <stack>
#include <map>

#include "../rbf/rbfm.h"

using namespace std;


# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
// The way to use it is like the following:
//  RM_ScanIterator rmScanIterator;
//  rm.open(..., rmScanIterator);
//  while (rmScanIterator(rid, data) != RM_EOF) {
//    process the data;
//  }
//  rmScanIterator.close();

class RM_ScanIterator
{
public:
	RM_ScanIterator();
	~RM_ScanIterator();

	// "data" follows the same format as RelationManager::insertTuple()
	RC getNextTuple(RID &rid, void *data);
	RC close();
	RBFM_ScanIterator getIterator();
private:
	RBFM_ScanIterator _iterator;
};

struct ColumnInfo;
struct TableInfo;

// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuples(const string &tableName);

  RC deleteTuple(const string &tableName, const RID &rid);

  // Assume the rid does not change after update
  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  RC reorganizePage(const string &tableName, const unsigned pageNumber);

  // scan returns an iterator to allow the caller to go through the results one by one. 
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

  //delete catalog
  void cleanup();

  //insert elements from the system catalog table into local maps (e.g. _catalogTable and _columnTable) , which store useful information about these tables
  void insertElementsFromTableIntoMap(std::string tableName, std::vector<Attribute> tableDesc);

  //insert element, iterated by <insertElementsFromTableIntoMap>, into _catalogTable
  void processTableRecordAndInsertIntoMap(char* data, const RID& rid);

  //insert element, iterated by <insertElementsFromTableIntoMap>, into _catalogColumn
  void processColumnRecordAndInsertIntoMap(char* data);

  //create catalog tables
  void createCatalog();

  //create a record inside table of Tables
  RC createRecordInTables(FileHandle tableHandle, std::vector<Attribute> table, const char* tableFields, int tableId, RID& rid);

  //create record inside table of Columns
  RC createRecordInColumns(FileHandle columnHandle, std::vector<Attribute> column, int index, unsigned int tableId, const char * columnName);

  //check whether table with the given name exists
  bool isTableExisiting(std::string tableName);

// Extra credit
public:
  RC dropAttribute(const string &tableName, const string &attributeName);

  RC addAttribute(const string &tableName, const Attribute &attr);

  RC reorganizeTable(const string &tableName);



protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;

  RecordBasedFileManager* _rbfm;

  //hash map for quick lookup of table inside the catalog's Table of tables
  std::map<string, TableInfo> _catalogTable;	//<table name, tableId>	(subject to further modification)

  //hash map for quick lookup of column inside the catalog's Table of columns
  std::map< int, std::vector< ColumnInfo > > _catalogColumn;	//<table id, <list of column info> >

  //list of table IDs that are now unoccupied, and were used by the deleted tables
  std::stack<unsigned int> _freeTableIds;

  //counter for keeping track of the largest table id assigned
  unsigned int _nextTableId;
};


//after this line, our constants, structures, and data types were added

struct TableInfo
{
	//table id
	unsigned int _id;

	//table name
	std::string _name;

	//rid
	RID _rid;
};

struct ColumnInfo
{
	//column name
	std::string _name;

	//unique identifier for column
	//unsigned int _columnId;

	AttrType _type;	//create a enum and within it a unique identifier for each type

	unsigned int _length;
};

//constants used to identify file names for two system tables - table and column
#define CATALOG_TABLE_NAME "table"
#define CATALOG_COLUMN_NAME "column"

//table IDs for the table and column
#define CATALOG_TABLE_ID 1
#define CATALOG_COLUMN_ID 2

//any name (table, column, field, file, ...) stored inside DB cannot exceed the maximum size of
//record minus 4 bytes for storing its actual size (due to VarChar format)
#define MAX_SIZE_OF_NAME_IN_DB MAX_SIZE_OF_RECORD - sizeof(unsigned int)

#endif
