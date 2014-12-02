
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <stack>
#include <map>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"

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
	RBFM_ScanIterator& getIterator();
private:
	RBFM_ScanIterator _iterator;
};

class RM_IndexScanIterator {
public:
	RM_IndexScanIterator();  	// Constructor
	~RM_IndexScanIterator(); 	// Destructor

	// "key" follows the same format as in IndexManager::insertEntry()
	RC getNextEntry(RID &rid, void *key);  	// Get next matching entry
	RC close();             			// Terminate index scan
	IX_ScanIterator _iterator;
};

struct ColumnInfo;
struct TableInfo;
struct IndexInfo;

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
  void insertElementsFromTableIntoMap(FileHandle tableHandle, const std::vector<Attribute>& tableDesc);

  //insert element, iterated by <insertElementsFromTableIntoMap>, into _catalogTable
  void processTableRecordAndInsertIntoMap(const void* data, const RID& rid);

  void processIndexRecordAndInsertIntoMap(const void* buffer, const RID& rid);

  //insert element, iterated by <insertElementsFromTableIntoMap>, into _catalogColumn
  void processColumnRecordAndInsertIntoMap(const void* data, const RID& rid);

  //create catalog tables
  void createCatalog();

  //create a record inside table of Tables
  RC createRecordInTables(FileHandle& tableHandle, const std::vector<Attribute>& desc, const char* tableName, int tableId, RID& rid);

  //create record inside table of Columns
  RC createRecordInColumns(FileHandle& columnHandle, const std::vector<Attribute>& desc, unsigned int tableId, const char * columnName,
		  AttrType columnType, unsigned int columnLength, bool isDropped, RID& rid);

  RC createRecordInIndexes(FileHandle& columnHandle, const std::vector<Attribute>& desc, unsigned int tableId,
		  const char* colName, const char* fileName, RID& rid);

  //check whether table with the given name exists
  bool isTableExisiting(const std::string& tableName);

  //debugging function for printing information
  RC printTable(const std::string& tableName);

  RC createIndex(const string &tableName, const string &attributeName);

  RC destroyIndex(const string &tableName, const string &attributeName);

  // indexScan returns an iterator to allow the caller to go through qualified entries in index
  RC indexScan(const string &tableName,
		  const string &attributeName,
		  const void *lowKey,
		  const void *highKey,
		  bool lowKeyInclusive,
		  bool highKeyInclusive,
		  RM_IndexScanIterator &rm_IndexScanIterator
		 );

// Extra credit
public:
  RC dropAttribute(const string &tableName, const string &attributeName);

  RC addAttribute(const string &tableName, const Attribute &attr);

  RC reorganizeTable(const string &tableName);


protected:
  RC printIndexFile(const string& fileName, const Attribute& attrOfKey);
  //compose index name that is used to access index files
  std::string composeIndexName(const std::string& tableName, const std::string& columnName);
  //get list of Attributes for any of 3 catalog tables, i.e. Tables, Columns, or Indexes
  void getCatalogDescription(const string& tableName, std::vector<Attribute>& result);
  //check whether the given name is identical to any catalog table names (i.e. Tables, Columns, or Indexes)
  bool isCatalogTable(const string& name);
  // given record (content of which is stored inside "data") and record description in terms of its attributes (stored in "desc")
  // determine an offset (byte-offset) from the start of the record to the start of the specified attribute (specified by index
  // "attrIndex")
  unsigned int getOffset(const void* data, const std::vector<Attribute> desc, int attrIndex);
  // remove entry from the IX index files
  RC deleteIXEntry(const string& tableName, const std::vector<Attribute> attrs, const void* data, const RID& rid);
  // insert entry into IX index files
  RC insertIXEntry(const string& tableName, const std::vector<Attribute> attrs, const void* data, const RID& rid);
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;

  RecordBasedFileManager* _rbfm;

  //hash map for quick lookup of table inside the catalog's Table of tables
  std::map<string, TableInfo> _catalogTable;	//<table name, tableId>	(subject to further modification)

  //hash map for quick lookup of column inside the catalog's Table of columns
  std::map< int, std::vector< ColumnInfo > > _catalogColumn;	//<table id, <list of column info> >

  //hash map for quick lookup of index inside the catalog's Table of indexes
  std::map< int, std::map< std::string, IndexInfo > > _catalogIndex;	//<table id, <column name, index file name>>

  //list of table IDs that are now unoccupied, and were used by the deleted tables
  std::stack<unsigned int> _freeTableIds;

  //counter for keeping track of the largest table id assigned
  unsigned int _nextTableId;
};


//after this line, our constants, structures, and data types were added

struct IndexInfo
{
	//there are 3 files associates with index for each attribute: *_meta, *_over, and *_prim
	//instead of storing each such name, _indexName would store common part among these
	//names, i.e. the content that goes instead of '*'
	std::string _indexName;

	//rid for record inside the catalog Indexes table file
	RID _rid;
};

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

	unsigned int _isDropped;	//1 = (true) field (i.e. column) is dropped; 0 = (false) the field is not dropped

	RID _rid;

	ColumnInfo()
	{
		_name = "";
		_type = AttrType(0);
		_length = 0;
		_rid.pageNum = 0;
		_rid.slotNum = 0;
		_isDropped = 0;
	}
	ColumnInfo(const std::string& name, const AttrType& type, unsigned int length, bool isDropped, const RID& rid)
	: _name(name), _type(type), _length(length), _isDropped(isDropped ? 1 : 0)
	{
		_rid.pageNum = rid.pageNum;
		_rid.slotNum = rid.slotNum;
	}
};

//constants used to identify file names for two system tables - table and column
#define CATALOG_TABLE_NAME "Tables"
#define CATALOG_COLUMN_NAME "Columns"
#define CATALOG_INDEX_NAME "Indexes"

//table IDs for the table and column
#define CATALOG_TABLE_ID 1
#define CATALOG_COLUMN_ID 2
#define CATALOG_INDEX_ID 3

#define INDEX_DEFAULT_NUM_PAGES 1

//any name (table, column, field, file, ...) stored inside DB cannot exceed the maximum size of
//record minus 4 bytes for storing its actual size (due to VarChar format)
#define MAX_SIZE_OF_NAME_IN_DB MAX_SIZE_OF_RECORD - sizeof(unsigned int)

#endif
