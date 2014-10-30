#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <map>

#include "../rbf/pfm.h"

using namespace std;


// Record ID
typedef struct
{
  unsigned pageNum;
  unsigned slotNum;
} RID;

bool operator<(const RID& x, const RID& y);
bool operator==(const RID& x, const RID& y);

// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { EQ_OP = 0,  // =
           LT_OP,      // <
           GT_OP,      // >
           LE_OP,      // <=
           GE_OP,      // >=
           NE_OP,      // !=
           NO_OP       // no condition
} CompOp;



/****************************************************************************
The scan iterator is NOT required to be implemented for part 1 of the project 
*****************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();


class RBFM_ScanIterator {
public:
	RBFM_ScanIterator();
	~RBFM_ScanIterator();

	// "data" follows the same format as RecordBasedFileManager::insertRecord()
	RC getNextRecord(RID &rid, void *data);
	/* returns rid:
	 * 		if record is a TombStone, then an actual rid is returned
	 * 		if record is deleted, then rid of {0,0} (i.e. page number = 0 and slot number = 0) is returned (that is a first header page, so such rid is incorrect)
	 * 		otherwise (regular record), same record id is returned, as was returned by the prior execution of getNextRecord.\
	**/
	RID getActualRecordId();
	RC close();
public:
	PageNum	_pagenum;
	unsigned int	_slotnum;
	FileHandle _fileHandle;
	vector<Attribute> _recordDescriptor;
	vector<string> _attributes;
	string _conditionAttribute;
	CompOp _compO;					// comparision type such as "<" and "="
	const void* _value;					// used in the comparison
	//vector<string> _attributeNames;
};

//new structure - PageDirSlot. beginning.
/*
 * page directory slot that stores offset to the record and its size
**/
struct PageDirSlot
{
	/*
	 * offset (in bytes) to the start of record from the start of the data page
	**/
	unsigned int _offRecord;
	/*
	 * size of record (in bytes)
	**/
	unsigned int _szRecord;
};
//new structure - PageDirSlot. end.

class RecordBasedFileManager
{
public:
  static RecordBasedFileManager* instance();

  RC createFile(const string &fileName);
  
  RC destroyFile(const string &fileName);
  
  RC openFile(const string &fileName, FileHandle &fileHandle);
  
  RC closeFile(FileHandle &fileHandle);

  //  Format of the data passed into the function is the following:
  //  1) data is a concatenation of values of the attributes
  //  2) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  //  !!!The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute()
  RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

  //read record and decoded it
  RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);
  
  //read record but do not decode it
  RC readEncodedRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);

  /*
   * get page (if necessary insert new) that has enough free space for specified size of record
  **/
  RC getDataPage(FileHandle &fileHandle, const unsigned int recordSize, PageNum& foundPage, PageNum& headerPage, unsigned int& freeSpaceLeftInPage);

  /*
   * find place for the record in the given page
  **/
  RC findRecordSlot(FileHandle &fileHandle, PageNum pagenum, const unsigned int szRecord, PageDirSlot& slot, unsigned int& slotNum, unsigned int freeSpaceLeftInPage);

  // This method will be mainly used for debugging/testing
  RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

  //get header page that contains the given data page id inside it
  RC findHeaderPage(FileHandle fileHandle, PageNum pageId, PageNum& retHeaderPage);

  //convert record's data (stored by originalRecordData) into new format that allows O(1) field access, which is accomplished through the use of
  //"record directory of offsets" that stores list of offsets of each field (from the start of the first record to the end of the last record)
  //NOTE: all of these offsets are measured from the start of the record
  void encodeRecord(
		  const vector<Attribute> &recordDescriptor,
		  const void* originalRecordData,
		  const unsigned int origSize,
		  unsigned int& newSzOfRecord,
		  void* newRecordData);

  //convert encoded record format into original, i.e. by removing "record directory of offsets" and restoring length parameters for the VARCHAR cases
  void decodeRecord(
		  const vector<Attribute>& recordDescriptor,
		  const void* encodedRecordData,
		  unsigned int& decodedSize,
		  void* decodedRecordData);

/**************************************************************************************************************************************************************
***************************************************************************************************************************************************************
IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for part 1 of the project
***************************************************************************************************************************************************************
***************************************************************************************************************************************************************/
  RC deleteRecords(FileHandle &fileHandle);

  RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

  // Assume the rid does not change after update
  RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

  RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data);

  RC reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber);

  // scan returns an iterator to allow the caller to go through the results one by one. 
  RC scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator);


// Extra credit for part 2 of the project, please ignore for part 1 of the project
public:

  RC reorganizeFile(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor);


protected:
  RecordBasedFileManager();
  ~RecordBasedFileManager();

private:
  static RecordBasedFileManager *_rbf_manager;
  static PagedFileManager *_pfm;
};

//after this line there are newly added structures, function, and constants

//size of TombStone (for part 2 of the project)
#define TOMBSTONE_SIZE (sizeof(RID))	//page number, slot number

/*
 * threshold size of the record, i.e. the largest size that can fit within single page and allow some space for meta-data and slot directory be left
**/
#define MAX_SIZE_OF_RECORD (PAGE_SIZE - sizeof(unsigned int) - sizeof(unsigned int) - sizeof(PageDirSlot))

/*
 * prototype for the stand-alone function for determining size of the record (in bytes)
**/
unsigned int sizeOfRecord(const vector<Attribute> &recordDescriptor, const void *data);

#endif
