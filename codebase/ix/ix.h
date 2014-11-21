#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

typedef unsigned int BUCKET_NUMBER;

class IX_ScanIterator;
class IXFileHandle;

struct indexInfo
{
	unsigned int N;
	unsigned int Level;
	unsigned int Next;
	map<BUCKET_NUMBER, map<int, PageNum> > _overflowPageIds;
	indexInfo()
	: N(0), Level(0), Next(0)
	{};
	indexInfo(unsigned int n, unsigned int level, unsigned int next)
	: N(n), Level(level), Next(next)
	{};
};

class IndexManager {
 public:
  static IndexManager* instance();

  // Create index file(s) to manage an index
  RC createFile(const string &fileName, const unsigned &numberOfPages);

  // Delete index file(s)
  RC destroyFile(const string &fileName);

  // Open an index and returns an IXFileHandle
  RC openFile(const string &fileName, IXFileHandle &ixFileHandle);

  // Close an IXFileHandle. 
  RC closeFile(IXFileHandle &ixfileHandle);

  // The following functions  are using the following format for the passed key value.
  //  1) data is a concatenation of values of the attributes
  //  2) For INT and REAL: use 4 bytes to store the value;
  //     For VarChar: use 4 bytes to store the length of characters, then store the actual characters.

  // Insert an entry to the given index that is indicated by the given IXFileHandle
  RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

  // Delete an entry from the given index that is indicated by the given IXFileHandle
  RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

  // scan() returns an iterator to allow the caller to go through the results
  // one by one in the range(lowKey, highKey).
  // For the format of "lowKey" and "highKey", please see insertEntry()
  // If lowKeyInclusive (or highKeyInclusive) is true, then lowKey (or highKey)
  // should be included in the scan
  // If lowKey is null, then the range is -infinity to highKey
  // If highKey is null, then the range is lowKey to +infinity
  
  // Initialize and IX_ScanIterator to supports a range search
  RC scan(IXFileHandle &ixfileHandle,
      const Attribute &attribute,
	  const void        *lowKey,
      const void        *highKey,
      bool        lowKeyInclusive,
      bool        highKeyInclusive,
      IX_ScanIterator &ix_ScanIterator);

  // Generate and return the hash value (unsigned) for the given key
  unsigned hash(const Attribute &attribute, const void *key);
  
  unsigned hash_at_specified_level(const int N, const int level, const unsigned int hashed_key);

  
  // Print all index entries in a primary page including associated overflow pages
  // Format should be:
  // Number of total entries in the page (+ overflow pages) : ?? 
  // primary Page No.??
  // # of entries : ??
  // entries: [xx] [xx] [xx] [xx] [xx] [xx]
  // overflow Page No.?? liked to [primary | overflow] page No.??
  // # of entries : ??
  // entries: [xx] [xx] [xx] [xx] [xx]
  // where [xx] shows each entry.
  RC printIndexEntriesInAPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const unsigned &primaryPageNumber);
  
  // Get the number of primary pages
  RC getNumberOfPrimaryPages(IXFileHandle &ixfileHandle, unsigned &numberOfPrimaryPages);

  // Get the number of all pages (primary + overflow)
  RC getNumberOfAllPages(IXFileHandle &ixfileHandle, unsigned &numberOfAllPages);
  
 protected:
  IndexManager   ();                            // Constructor
  ~IndexManager  ();                            // Destructor

 private:
  static IndexManager *_index_manager;
  std::map<std::string, indexInfo> _info;
};


class IX_ScanIterator {
 public:
  IX_ScanIterator();  							// Constructor
  ~IX_ScanIterator(); 							// Destructor

  RC getNextEntry(RID &rid, void *key);  		// Get next matching entry
  RC close();             						// Terminate index scan
};


class IXFileHandle {
public:
	// Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    IXFileHandle();  							// Constructor
    ~IXFileHandle(); 							// Destructor

    int N_Level();

    //file handles
	FileHandle _metaDataFileHandler;
	FileHandle _primBucketDataFileHandler;
	FileHandle _overBucketDataFileHandler;
	indexInfo* _info;
    
private:
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;

};

// print out the error message for a given return code
void IX_PrintError (RC rc);

enum FileHandleInUse
{
	PRIMARY = 0,
	OVERFLOW = 1,
	METADATA = 2
};

struct MetaDataEntry
{
	unsigned int _bucket_number;
	PageNum _overflow_page_number;
	unsigned int _order;
};
//#define SZ_OF_META_ENTRY sizeof(MetaDataEntry)
//#define MAX_META_ENTRIES_IN_PAGE ( (PAGE_SIZE - sizeof(unsigned int)) / SZ_OF_META_ENTRY )

struct BucketDataEntry
{
	void* _key;
	RID _rid;
};

#define SZ_OF_BUCKET_ENTRY sizeof(BucketDataEntry)  // variable size??

#define MAX_BUCKET_ENTRIES_IN_PAGE ( (PAGE_SIZE - sizeof(unsigned int) * 2) / SZ_OF_BUCKET_ENTRY )

class PFMExtension
{
public:
	PFMExtension(IXFileHandle& handle, const BUCKET_NUMBER bkt_number);
	RC translateVirtualToPhysical(PageNum& physicalPageNum, const BUCKET_NUMBER bktNumber, const PageNum virtalPageNum);
	RC getPage(const BUCKET_NUMBER bkt_number, const PageNum pageNumber);
	RC getTuple(void* tuple, const BUCKET_NUMBER bkt_number, const PageNum pageNumber, const int slotNumber);
	RC shiftRecordsToStart(
			const BUCKET_NUMBER bkt_number, const PageNum startingInPageNumber, const int startingFromSlotNumber);
	RC shiftRecordsToEnd(
			const BUCKET_NUMBER bkt_number, const PageNum startingInPageNumber,
			const int startingFromSlotNumber, unsigned int szInBytes);
	RC deleteTuple(const BUCKET_NUMBER bkt_number, const PageNum pageNumber, const int slotNumber, bool lastPageIsEmpty);
			//i can also add new parameter => bool lastPageIsEmpty => so that higher level class can perform merge
	RC insertTuple(
			void* tupleData, const unsigned int tupleLength, const BUCKET_NUMBER bkt_number,
			const PageNum pageNumber, const int slotNumber, bool &newPageIsCreated);
			//i can also add new parameter => bool newPageIsCreated => so that higher level class can perform split
protected:
	RC writePage(const BUCKET_NUMBER bkt_number, const PageNum pageNumber);
	RC removePage(const BUCKET_NUMBER bkt_number, const PageNum pageNumber);
	RC addPage(const void* dataPage, const BUCKET_NUMBER bkt_number);
	//RC updatePageSizeInHeader(FileHandle& fileHandle, const PageNum pageNumber, const unsigned int freeSpace);
	RC shiftRecursivelyToEnd(
			const BUCKET_NUMBER bkt_number, const PageNum currentPageNumber, const unsigned int startingFromSlotNumber,
			map<void*, unsigned int> slotsToShiftFromPriorPage);
private:
	IXFileHandle* _handle;
	void* _buffer;
	PageNum _curVirtualPage;
};

class MetaDataSortedEntries
{
public:
	MetaDataSortedEntries(IXFileHandle& ixfilehandle, BUCKET_NUMBER bucket_number, const Attribute& attr, void* key, const void* entry, unsigned int entryLength);
	~MetaDataSortedEntries();
	void insertEntry();
	RC searchEntry(RID& position, void* entry);
	RC deleteEntry(const RID& rid);
protected:
	int searchEntryInPage(RID& result, const PageNum& pageNumber, const int indexStart, const int indexEnd);
	bool searchEntryInArrayOfPages(RID& position, const int start, const int end);
	RC getPage();
	RC translateToPageNumber(const PageNum& pagenumber, PageNum& result);
	//PageDirSlot* getRecordSlotFromCurrentPage(unsigned int slotNumber);
	RC getTuple(const PageNum pageNumber, const unsigned int slotNumber, void* entry);
	void addPage();
	RC removePageRecord();
	RC erasePageFromHeader(FileHandle& fileHandle);
	unsigned int numOfPages();
	RC splitBucket();
	RC mergeBuckets();
	RC mergeBuckets(BUCKET_NUMBER lowBucket);
private:
	IXFileHandle *_ixfilehandle;
	void* _key;
	Attribute _attr;
	const void* _entryData;
	unsigned int _entryLength;
	int _curPageNum;
	void* _curPageData;
	BUCKET_NUMBER _bktNumber;
	PFMExtension *pfme;
};

#endif
