#ifndef _pfm_h_
#define _pfm_h_

#include <string>
#include <map>

typedef int RC;
typedef unsigned PageNum;

#define PAGE_SIZE 4096

class FileHandle;

/*
 * maintain information about the file
**/
class FileInfo
{
public:
	//NOTE: all class-methods are intended to be publicly accessible
	FileInfo(std::string name, unsigned int numOpen, PageNum numpages);
	~FileInfo();
public:
	//NOTE: all class-members are intended to be publicly accessible
	/*
	 * file name including the path if there is any
	**/
	std::string _name;
	/*
	 * how many times is the file opened. if value == 0, then it is not opened
	**/
	unsigned int _numOpen;
	/*
	 * number of pages
	**/
	unsigned int _numPages;
};

class PagedFileManager
{
public:
    static PagedFileManager* instance();                     // Access to the _pf_manager instance

    bool isExisting(const char *fileName)	const;

    RC createFile    (const char *fileName);                         // Create a new file
    RC createFileHeader(const char* fileName);
    RC destroyFile   (const char *fileName);                         // Destroy a file
    int countNumberOfOpenedInstances(const char* fileName);
    RC openFile      (const char *fileName, FileHandle &fileHandle); // Open a file
    RC closeFile     (FileHandle &fileHandle);                       // Close a file
    RC getDataPage(FileHandle &fileHandle, const unsigned int recordSize, PageNum& pageNum, PageNum& headerPage, unsigned int& freeSpaceLeftInPage);
    RC insertPage(FileHandle &fileHandle, PageNum& headerPageId, PageNum& dataPageId, const void* content);
    RC findHeaderPage(FileHandle fileHandle, PageNum pageId, PageNum& retHeaderPage);

protected:
    PagedFileManager();                                   // Constructor
    ~PagedFileManager();                                  // Destructor

private:
    static PagedFileManager *_pf_manager;
    /*
     * hash-map that stores information about all files
    **/
    std::map<std::string, FileInfo> _files;
};

//accessibility to the files, in the sense which files can be modified by (user and system) and which solely by the system
enum access_flag
{
	user_can_modify = 0,	//files can be modified by both the user and the system
	only_system_can_modify = 1	//modification can be done just by the system
};

class FileHandle
{
public:
    FileHandle();                                                    // Default constructor
    ~FileHandle();                                                   // Destructor

    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();                                        // Get the number of pages in the file
    void writeBackNumOfPages();											// write back to the header the number of pages
    /* for project 2: flag indicates whether file represents data for the system table or the user table */
    void setAccess(access_flag flag, bool& success);
    access_flag getAccess(bool& success);

    //new method for project 3
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

public:
    /*
     * pointer to the information entity of the file
    **/
    FileInfo* _info;
    /*
     * pointer to the OS file-handler
    **/
    FILE* _filePtr;

    //new variables for project 3
    //keep counter for each operation
	unsigned readPageCounter;
	unsigned writePageCounter;
	unsigned appendPageCounter;
 };


//moved from RBFM.h so that FileHandle -> setAccess could modify the accessibility parameter
/*
 * type for number of page IDs in a header page
**/
typedef unsigned PageIdNum;

/*
 * type of page id
**/
typedef unsigned PageId;

/*
 * information about the page
**/
struct PageInfo
{
	/*
	 * page id (unsigned integer) - index from 0 to N-1, where N is total number of pages in a file and the fact that 0th page is a header
	**/
	PageId _pageid;
	/*
	 * number of bytes free
	**/
	unsigned int _numFreeBytes;
};

#define NUM_OF_PAGE_IDS ( PAGE_SIZE - sizeof(PageNum) - sizeof(PageIdNum) - sizeof(PageNum) - sizeof(access_flag) ) / sizeof(PageInfo)

/*
 * header page
**/
struct Header
{
	/*
	 * total file size
	**/
	PageNum _totFileSize;
	/*
	 * accessibility in terms of who can modify the file
	**/
	access_flag _access;
	/*
	 * page number of the next header page
	**/
	PageNum _nextHeaderPageId;
	/*
	 * number of page IDs used in this page header
	**/
	PageIdNum _numUsedPageIds;
	/*
	 * array of page IDs, occupying the remaining part of the header file
	**/
	PageInfo _arrOfPageIds[ NUM_OF_PAGE_IDS ];
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

//utility functions
void printFile(FileHandle& fileHandle);

#endif
