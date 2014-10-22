#ifndef _pfm_h_
#define _pfm_h_

#include <stdlib.h>
#include <string>
#include <map>

typedef int RC;
typedef unsigned PageNum;

#define PAGE_SIZE 4096

class FileHandle;

/*
 * type for number of page IDs in a header page
**/
typedef unsigned PageIdNum;

/*
 * type of page id
**/
typedef unsigned PageId;

#define NUM_OF_PAGE_IDS ( PAGE_SIZE - sizeof(PageNum) - sizeof(PageIdNum) ) / sizeof(PageInfo)

enum access_flag
{
	system_access = 1,
	user_access = 0
};

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

/*
 * header page (not first header, since it has several extra attributes in front: number of pages of file and accessibility flag)
**/
struct Header
{
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

#define NUM_OF_PAGE_IDS_IN_FIRST_HEADER ( PAGE_SIZE - sizeof(PageNum) - sizeof(access_flag) - sizeof(PageNum) - sizeof(PageIdNum) ) / sizeof(PageInfo)

struct FirstPageHeader
{
	/*
	 * total number of pages
	**/
	PageNum totNumPages;
	/*
	 * accessibility flag
	**/
	access_flag _flag;
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
	PageInfo _arrOfPageIds[ NUM_OF_PAGE_IDS_IN_FIRST_HEADER ];
};

/*
 * maintain information about the file
**/
class FileInfo
{
public:
	//NOTE: all class-methods are intended to be publicly accessible
	FileInfo(std::string name, unsigned int numOpen, unsigned int numPages);//, access_flag accessFlag);
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
	/*
	 * access flag
	**/
	access_flag _accessFlag;
};

class PagedFileManager
{
public:
    static PagedFileManager* instance();                     // Access to the _pf_manager instance

    bool isExisting(const char *fileName)	const;

    RC createFile    (const char *fileName);                         // Create a new file
    RC destroyFile   (const char *fileName);                         // Destroy a file
    RC openFile      (const char *fileName, FileHandle &fileHandle); // Open a file
    RC closeFile     (FileHandle &fileHandle);                       // Close a file

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

class FileHandle
{
public:
    FileHandle();                                                    // Default constructor
    ~FileHandle();                                                   // Destructor

    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();                                        // Get the number of pages in the file
    /* for project 2: flag indicates whether file represents data for the system table or the user table */
    void setAccess(access_flag flag, bool& success);
    access_flag getAccess(bool& success);

public:
    /*
     * pointer to the information entity of the file
    **/
    FileInfo* _info;
    /*
     * pointer to the OS file-handler
    **/
    FILE* _filePtr;
 };

 #endif
