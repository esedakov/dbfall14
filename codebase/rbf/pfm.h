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
	FileInfo(std::string name, unsigned int numOpen, unsigned int numPages);
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
