#include "pfm.h"
#include <stdio.h>
#include <sys/stat.h>
#include <string.h>
#include <stdlib.h>

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
    {
        _pf_manager = new PagedFileManager();
        _pf_manager->_files = std::map<std::string, FileInfo>();
    }

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}

/*
 * error codes:
 * -1 = attempting to create a file that already exists
 * -2 = fopen failed to create/open new file
 * -3 = information record conflict, i.e. entry with generated FILE* is already in existence
 * -4 = file does not exist
 * -5 = attempting to delete/re-open an opened file
 * -6 = remove failed to delete a file
 * -7 = record for specific file does not exist in _files
 * -8 = file handler that is used for one file is attempted to be used for opening a second file
 * -9 = file handler does not (point to)/(used by) any file
 * -10 = accessed page number is greater than the available maximum
 * -11 = data is corrupted
 * -12 = fseek failed
 * -13 = fread/fwrite failed
 * ---boundary cases:
 * -14 = fileName is illegal (either NULL or empty string)
 * -15 = unknown number of pages, because handle used to count the pages is not valid (used for not opened file)
**/

/*
 * check if file exists
 * NOTE: Taken from rbftest.cc
**/
bool PagedFileManager::isExisting(const char *fileName) const
{
	struct stat stFileInfo;
	if(stat(fileName, &stFileInfo) == 0) return true;
	else return false;
}

RC PagedFileManager::createFile(const char *fileName)
{
	//check for illegal file name
	if( fileName == NULL || strlen(fileName) == 0 )
	{
		return -14;
	}

    //check if file already exists
	if(isExisting(fileName))
	{
		return -1;	//unable to create file, because one already exists with this name
	}

	//create new file
	FILE *write_ptr = fopen(fileName,"wb");

	//check if file was created successfully
	if(!write_ptr)
	{
		return -2;	//system is unable to create new file
	}

	//create string representing file name
	std::string name = std::string(fileName);

	//create file information entity
	FileInfo info = FileInfo(name, 0, 0);

	//insert record into hash-map (_files)
	if( _files.insert( std::pair<std::string, FileInfo>(name, info) ).second == false )
		return -3;	//entry with this FILE* already exists

	//file created successfully
	//close it and return 0
	fclose(write_ptr);
	return 0;
}

RC PagedFileManager::createFileHeader(const char * fileName)
{
	/*
	 * create a header a page:
	 * I use a page directory method, which implies usage of "linked list" of page headers.
	 * The content of header page is as follows:
	 *   + nextHeaderPageId:PageNum
	 *   + numUsedPageIds:PageNum (i.e. unsigned integer)
	 *   + array of <pageIds:(unsigned integer), numFreeBytes:{unsigned integer}> for the rest of header page => (unsigned integer)[PAGE_SIZE - 2*SIZEOF(pageNum)]
	**/
	RC errCode = 0;

	//open file
	FileHandle fileHandle;
	errCode = openFile(fileName, fileHandle);
	if(errCode != 0)
	{
		return errCode;
	}

	//create a header
	Header header;

	//set all header fields to 0
	memset(&header, 0, sizeof(Header));

	//insert page header into the file
	fileHandle.appendPage(&header);

	//write back that this file has number of pages = 1
	fileHandle.writeBackNumOfPages();

	//close file
	errCode = closeFile(fileHandle);
	if(errCode != 0)
	{
		return errCode;
	}

	//success
	return errCode;
}

RC PagedFileManager::findHeaderPage(FileHandle fileHandle, PageNum pageId, PageNum& retHeaderPage)
{
	RC errCode = 0;

	//counters for current header IDs to loop thru
	PageNum headerPageId = 0;

	//pointer to the header page
	Header* hPage = NULL;

	//allocate temporary buffer for page
	void* data = malloc(PAGE_SIZE);

	//loop thru header pages
	do
	{
		//get the first header page
		if( (errCode = fileHandle.readPage((PageNum)headerPageId, data)) != 0 )
		{
			//deallocate data
			free(data);

			//return error
			return errCode;
		}

		//cast data to Header
		hPage = (Header*)data;

		if( pageId <= hPage->_numUsedPageIds )	//NumUsedPageIds counts only the data pages used within this header
												//data pages start from 1 and go up
												//pageId is not necessarily equal to page number (i.e. for header pages > 0, it is a module of page number and num_of_page_ids, so it varies between [1, num_page_ids))
												//for this reason, the comparison between pageId and and numUsedPageIds is inclusive
		{
			//assign a found page id
			retHeaderPage = headerPageId;

			//free temporary buffer for header page
			free(data);

			//return success
			return errCode;
		}

		pageId -= NUM_OF_PAGE_IDS;

		//go to next header page
		headerPageId = hPage->_nextHeaderPageId;

	} while(headerPageId > 0);

	//deallocate temporary buffer for header page
	free(data);

	//return error
	return -16;
}

RC PagedFileManager::getDataPage(FileHandle &fileHandle, const unsigned int recordSize, PageNum& pageNum, PageNum& headerPage, unsigned int& freeSpaceLeftInPage)
{
	RC errCode = 0;

	//maintain current header page id and its former value (from past iteration)
	unsigned int headerPageId = 0, lastHeaderPageId = 0;

	//keep array of bytes of size of page for reading in page data
	void* data = malloc(PAGE_SIZE);

	//casted data to header page
	Header* hPage = NULL;

	//loop thru header pages
	do
	{
		//get the first header page
		if( (errCode = fileHandle.readPage((PageNum)headerPageId, data)) != 0 )
		{
			//deallocate data
			free(data);

			//return error
			return errCode;
		}

		//cast data to Header
		hPage = (Header*)data;

		//get the id of the next header page
		lastHeaderPageId = headerPageId;
		headerPageId = hPage->_nextHeaderPageId;

		//loop thru page entries of the current header page
		for( unsigned int i = 0; i < hPage->_numUsedPageIds; i++ )
		{
			//get the current data page information
			PageInfo* pi = &(hPage->_arrOfPageIds[i]);

			//if it has the proper number of free bytes than return information about this page
			if( pi->_numFreeBytes >= recordSize + sizeof(PageDirSlot) )	//inconsistency found: if amount of free space left in page and requested size are exactly equal than it would skip this candidate page and allocate a new data page, so I changed '>' to '>='
			{
				//assign id
				pageNum = pi->_pageid;

				//update free space left
				pi->_numFreeBytes -= recordSize + sizeof(PageDirSlot);

				freeSpaceLeftInPage = pi->_numFreeBytes;

				//assign a header page
				headerPage = lastHeaderPageId;

				//write header page
				fileHandle.writePage(headerPage, data);

				//deallocate data
				free(data);

				//return success
				return 0;
			}
		}

	} while(headerPageId > 0);

	//assign a header page id
	headerPage = lastHeaderPageId;

	//check if last processed header page can NOT fit a meta-data for new record
	if( hPage->_numUsedPageIds >= NUM_OF_PAGE_IDS )
	{
		//this header page is full, need to create new header page
		//assign a new header page id
		PageNum nextPageId = fileHandle.getNumberOfPages();

		//assign a next header page
		hPage->_nextHeaderPageId = nextPageId;

		//save header page
		fileHandle.writePage(headerPage, data);

		headerPage = nextPageId;

		//prepare parameters for header page allocation
		memset(data, 0, PAGE_SIZE);

		//append new header page
		if( (errCode = fileHandle.appendPage(data)) != 0 )
		{
			//deallocate data
			free(data);

			//return error code
			return errCode;
		}

		//later all fields of header page are accessed by dereferencing to Header structure
		hPage = (Header*)data;
	}

	//need to allocate new data page, because all available data pages do not have enough of free space
	//prepare parameters for data page allocation
	void* dataPage = malloc(PAGE_SIZE);
	memset((void*)dataPage, 0, PAGE_SIZE);

	//allocate a new data page
	if( (errCode = fileHandle.appendPage(dataPage)) != 0 )
	{
		//deallocate data and dataPaga
		free(data);
		free(dataPage);

		//if failed, return error code
		return errCode;
	}

	//get page id of this newly added page
	PageNum dataPageId = fileHandle.getNumberOfPages() - 1;	//page indexes are off by extra 1, header page is '0' and first data page is '1'!

	//set the new record
	hPage->_arrOfPageIds[hPage->_numUsedPageIds]._pageid = dataPageId;
	if( hPage->_arrOfPageIds[hPage->_numUsedPageIds]._numFreeBytes == 0 )
		hPage->_arrOfPageIds[hPage->_numUsedPageIds]._numFreeBytes = PAGE_SIZE - 2 * sizeof(unsigned int);
	hPage->_arrOfPageIds[hPage->_numUsedPageIds]._numFreeBytes -= recordSize + sizeof(PageDirSlot);

	freeSpaceLeftInPage = hPage->_arrOfPageIds[hPage->_numUsedPageIds]._numFreeBytes;

	//assign data page id
	pageNum = dataPageId;

	//increment number of page IDs used in this page
	hPage->_numUsedPageIds++;

	//write header page
	fileHandle.writePage(headerPage, data);

	//write data page
	fileHandle.writePage(dataPageId, dataPage);

	//deallocate data(s)
	free(data);
	free(dataPage);

	//return success, because technically no error occurred
	return 0;
}

int PagedFileManager::countNumberOfOpenedInstances(const char* fileName)
{
	//check for illegal file name
	if( fileName == NULL || strlen(fileName) == 0 )
	{
		return -14;
	}

	//iterator that should point to the item in _files
	std::map<std::string, FileInfo>::iterator iter;

	//get the record of this file from _files
	if( ( iter = _files.find(std::string(fileName)) ) == _files.end() )
	{
		return -4;	//cannot delete, because this file does not exist
	}

	//make sure that file to be deleted is not opened/used
	return (int)(iter->second._numOpen);
}

RC PagedFileManager::destroyFile(const char *fileName)
{
	//check for illegal file name
	if( fileName == NULL || strlen(fileName) == 0 )
	{
		return -14;
	}

	//iterator that should point to the item in _files
	std::map<std::string, FileInfo>::iterator iter;

	//get the record of this file from _files
	if( ( iter = _files.find(std::string(fileName)) ) == _files.end() )
	{
		return -4;	//cannot delete, because this file does not exist
	}

	//make sure that file to be deleted is not opened/used
	if( iter->second._numOpen != 0 )
	{
		return -5;	//attempting to delete an opened file
	}

	//remove file from FileSystem
	if( remove(fileName) != 0 )
		return -6;	//system is unable to delete a file

	//delete record from _files
	_files.erase(iter);

	//destruction of a file was successful, return 0
	return 0;
}

RC PagedFileManager::openFile(const char *fileName, FileHandle &fileHandle)
{
	//check for illegal file name
	if( fileName == NULL || strlen(fileName) == 0 )
	{
		return -14;
	}

	//if file does not exist, then abort
	if( !isExisting(fileName) )
	{
		return -4;	//file does not exist
	}

	//iterator that should point to the item in _files
	std::map<std::string, FileInfo>::iterator iter;

	bool setup_file_info = false;

	//get the record of this file from _files
	if( ( iter = _files.find(std::string(fileName)) ) == _files.end() )
	{

		//create file information entity (by default set created file to be user accessible and modifiable)
		FileInfo info = FileInfo(fileName, 0, 0);//, user_access);

		//mark this file handle to be in need to setup a file info
		setup_file_info = true;

		std::pair<std::map<std::string, FileInfo>::iterator, bool> resultOfInsertion = _files.insert( std::pair<std::string, FileInfo>(fileName, info) );

		//insert record into hash-map (_files)
		if( resultOfInsertion.second == false )
			return -3;	//entry with this FILE* already exists

		//set the iter
		iter = resultOfInsertion.first;
		//return -7; //error removed, since it appears to be possible to open a file that was not created by original DB
	}

	//make sure that given file handler is not used for any file
	if( fileHandle._info != NULL || fileHandle._filePtr != NULL )
	{
		return -8;	//this file handler is being used for another file, yet it is attempted to be used for opening this file
	}

	//open a binary file for both reading and writing
	FILE* file_ptr = fopen(fileName, "rb+");

	//check if file was created successfully
	if( !file_ptr )
	{
		return -2;	//system is unable to create new file
	}

	//setup information for file handler
	fileHandle._info = &(iter->second);
	fileHandle._filePtr = file_ptr;

	//increment "file open instance" counter
	(fileHandle._info->_numOpen)++;

	if( setup_file_info )
	{
		//allocate memory buffer for 1st page
		void* data = malloc(PAGE_SIZE);

		int errCode = 0;

		//allow access to the header page
		fileHandle._info->_numPages = 1;

		//explicitly read the number of pages from the file's first header
		if( (errCode = fileHandle.readPage(0, data)) != 0 )
			return errCode;

		//get the page number
		fileHandle._info->_numPages = ((Header*)data)->_totFileSize;
	}

	//file is opened successfully => return 0
	return 0;

}

RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	//make sure that fileHandle actually points to some file
	if( fileHandle._filePtr == NULL || fileHandle._info == NULL )
	{
		return -9;
	}

	//decrement "file open instance" counter
	(fileHandle._info->_numOpen)--;

	//close file
	fclose(fileHandle._filePtr);

	//reset attributes of fileHandler to null for both info and file_ptr
	fileHandle._filePtr = NULL;
	fileHandle._info = NULL;

	//file is closed successfully => return 0
	return 0;
}


FileHandle::FileHandle()
: _info(NULL), _filePtr(NULL), readPageCounter(0), writePageCounter(0), appendPageCounter(0)
{
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	RC errCode = 0;

	readPageCount += this->readPageCounter;
	writePageCount += this->writePageCounter;
	appendPageCount += this->appendPageCounter;

	return errCode;
}

FileHandle::~FileHandle()
{
	_info = NULL;
	_filePtr = NULL;
}



RC FileHandle::readPage(PageNum pageNum, void *data)
{
    //check that file handler is pointing to some file
    if( _filePtr == NULL || _info == NULL )
    {
    	return -9;
    }

	//check that pageNum is within the boundaries of a given file
    if( pageNum >= getNumberOfPages() )
    {
    	return -10;
    }

    //check that data is not corrupted (i.e. data ptr is not null)
    if( data == NULL )
    {
    	return -11;
    }

    long int size = PAGE_SIZE * pageNum;

    //go to the specified page
    if( fseek(_filePtr, size, SEEK_SET) != 0 )
    {
    	//error occurred during fseek
    	return -12;
    }

    //attempt to read PAGE_SIZE bytes
    size_t numBytes = fread(data, 1, PAGE_SIZE, _filePtr);

    if( numBytes == 0 )
    	return -13;

    //update counter
    readPageCounter = readPageCounter + 1;

    //success
    return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	//check that file handler is pointing to some file
	if( _filePtr == NULL || _info == NULL )
	{
		return -9;
	}

	//check that pageNum is within the boundaries of a given file
	if( pageNum >= getNumberOfPages() )
	{
		return -10;
	}

	//check that data is not corrupted (i.e. data ptr is not null)
	if( data == NULL )
	{
		return -11;
	}

	//go to the specified page
	if( fseek(_filePtr, PAGE_SIZE * pageNum, SEEK_SET) != 0 )
	{
		//error occurred during fseek
		return -12;
	}

	//attempt to write PAGE_SIZE bytes
	size_t numBytes = fwrite(data, 1, PAGE_SIZE, _filePtr);

	if( numBytes != PAGE_SIZE )
		return -13;

	//update counter
	writePageCounter = writePageCounter + 1;

	//success
	return 0;
}

RC FileHandle::appendPage(const void *data)
{
    //check that data is not corrupted (i.e. data ptr is not null)
	if( data == NULL )
	{
		return -11;
	}

	//check that file handler is pointing to some file
	if( _filePtr == NULL || _info == NULL )
	{
		return -9;
	}

	//go to the ending page of the file
	if( fseek(_filePtr, 0, SEEK_END) != 0 )
	{
		//error occurred during fseek
		return -12;
	}

	//write data
	size_t numBytes = fwrite(data, 1, PAGE_SIZE, _filePtr);

	//check that number of bytes written is equal exactly to the page size
	if( numBytes != PAGE_SIZE )
	{
		//fwrite failed
		return -13;
	}

	//increment page count
	_info->_numPages++;

	//update counter
	appendPageCounter = appendPageCounter + 1;

	//success
	return 0;
}

void FileHandle::writeBackNumOfPages()
{
	//allocate buffer for the first header page
	void* firstPageBuffer = malloc(PAGE_SIZE);

	//null it
	memset(firstPageBuffer, 0, PAGE_SIZE);

	//try to read the first header page
	if( readPage(0, firstPageBuffer) != 0 )
		return;

	//increment the page count
	((Header*)firstPageBuffer)->_totFileSize = _info->_numPages;

	if( writePage(0, firstPageBuffer) != 0 )
		return;

	//free buffer
	free(firstPageBuffer);
}

unsigned FileHandle::getNumberOfPages()
{
	//check that handle is pointing to some file
	if( _info == NULL )
	{
		return -15;	//unknown number of pages
	}

	//return number of pages
	return _info->_numPages;
}


FileInfo::FileInfo(std::string name, unsigned int numOpen, PageNum numpages)
: _name(name), _numOpen(numOpen), _numPages(numpages)
{
	//do nothing
}

FileInfo::~FileInfo()
{
	//do nothing
}

void FileHandle::setAccess(access_flag flag, bool& success)
{
	//this is a function added in the second project, due to necessity to include system/user permissions for specific files, which
	//contain a sensitive information, such as catalog

	//this function brakes an abstraction in which file handler does not know what goes on top of its class, i.e. it accesses first header page and
	//modifies information about the system flag.

	//allocate data for first header page
	void* data = malloc(PAGE_SIZE);

	//read this header
	if( readPage(0, data) != 0 )
	{
		//abort procedure
		success = false;
		free(data);
		return;
	}

	//set access
	((Header*)data)->_access = flag;

	//write back header
	if( writePage(0, data) != 0 )
	{
		success = false;
		free(data);
		return;
	}

	//success
	success = true;

	free(data);
}

access_flag FileHandle::getAccess(bool& success)
{
	access_flag flag = user_can_modify;

	//allocate data for first header page
	void* data = malloc(PAGE_SIZE);

	//read this header
	if( readPage(0, data) != 0 )
	{
		success = false;
		free(data);
		return flag;
	}

	//get access
	flag = ((Header*)data)->_access;

	free(data);

	//success
	success = true;
	return flag;
}
