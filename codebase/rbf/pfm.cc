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
: _info(NULL), _filePtr(NULL)
{
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

    //check that number of bytes read is greater than 0
    return (numBytes > 0) ? 0 : -13;
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

	//check that number of bytes written is greater equal to page size
	return (numBytes == PAGE_SIZE) ? 0 : -13;
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

	//success, return 0
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
