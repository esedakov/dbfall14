
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
: _N(NULL), _Level(NULL), _Next(NULL)
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName, const unsigned &numberOfPages)	//NEED CHECKING
{
	RC errCode = 0;

	//for faster function access create a PFM pointer
	PagedFileManager* _pfm = PagedFileManager::instance();

	//create two files - meta-data file AND bucket-data file
	//	the first (meta-data) file will be called <fileName>_meta
	//	the second (bucket-data) file will be called <fileName>_bucket
	const char * metaFileName = (fileName + "_meta").c_str(), *bucketFileName = (fileName + "_bucket").c_str();
	if( (errCode = _pfm->createFile(metaFileName)) != 0 ||
		(errCode = _pfm->createFileHeader(metaFileName, 2)) != 0 ||
		(errCode = _pfm->createFile(bucketFileName)) != 0 ||
		(errCode = _pfm->createFileHeader(bucketFileName, 1)) != 0 )
	{
		//return error code
		return errCode;
	}

	//allocate buffer for storing the meta-data
	void* data = malloc(PAGE_SIZE);

	//initialize N, Level, and Next
	_N = ((unsigned int*)(data) + 0);
	*_N = IX_INITIAL_NUMBER_OF_BUCKETS;
	_Level = ((unsigned int*)(data) + 1);
	*_Level = 0;
	_Next = ((unsigned int*)(data) + 2);
	*_Next = 0;

	//place these meta-data fields (N, Level, and Next) inside a separate meta-data header (it is
	//in the second page, given that the PFM header that keeps page directory is located in the first page)
	IXFileHandle handle;
	if( (errCode = _pfm->openFile(metaFileName, handle._metaDataFileHandler)) != 0 )
	{
		//return error code
		return errCode;
	}

	//write in a second page that will store meta-data header
	if( (errCode = handle._metaDataFileHandler.writePage(1, data)) != 0 )
	{
		//close file
		_pfm->closeFile(handle._metaDataFileHandler);
		//deallocate buffer
		free(data);
		//return error code
		return errCode;
	}

	//close meta-file
	if( (errCode = _pfm->closeFile(handle._metaDataFileHandler)) != 0 )
	{
		//deallocate buffer
		free(data);
		//return error code
		return errCode;
	}

	//free buffer
	free(data);

	//at this point:
	//meta file should have 2 pages:
	//	1. PFM header with [total size=2][access code=0][next header page=0][number of data pages=1][<page_id=1,number of free bytes=#>]
	//	2. IX meta header with [N=256][Level=0][Next=0]
	//bucket file should have 1 page:
	//	1. PFM header with [total size=1][access code=0][next header page=0][number of data pages=0][NONE]

	//success
	return errCode;
}

RC IndexManager::destroyFile(const string &fileName)
{
	return -1;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixFileHandle)
{
	return -1;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	return -1;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	return -1;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	return -1;
}

unsigned IndexManager::hash(const Attribute &attribute, const void *key)
{
	return 0;
}

RC IndexManager::printIndexEntriesInAPage(IXFileHandle &ixfileHandle, const unsigned &primaryPageNumber) 
{
	return -1;
}

RC IndexManager::getNumberOfPrimaryPages(IXFileHandle &ixfileHandle, unsigned &numberOfPrimaryPages) 
{
	return -1;
}

RC IndexManager::getNumberOfAllPages(IXFileHandle &ixfileHandle, unsigned &numberOfAllPages) 
{
	return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
    const Attribute &attribute,
    const void      *lowKey,
    const void      *highKey,
    bool			lowKeyInclusive,
    bool        	highKeyInclusive,
    IX_ScanIterator &ix_ScanIterator)
{
	return -1;
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	return -1;
}

RC IX_ScanIterator::close()
{
	return -1;
}


IXFileHandle::IXFileHandle()
: appendPageCounter(0), writePageCounter(0), appendPageCounter(0)
{
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	RC errCode = 0;

	_bucketDataFileHandler.collectCounterValues(readPageCount, writePageCount, appendPageCount);
	_metaDataFileHandler.collectCounterValues(readPageCount, writePageCount, appendPageCount);

	return errCode;
}

void IX_PrintError (RC rc)
{
	std::string compName = "";
	if( rc <= -1 && rc >= -16 )
	{
		compName = "PagedFileManager";
	}
	else if( rc <= -20 && rc >= -28 )
	{
		compName = "RecordBasedFileManager";
	}
	else if( rc <= -30 && rc >= -37 )
	{
		compName = "RelationalManager";
	}
	std::string errMsg = "";
	switch(rc)
	{
	case -1:
		errMsg = "attempting to create a file that already exists";
		break;
	case -2:
		errMsg = "fopen failed to create/open new file";
		break;
	case -3:
		errMsg = "information record conflict, i.e. entry with generated FILE* is already in existence";
		break;
	case -4:
		errMsg = "file does not exist";
		break;
	case -5:
		errMsg = "attempting to delete/re-open an opened file";
		break;
	case -6:
		errMsg = "remove failed to delete a file";
		break;
	case -7:
		errMsg = "record for specific file does not exist in _files";
		break;
	case -8:
		errMsg = "file handler that is used for one file is attempted to be used for opening a second file";
		break;
	case -9:
		errMsg = "file handler does not (point to)/(used by) any file";
		break;
	case -10:
		errMsg = "accessed page number is greater than the available maximum";
		break;
	case -11:
		errMsg = "data is corrupted";
		break;
	case -12:
		errMsg = "fseek failed";
		break;
	case -13:
		errMsg = "fread/fwrite failed";
		break;
	case -14:
		errMsg = "fileName is illegal (either NULL or empty string)";
		break;
	case -15:
		errMsg = "unknown number of pages, because handle used to count the pages is not valid (used for not opened file)";
		break;
	case -16:
		errMsg = "could not find header page for the specified id of the data page (list of headers is corrupted)";
		break;
	case -20:
		errMsg = "unknown type of record caught at printRecord function";
		break;
	case -21:
		errMsg = "size of record exceeds size of page (less required page meta-data)";
		break;
	case -22:
		errMsg = "could not insert record, because page header contains incorrect information about free space in the data pages";
		break;
	case -23:
		errMsg = "rid is not setup correctly";
		break;
	case -24:
		errMsg = "directory slot stores wrong information";
		break;
	case -25:
		errMsg = "cannot update record without change of rid (no space within the given page, and cannot move to a different page!)";
		break;
	case -26:
		errMsg = "no requested attribute was found inside the record";
		break;
	case -28:
		errMsg = "page number exceeds the total number of pages in a file";
		break;
	case -30:
		errMsg = "attempt to re-create existing table";
		break;
	case -31:
		errMsg = "accessing/modifying/deleting table that does not exists";
		break;
	case -32:
		errMsg = "Table of tables corrupted";
		break;
	case -33:
		errMsg = "invalid write access to system tableerrMsg = \'attempting to create a file that already exists\'";
		break;
	case -34:
		errMsg = "destructive operation on the catalog";
		break;
	case -35:
		errMsg = "specified column does not exist";
		break;
	case -36:
		errMsg = "attempting to drop non-existing OR add existing field inside the record";
		break;
	case -37:
		errMsg = "wrong table arguments";
		break;
	}
	//print message
	std::cout << "component: " << compName << " => " << errMsg;
}
