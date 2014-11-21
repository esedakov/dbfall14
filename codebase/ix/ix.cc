#include <string.h>
#include "ix.h"
#include <iostream>
#include <stdlib.h>
//#include <functional>
//consult with: http://stackoverflow.com/questions/9848692/c-stl-hash-compilation-issues
#include <tr1/functional>
#include <cmath>

/*
 * error code:
 * -40 => index map is corrupted
 * -41 => primary bucket has wrong number of pages
 * -42 => accessing page beyond the those that are stored in the given bucket
 * -43 => attempting to delete index-entry that does not exist
 * -44 => no overflow page is found in the local map
 * -45 => could not delete page
 * -46 => neither lower nor higher bucket is chosen by the hash function
 *
 * -50 = key was not found
 * -51 = cannot shift from one page more data than can fit inside the next page
 */

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
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

	//create three files - meta-data file AND two bucket-data file
	//	the first (meta-data) file will be called <fileName>_meta
	//	the second (primary bucket-data) file will be called <fileName>_prim
	//	the third (overflow bucket-data) file will be called <fileName>_over
	string strMeta = fileName + "_meta", strBucket = fileName + "_prim", strOverflow = fileName + "_over";
	const char *metaFileName = strMeta.c_str();
	const char *primaryBucketFileName = strBucket.c_str();
	const char *overflowBucketFileName = strOverflow.c_str();

	if( (errCode = _pfm->createFile(metaFileName)) != 0 ||
		(errCode = _pfm->createFileHeader(metaFileName)) != 0 ||
		(errCode = _pfm->createFile(primaryBucketFileName)) != 0 ||
		(errCode = _pfm->createFileHeader(primaryBucketFileName)) != 0 ||
		(errCode = _pfm->createFile(overflowBucketFileName)) != 0 ||
		(errCode = _pfm->createFileHeader(overflowBucketFileName)) != 0 )
	{
		//return error code
		return errCode;
	}

	//allocate buffer for storing the meta-data
	void* data = malloc(PAGE_SIZE);
	memset(data, 0, PAGE_SIZE);

	//initialize N, Level, and Next
	indexInfo info(numberOfPages, 0, 0);
	*((unsigned int*)(data) + 0) = info.N;
	*((unsigned int*)(data) + 1) = info.Level;
	*((unsigned int*)(data) + 2) = info.Next;

	//insert info into map
	_info.insert(std::pair<std::string, indexInfo>(fileName, info));

	//place these meta-data fields (N, Level, and Next) inside a separate meta-data header (it is
	//in the second page, given that the PFM header that keeps page directory is located in the first page)
	IXFileHandle handle;
	if( (errCode = _pfm->openFile(metaFileName, handle._metaDataFileHandler)) != 0 )
	{
		//return error code
		return errCode;
	}

	PageNum headerPageId = 0, dataPageId = 0;

	//find the last header, and insert "data page" in this header
	if( (errCode = _pfm->getLastHeaderPage(handle._metaDataFileHandler, headerPageId)) != 0 )
	{
		//close file
		_pfm->closeFile(handle._metaDataFileHandler);
		//deallocate buffer
		free(data);
		//return error code
		return errCode;
	}

	//write in a second page that will store meta-data header
	//if( (errCode = handle._metaDataFileHandler.appendPage(data)) != 0 )
	unsigned int freeSpaceLeft = 0;
	if( (errCode = _pfm->getDataPage(handle._metaDataFileHandler, (unsigned int)-1, dataPageId, headerPageId, freeSpaceLeft)) != 0 )
	{
		//close file
		_pfm->closeFile(handle._metaDataFileHandler);
		//deallocate buffer
		free(data);
		//return error code
		return errCode;
	}
	if( (errCode = handle._metaDataFileHandler.writePage(dataPageId, data)) != 0 )
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

	//open primary bucket file
	if( (errCode = _pfm->openFile(primaryBucketFileName, handle._primBucketDataFileHandler)) != 0 )
	{
		//deallocate buffer
		free(data);
		//return error code
		return errCode;
	}

	//clear the buffer, since it has contents of IX header now
	memset(data, 0, PAGE_SIZE);

	//insert N primary pages
	for( unsigned int i = 0; i < numberOfPages; i++ )
	{
		//if( (errCode = handle._primBucketDataFileHandler.appendPage(data)) != 0 )
		if( (errCode = _pfm->getDataPage(handle._primBucketDataFileHandler, (unsigned int)-1, dataPageId, headerPageId, freeSpaceLeft)) != 0 )
		{
			//deallocate buffer
			free(data);
			//close primary bucket file
			_pfm->closeFile(handle._primBucketDataFileHandler);
			//return error code
			return errCode;
		}
		if( (errCode = handle._primBucketDataFileHandler.writePage(dataPageId, data)) != 0 )
		{
			//deallocate buffer
			free(data);
			//close primary bucket file
			_pfm->closeFile(handle._primBucketDataFileHandler);
			//return error code
			return errCode;
		}
	}

	//close primary bucket file
	if( (errCode = _pfm->closeFile(handle._primBucketDataFileHandler)) != 0 )
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
	//primary bucket file should have N+1 pages:
	//	1. PFM header with [total size=1][access code=0][next header page=0][number of data pages=0]REPEAT_10_TIMES{ [<page_id=#,number_of_free_bytes=#>] }
	//	2-11. empty pages
	//overflow bucket file should have 1 page:
	//	1. PFM header with [total size=1][access code=0][next header page=0][number of data pages=0][NONE]

	//success
	return errCode;
}

RC IndexManager::destroyFile(const string &fileName)	//NOT TESTED
{
	//delete 3 files - meta-data, primary bucket, overflow bucket files
	RC errCode = 0;

	std::map<std::string, indexInfo>::iterator it;

	//check if map has the specified file
	if( (it = _info.find(fileName)) != _info.end() )
	{
		//since index map has this file, delete it
		_info.erase(it);
	}

	//for faster function access create a PFM pointer
	PagedFileManager* _pfm = PagedFileManager::instance();

	string strMeta = fileName + "_meta", strBucket = fileName + "_prim", strOverflow = fileName + "_over";
	const char *metaFileName = strMeta.c_str();
	const char *primaryBucketFileName = strBucket.c_str();
	const char *overflowBucketFileName = strOverflow.c_str();

	if( (errCode = _pfm->destroyFile( metaFileName )) != 0 ||
		(errCode = _pfm->destroyFile( primaryBucketFileName )) != 0 ||
		(errCode = _pfm->destroyFile( overflowBucketFileName )) != 0 )
	{
		//return error code
		return errCode;
	}

	//success
	return errCode;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixFileHandle)	//NOT TESTED
{
	RC errCode = 0;

	//for faster function access create a PFM pointer
	PagedFileManager* _pfm = PagedFileManager::instance();

	string strMeta = fileName + "_meta", strBucket = fileName + "_prim", strOverflow = fileName + "_over";
	const char *metaFileName = strMeta.c_str();
	const char *primaryBucketFileName = strBucket.c_str();
	const char *overflowBucketFileName = strOverflow.c_str();

	//open meta-data, primary-bucket, and overflow-bucket files
	if( (errCode = _pfm->openFile( metaFileName, ixFileHandle._metaDataFileHandler )) != 0 ||
		(errCode = _pfm->openFile( primaryBucketFileName, ixFileHandle._primBucketDataFileHandler )) != 0 ||
		(errCode = _pfm->openFile( overflowBucketFileName, ixFileHandle._overBucketDataFileHandler )) != 0 )
	{
		//return error code
		return errCode;
	}

	//read meta information into map which is located at the ix-header (second page) of the meta-data file
	void* data = malloc(PAGE_SIZE);

	//read page
	if( (errCode = ixFileHandle._metaDataFileHandler.readPage(1, data)) != 0 )
	{
		//deallocate buffer
		free(data);
		//return error code
		return errCode;
	}

	std::map<std::string, indexInfo>::iterator it;

	//check if the entry exists inside the map with this file name
	if( (it = _info.find(fileName)) == _info.end() )
	{
		//insert a new entry
		std::pair<std::map<std::string, indexInfo>::iterator, bool> resultOfInsertion =
				_info.insert(std::pair<std::string, indexInfo>(fileName, indexInfo()));

		if( resultOfInsertion.second == false )
		{
			//deallocate buffer
			free(data);
			//return error code
			return -40;	//index map is corrupted
		}

		it = resultOfInsertion.first;

		//load list of overflow page IDs
		int curMetaDataPage = 2, maxMetaDataPages = ixFileHandle._metaDataFileHandler._info->_numPages;
		void* metaPageData = malloc(PAGE_SIZE);
		for( ; curMetaDataPage < maxMetaDataPages; curMetaDataPage++ )
		{
			//read overflow page
			if( (errCode = ixFileHandle._metaDataFileHandler.readPage(curMetaDataPage, metaPageData)) != 0 )
			{
				free(metaPageData);
				free(data);
				return errCode;
			}
			//starting from the 2nd meta-data pages, contain list of tuples <bucket number, overflow page id, order>
			//the format of the page is as follows:
			//[number of tuples in the page][bucket number, overflow page id, order][bucket number, overflow page id, order]...
			//\____________________________/\______________________________________/\______________________________________/
			//              4                              12=3*4                                      12
			//iterate over the overflow tuples
			unsigned int tupleIndex = 0;
			for( ; tupleIndex < ((unsigned int*)metaPageData)[0]; tupleIndex++ )
			{
				MetaDataEntry* ptrEntry = (MetaDataEntry*)((char*)metaPageData + sizeof(unsigned int) + tupleIndex * sizeof(MetaDataEntry));
				if( it->second._overflowPageIds.find( ptrEntry->_bucket_number ) == it->second._overflowPageIds.end() )
				{
					it->second._overflowPageIds.insert( std::pair<BUCKET_NUMBER, map<int, PageNum> >(ptrEntry->_bucket_number, std::map<int, PageNum>() ) );
				}
				it->second._overflowPageIds[ptrEntry->_bucket_number].insert( std::pair<int, PageNum>(ptrEntry->_order, ptrEntry->_overflow_page_number) );
			}
		}
	}

	//place infoIndex into IX file handler
	ixFileHandle._info = &(it->second);

	//assign index attributes
	it->second.N = *( ((unsigned int*)data) + 0 );
	it->second.Level = *( ((unsigned int*)data) + 1 );
	it->second.Next = *( ((unsigned int*)data) + 2 );

	unsigned int numPages = 0;
	if( (errCode = getNumberOfPrimaryPages(ixFileHandle, numPages)) != 0 )
	{
		//deallocate buffer
		free(data);
		//return error code
		return errCode;
	}

	//make sure that primary-bucket file has N+1 pages
	if( numPages != it->second.N )
	{
		//deallocate buffer
		free(data);
		//return error code
		return -41;	//primary bucket has wrong number of pages
	}

	//deallocate buffer
	free(data);

	//success
	return errCode;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)	//NOT TESTED
{
	RC errCode = 0;

	//for faster function access create a PFM pointer
	PagedFileManager* _pfm = PagedFileManager::instance();

	//write back the overflow page IDs
	std::map<BUCKET_NUMBER, std::map<int, PageNum> >::iterator bucketIter = ixfileHandle._info->_overflowPageIds.begin(),
			bucketMax = ixfileHandle._info->_overflowPageIds.end();

	//keep the current page counter inside meta-data file
	//(starting from page index 2, i.e. page 0 for PFM header, page 1 for IX header, and then from page 2 is the list of page IDs)
	PageNum curMetaPageIndex = 2;

	//keep the counter of the meta data record within the current page
	unsigned int curMetaEntryIndex = 0;

	bool needToSave = false;

	//buffer for meta-data pages that store list of (overflow) page IDs
	void* buffer = malloc(PAGE_SIZE);
	memset(buffer, 0, PAGE_SIZE);

	unsigned int freeSpaceLeft = 0;

	for( ; bucketIter != bucketMax; bucketIter++ )
	{
		std::map<int, PageNum>::iterator overflowPageIter = bucketIter->second.begin(), overflowPageMax = bucketIter->second.end();
		for( ; overflowPageIter != overflowPageMax; overflowPageIter++ )
		{
			//if update to overflow the page with tuples, then
			if( curMetaEntryIndex == MAX_META_ENTRIES_IN_PAGE )
			{
				//write the number of elements inside the page (it is the very first integer)
				((unsigned int*)buffer)[0] = MAX_META_ENTRIES_IN_PAGE;
				//write this page into file
				//but first determine if the file actually contains this page, if it does not then append a new one with the contents of buffer
				if( ixfileHandle._metaDataFileHandler._info->_numPages <= curMetaPageIndex )
				{
					//append a page
					//if( (errCode = ixfileHandle._metaDataFileHandler.appendPage(buffer)) != 0 )
					unsigned int headerPageId = 0, dataPageId = 0;

					if( (errCode = _pfm->getLastHeaderPage(ixfileHandle._metaDataFileHandler, headerPageId)) != 0 )
					{
						//deallocate buffer
						free(buffer);
						//return error code
						return errCode;
					}

					if( (errCode = _pfm->getDataPage(ixfileHandle._metaDataFileHandler, (unsigned int)-1, dataPageId, headerPageId, freeSpaceLeft)) != 0 )
					{
						free(buffer);
						return errCode;
					}
					if( (errCode = ixfileHandle._metaDataFileHandler.writePage(dataPageId, buffer)) != 0 )
					{
						free(buffer);
						return errCode;
					}
				}
				else
				{
					//write the new contents
					if( (errCode = ixfileHandle._metaDataFileHandler.writePage(curMetaPageIndex, buffer)) != 0 )
					{
						free(buffer);
						return errCode;
					}
				}
				//free the space of the buffer
				memset(buffer, 0, PAGE_SIZE);
				//update counters
				curMetaPageIndex++;
				curMetaEntryIndex = 0;

				//just saved the data
				needToSave = false;
			}

			//set the meta data entry
			MetaDataEntry* ptrEntry = (MetaDataEntry*)((char*)buffer + sizeof(unsigned int) + curMetaEntryIndex * sizeof(MetaDataEntry));
			ptrEntry->_bucket_number = bucketIter->first;
			ptrEntry->_order = (unsigned int) overflowPageIter->first;
			ptrEntry->_overflow_page_number = overflowPageIter->second;

			//update current entry counter
			curMetaEntryIndex++;

			//altered data, need saving
			needToSave = true;
		}
	}

	//backup (not good coding strategy - code duplication from the loop, if you can please refactor it ...)
	if( needToSave )
	{
		//write the number of elements inside the page (it is the very first integer)
		((unsigned int*)buffer)[0] = curMetaEntryIndex;
		//write this page into file
		//but first determine if the file actually contains this page, if it does not then append a new one with the contents of buffer
		if( ixfileHandle._metaDataFileHandler._info->_numPages <= curMetaPageIndex )
		{
			//append a page
			//if( (errCode = ixfileHandle._metaDataFileHandler.appendPage(buffer)) != 0 )
			unsigned int headerPageId = 0, dataPageId = 0;

			if( (errCode = _pfm->getLastHeaderPage(ixfileHandle._metaDataFileHandler, headerPageId)) != 0 )
			{
				//deallocate buffer
				free(buffer);
				//return error code
				return errCode;
			}

			if( (errCode = _pfm->getDataPage(ixfileHandle._metaDataFileHandler, (unsigned int)-1, dataPageId, headerPageId, freeSpaceLeft)) != 0 )
			{
				free(buffer);
				return errCode;
			}
			if( (errCode = ixfileHandle._metaDataFileHandler.writePage(dataPageId, buffer)) != 0 )
			{
				free(buffer);
				return errCode;
			}
		}
		else
		{
			//write the new contents
			if( (errCode = ixfileHandle._metaDataFileHandler.writePage(curMetaPageIndex, buffer)) != 0 )
			{
				free(buffer);
				return errCode;
			}
		}
	}

	//deallocate buffer
	free(buffer);

	//for each file handler write back number of pages
	ixfileHandle._metaDataFileHandler.writeBackNumOfPages();
	ixfileHandle._overBucketDataFileHandler.writeBackNumOfPages();
	ixfileHandle._primBucketDataFileHandler.writeBackNumOfPages();

	//close all file handlers
	if( (errCode = _pfm->closeFile(ixfileHandle._metaDataFileHandler)) != 0 ||
		(errCode = _pfm->closeFile(ixfileHandle._primBucketDataFileHandler)) != 0 ||
		(errCode = _pfm->closeFile(ixfileHandle._overBucketDataFileHandler)) != 0 )
	{
		//return error code
		return errCode;
	}

	//success
	return errCode;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	RC errCode = 0;

	//assume that file handle, attribute, key, and rid are correct

	//hashed key
	unsigned int hkey = hash_at_specified_level(ixfileHandle._info->N, ixfileHandle._info->Level, hash(attribute, key));

	//TODO: key needs to become void*
/*
	BucketDataEntry me = (BucketDataEntry){key, (RID){rid.pageNum, rid.slotNum}};
	MetaDataSortedEntries mdse(ixfileHandle, hkey, (unsigned int)key, (void*)&me);
	mdse.insertEntry();
*/
	//success
	return errCode;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	RC errCode = 0;

	//assume that file handle, attribute, key, and rid are correct

	//hashed key
	unsigned int hkey = hash_at_specified_level(ixfileHandle._info->N, ixfileHandle._info->Level, hash(attribute, key));
/*
	BucketDataEntry me = (BucketDataEntry){key, (RID){rid.pageNum, rid.slotNum}};

	//TODO, change the key
	//Not going to work as expected because it will treat the key as the pointer to the data-key

	MetaDataSortedEntries mdse(ixfileHandle, hkey, (unsigned int)key, (void*)&me);
	if( (errCode = mdse.deleteEntry(me._rid)) != 0 )
	{
		cout << "error : " << errCode;
		exit(errCode);
	}
*/
	//success
	return errCode;
}

//consult with: http://stackoverflow.com/questions/9848692/c-stl-hash-compilation-issues
unsigned IndexManager::hash(const Attribute &attribute, const void *key)
{
	//result is stored inside this variable
	unsigned int hashed_key = 0;

	//depending on the type of the key, use a different std::hash
	switch(attribute.type)
	{
	case TypeInt:
		std::tr1::hash<int> hash_int_fn;
		hashed_key = hash_int_fn(*(int*)key);
		break;
	case TypeReal:
		std::tr1::hash<float> hash_real_fn;
		hashed_key = hash_real_fn(*(float*)key);
		break;
	case TypeVarChar:
		std::tr1::hash<std::string> hash_str_fn;
		//create null-terminated character array
		int sz_of_carr = *(unsigned int*)key;
		char* carr = (char*)malloc( sz_of_carr + 1 );
		memcpy(carr, (char*)key + 4, sz_of_carr);
		carr[sz_of_carr] = 0;	//null terminating the character array
		//create string from the character array and generate a hash
		std::string str = std::string(carr);
		hashed_key = hash_str_fn(str);
		break;
	}

	//success
	return hashed_key;
}

unsigned int IndexManager::hash_at_specified_level(const int N, const int level, const unsigned int hashed_key)
{
	//take a modulo
	return hashed_key % (int)( pow((double)2.0, level) * N );
}

RC IndexManager::printIndexEntriesInAPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const unsigned &primaryPageNumber)
{
	return -1;
}

RC IndexManager::getNumberOfPrimaryPages(IXFileHandle &ixfileHandle, unsigned &numberOfPrimaryPages) 	//NOT TESTED
{
	RC errCode = 0;

	numberOfPrimaryPages = ixfileHandle._primBucketDataFileHandler.getNumberOfPages() - 1;

	//success
	return errCode;
}

RC IndexManager::getNumberOfAllPages(IXFileHandle &ixfileHandle, unsigned &numberOfAllPages) 	//NOT TESTED
{
	RC errCode = 0;

	numberOfAllPages = ixfileHandle._primBucketDataFileHandler.getNumberOfPages() +
			           ixfileHandle._metaDataFileHandler.getNumberOfPages() +
			           ixfileHandle._overBucketDataFileHandler.getNumberOfPages();

	//success
	return errCode;
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
: _info(NULL), readPageCounter(0), writePageCounter(0), appendPageCounter(0)
{
}

IXFileHandle::~IXFileHandle()
{
}

int IXFileHandle::N_Level()
{
	return (int)( pow((double)2.0, (int)_info->Level) * _info->N );
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	RC errCode = 0;

	_primBucketDataFileHandler.collectCounterValues(readPageCount, writePageCount, appendPageCount);
	_overBucketDataFileHandler.collectCounterValues(readPageCount, writePageCount, appendPageCount);
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
	else if( rc <= -40 )
	{
		compName = "IndexManager";
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
	case -40:
		errMsg = "index map is corrupted";
		break;
	case -41:
		errMsg = "primary bucket has wrong number of pages";
		break;
	case -42:
		errMsg = "accessing page beyond the those that are stored in the given bucket";
		break;
	case -43:
		errMsg = "attempting to delete index-entry that does not exist";
		break;
	case -44:
		errMsg = "no overflow page is found in the local map";
		break;
	case -45:
		errMsg ="could not delete page";
		break;
	case -46:
		errMsg = "neither lower nor higher bucket is chosen by the hash function";
		break;
	}
	//print message
	std::cout << "component: " << compName << " => " << errMsg;
}

//PFM EXTENSION CLASS METHODS -- BEGIN

//both virtual and physical page numbers
RC PFMExtension::translateVirtualToPhysical(PageNum& physicalPageNum, const BUCKET_NUMBER bkt_number, const PageNum virtualPageNum)
{
	physicalPageNum = bkt_number + 1;	//setup by default to point at primary page
										//primary file has first page reserved for PFM header, so bucket # 0 starts at page # 1, which
										//is why actualPageNumber is bucket number + 1

	if( virtualPageNum > 0 )	//if inside the overflow file
	{
		//then, consult with information about overflow page locations stored inside the file handler to
		//determine which page to load

		//first check whether the page exists inside overflow file (i.e. is virtual page points beyond the file)
		unsigned int size = _handle->_info->_overflowPageIds[bkt_number].size();
		if( (virtualPageNum - 1) >= size )
		{
			return -42;	//accessing a page beyond bucket's data
		}

		//get physical overflow page number corresponding to the "virtual page number"
		physicalPageNum = _handle->_info->_overflowPageIds[bkt_number][virtualPageNum - 1];
	}

	//success
	return 0;
}

//virtual page number
RC PFMExtension::getPage(const BUCKET_NUMBER bkt_number, const PageNum pageNumber, void* buffer)
{
	RC errCode = 0;

	//determine physical page number from the given virtual page number
	PageNum physicalPageNumber = 0;
	if( (errCode = translateVirtualToPhysical(physicalPageNumber, bkt_number, pageNumber)) != 0 )
	{
		return errCode;
	}

	FileHandle handle;
	if( pageNumber > 0 )
	{
		//if inside the overflow file
		handle = _handle->_overBucketDataFileHandler;
	}
	else
	{
		//if inside the primary file
		handle = _handle->_primBucketDataFileHandler;
	}

	//check if physical page number is beyond boundaries
	if( physicalPageNumber >= handle.getNumberOfPages() )
	{
		return -27;
	}

	//retrieve the data and store in the current buffer
	if( (errCode = handle.readPage(physicalPageNumber, buffer)) != 0 )
	{
		return errCode;
	}

	//success
	return errCode;
}

PFMExtension::PFMExtension(IXFileHandle& handle, BUCKET_NUMBER bkt_number)
: _handle(&handle), _buffer(malloc(PAGE_SIZE)), _curVirtualPage(0)
{
	RC errCode = 0;
	if( (errCode = getPage(bkt_number, _curVirtualPage, _buffer)) != 0 )
	{
		IX_PrintError(errCode);
		exit(errCode);
	}
}

//virtual page number
RC PFMExtension::getTuple(void* tuple, BUCKET_NUMBER bkt_number, const unsigned int pageNumber, const int slotNumber) //modified version of ReadRecord from RBFM
{
	RC errCode = 0;

	if( tuple == NULL )
	{
		return -11; //data is corrupted
	}

	FileHandle handle = ( _curVirtualPage == 0 ? _handle->_primBucketDataFileHandler : _handle->_overBucketDataFileHandler );

	//if necessary read in the page
	if( pageNumber != _curVirtualPage )
	{
		//write the current page to some (primary or overflow depending on the current virtual page number) file
		if( (errCode = writePage(bkt_number, _curVirtualPage)) != 0 )
		{
			return errCode;
		}

		//read data page
		if( (errCode = getPage(bkt_number, pageNumber, _buffer)) != 0 )
		{

			//read failed
			return errCode;
		}

		_curVirtualPage = pageNumber;
	}

	/*
	 * data page has a following format:
	 * [list of records without any spaces in between][free space for records][list of directory slots][(number of slots):unsigned int][(offset from page start to the start of free space):unsigned int]
	 * ^                                                                      ^                       ^                                                                                                 ^
	 * start of page                                                          start of dirSlot        end of dirSlot                                                                          end of page
	 */

	//get pointer to the end of directory slots
	PageDirSlot* ptrEndOfDirSlot = (PageDirSlot*)((char*)_buffer + PAGE_SIZE - 2 * sizeof(unsigned int));

	//find out number of directory slots
	unsigned int numSlots = *((unsigned int*)ptrEndOfDirSlot);

	//check if rid is correct in terms of indexed slot
	if( slotNumber >= (int)numSlots )
	{

		return -23; //rid is not setup correctly
	}

	//get slot
	PageDirSlot* curSlot = (PageDirSlot*)(ptrEndOfDirSlot - slotNumber - 1);

	//find slot offset
	unsigned int offRecord = curSlot->_offRecord;
	unsigned int szRecord = curSlot->_szRecord;

	//check if slot attributes make sense
	if( offRecord == 0 && szRecord == 0 )
	{
		return -24;	//directory slot stores wrong information
	}

	//determine pointer to the record
	char* ptrRecord = (char*)(_buffer) + offRecord;

	//copy record contents
	memcpy(tuple, ptrRecord, szRecord);

	//return success
	return errCode;
}

//startingInPageNumber is a virtual page number
RC PFMExtension::shiftRecordsToStart //TESTED, seems to work
	(const BUCKET_NUMBER bkt_number, const PageNum startingInPageNumber, const int startingFromSlotNumber)
{
	RC errCode = 0;

	//if necessary read in the page
	if( startingInPageNumber != _curVirtualPage )
	{
		_curVirtualPage = startingInPageNumber;

		//read data page
		if( (errCode = getPage(bkt_number, startingInPageNumber, _buffer)) != 0 )
		{
			//read failed
			return errCode;
		}
	}

	FileHandle handle = ( _curVirtualPage == 0 ? _handle->_primBucketDataFileHandler : _handle->_overBucketDataFileHandler );

	PageNum physicalPageNumber = 0;
	if( (errCode = translateVirtualToPhysical(physicalPageNumber, bkt_number, startingInPageNumber)) != 0 )
	{
		return errCode;
	}

	//get pointer to the end of directory slots
	PageDirSlot* startOfDirSlot = (PageDirSlot*)((char*)_buffer + PAGE_SIZE - 2 * sizeof(unsigned int));

	//find out number of directory slots
	unsigned int* numSlots = ((unsigned int*)startOfDirSlot);

	//check if rid is correct in terms of indexed slot
	if( startingFromSlotNumber >= (int)*numSlots )
	{
		return -23; //rid is not setup correctly
	}

	//pointer to the start of the list of directory slots
	PageDirSlot* ptrEndOfDirSlot = (PageDirSlot*)( startOfDirSlot - *numSlots );

	unsigned int* ptrVarForFreeSpace = (unsigned int*)( (char*)(startOfDirSlot) + sizeof(unsigned int) );

	unsigned int offsetToFreeSpace = *ptrVarForFreeSpace;

	//                                                      [offset, size]
	//   L1        L2       L3          Lk                    \        /
	//<-------> <------> <------>   <------->                  \      /
	//[key|rid][key|rid][key|rid]...[key|rid][                ][slot k]...[slot 3][slot 2][slot 1][num slots][offset to free space]
	//^                                     ^                 ^                                  ^          ^                     ^
	//|                                     |                 |                                  |          |                     |
	//+-------------------------------------+-----------------+----------------------------------+----------+---------------------+
	//          list of tuples               free space if any        page directory slots

	//when item is shifted to the start then:
	//1. tuples <key, rid> are shifted to the start of the page by L1 amount which is stored inside the first slot

	PageDirSlot* deletedSlot = startOfDirSlot - (startingFromSlotNumber + 1);

	char *destination = (char*)_buffer + deletedSlot->_offRecord;
	char *startOfShiftingBlock = destination + deletedSlot->_szRecord;
	char *endOfShiftingBlock = (char*)_buffer + *ptrVarForFreeSpace;
	unsigned int szToShift = endOfShiftingBlock - startOfShiftingBlock;

	//update the offset to free space
	offsetToFreeSpace -= deletedSlot->_szRecord;

	if( szToShift > 0 )
		memmove(destination, startOfShiftingBlock, szToShift);

	//erase the record
	memset( (char*)_buffer + offsetToFreeSpace, 0, deletedSlot->_szRecord );//ptrEndOfDirSlot->_szRecord );

	bool needToSavePage = true;

	//shift slots
	//       copy array of slots
	//   +-------------------------+ => move in this direction to replace say slot # 1 (assuming it is the slot to be deleted)
	//   v                         v
	//...[slot k]...[slot 3][slot 2][slot 1]...
	//   ^                                 ^
	//   |                                 |
	// ptrEndOfDirSlot          startOfDirSlot
	unsigned int numOfSlotsToShift = (*numSlots - 1 - startingFromSlotNumber) * sizeof(PageDirSlot);
	unsigned int deleted_slot_size = deletedSlot->_szRecord;
	if( numOfSlotsToShift > 0 )
	{
		memmove( ptrEndOfDirSlot + 1, ptrEndOfDirSlot, numOfSlotsToShift );
		//fix offsets since they are messed up due to slots
		PageDirSlot* curEntry = deletedSlot;
		unsigned int offset = 0;
		if( (deletedSlot + 1) != startOfDirSlot )
			offset = (deletedSlot + 1)->_offRecord + (deletedSlot + 1)->_szRecord;
		for( ; curEntry != ptrEndOfDirSlot; curEntry -= 1 )
		{
			curEntry->_offRecord = offset;
			offset += curEntry->_szRecord;
		}
	}

	//null the last slot
	memset(ptrEndOfDirSlot, 0, sizeof(PageDirSlot));

	bool decreaseNumberOfSlotsInCurrentPage = true;

	//2. if next page exists
	if( _curVirtualPage + 1 <= _handle->_info->_overflowPageIds[bkt_number].size() )
	{
		//	2.1.1 check if first entry inside the page can fit in the former page, if not go to step 3
		//		=> free space in question is calculated as follows:
		//			PAGE_SIZE - offset_to_free_space - num_slots * size_of_slot - 2 * size_of_integer

		unsigned int freeSpace =
				(PAGE_SIZE - *ptrVarForFreeSpace - *numSlots * sizeof(PageDirSlot) - 2 * sizeof(unsigned int)) + deleted_slot_size;

		//allocate a separate buffer for holding the current page contents
		void* dataBuffer = malloc(PAGE_SIZE);
		memcpy(dataBuffer, _buffer, PAGE_SIZE);

		//read data page
		if( (errCode = getPage(bkt_number, _curVirtualPage + 1, _buffer)) != 0 )
		{
			//deallocate dataPage
			free(dataBuffer);

			//read failed
			return errCode;
		}

		//calculate size of the first record in the next page (ptrEndOfDirSlot and numSlots are used as pointers in the next page)
		unsigned int szOfFirstRecordInNextPage = (startOfDirSlot - 1)->_szRecord;

		if( szOfFirstRecordInNextPage <= freeSpace && *numSlots > 0 )
		{
			decreaseNumberOfSlotsInCurrentPage = false;
			//	2.1.2 copy this item (first record from the next page) after the record # k

			memcpy( (char*)dataBuffer + offsetToFreeSpace, _buffer, szOfFirstRecordInNextPage );

			//copy back the contents of the current page to the _buffer
			memcpy( _buffer, dataBuffer, PAGE_SIZE );

			//	2.1.3 insert information into new slot into page directory to account for shifted record

			ptrEndOfDirSlot->_offRecord = offsetToFreeSpace;
			ptrEndOfDirSlot->_szRecord = szOfFirstRecordInNextPage;
			*ptrVarForFreeSpace = offsetToFreeSpace + szOfFirstRecordInNextPage;

			//	2.1.4 save page before recursively calling this function on the next page
			if( (errCode = handle.writePage(physicalPageNumber, _buffer)) != 0 )
			{
				free(dataBuffer);
				return errCode;
			}
			needToSavePage = false;

			//	2.1.5 call shift to start on the next page
			if( (errCode = shiftRecordsToStart(bkt_number, _curVirtualPage + 1, 0)) != 0 )
			{
				free(dataBuffer);
				return errCode;
			}
		}
		else
		{
			//copy back the contents of the current page to the _buffer
			memcpy( _buffer, dataBuffer, PAGE_SIZE );
		}

		//deallocate data buffer
		free(dataBuffer);
	}

	//if the next page is non-existent or empty, then
	if( decreaseNumberOfSlotsInCurrentPage == true )
	{
		//	2.2.1 change number slots, offset to free space
		//update number of slots
		*numSlots = *numSlots - 1;

		//update offset to free space
		*ptrVarForFreeSpace = offsetToFreeSpace;
	}
	//3. save the page and return success
	//save page before recursively calling this function on the next page
	if( needToSavePage )
	{
		if( (errCode = handle.writePage(physicalPageNumber, _buffer)) != 0 )
		{
			return errCode;
		}
	}
	//success
	return errCode;
}

/*RC PFMExtension::updatePageSizeInHeader(FileHandle& fileHandle, const PageNum pageNumber, const unsigned int freeSpace)
{
	RC errCode = 0;

	//start from page 0 (which is the first header)
	PageNum headerPageId = 0;

	//allocate data buffer for storing the current header page
	void* data = malloc(PAGE_SIZE);

	//create pointer for header
	Header* hPage = NULL;

	bool found = false;

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

		//loop thru PageInfo tuples
		for( unsigned int i = 0; i < NUM_OF_PAGE_IDS; i++ )
		{
			if( hPage->_arrOfPageIds[i]._pageid == pageNumber )
			{
				//found the page record
				hPage->_arrOfPageIds[i]._numFreeBytes = freeSpace;
				found = true;
				//quit
				break;
			}
		}

		if( found )
			break;

		//go to next header page
		headerPageId = hPage->_nextHeaderPageId;

	} while(headerPageId > 0);

	if( found == false )
	{
		//have not found the appropriate page
		return -45;
	}

	//write back the header page
	if( (errCode = fileHandle.writePage((PageNum)headerPageId, data)) != 0 )
	{
		//deallocate data
		free(data);

		//return error
		return errCode;
	}

	//success
	return errCode;
}*/

RC PFMExtension::writePage(const BUCKET_NUMBER bkt_number, const PageNum pageNumber)
{
	RC errCode = 0;

	//translate to physical page number
	PageNum physicalPageNumber = 0;
	if( (errCode = translateVirtualToPhysical(physicalPageNumber, bkt_number, pageNumber)) != 0 )
	{
		return errCode;
	}

	//write into either primary or overflow file depending on the virtual page number
	if( pageNumber == 0 )
	{
		if( (errCode = _handle->_primBucketDataFileHandler.writePage(physicalPageNumber, _buffer)) != 0 )
		{
			return errCode;
		}
	}
	else
	{
		if( (errCode =_handle->_overBucketDataFileHandler.writePage(physicalPageNumber, _buffer)) != 0 )
		{
			return errCode;
		}
	}

	//success
	return errCode;
}

RC PFMExtension::shiftRecursivelyToEnd //NOT TESTED
	(const BUCKET_NUMBER bkt_number, const PageNum currentPageNumber, const unsigned int startingFromSlotNumber,
	map<void*, unsigned int> slotsToShiftFromPriorPage)
{
	RC errCode = 0;

	//if necessary read in the page
	if( currentPageNumber != _curVirtualPage )
	{
		//read data page
		if( (errCode = getPage(bkt_number, currentPageNumber, _buffer)) != 0 )
		{
			//read failed
			return errCode;
		}

		//update
		_curVirtualPage = currentPageNumber;
	}

	FileHandle handle = ( _curVirtualPage == 0 ? _handle->_primBucketDataFileHandler : _handle->_overBucketDataFileHandler );

	//get pointer to the end of directory slots
	PageDirSlot* startOfDirSlot = (PageDirSlot*)((char*)_buffer + PAGE_SIZE - 2 * sizeof(unsigned int));

	//find out number of directory slots
	unsigned int* numSlots = ((unsigned int*)startOfDirSlot);

	//check if rid is correct in terms of indexed slot
	if( startingFromSlotNumber > *numSlots )
	{
		return -23; //rid is not setup correctly
	}

	//pointer to the start of the list of directory slots
	PageDirSlot* ptrEndOfDirSlot = (PageDirSlot*)( startOfDirSlot - *numSlots );

	unsigned int* ptrVarForFreeSpace = (unsigned int*)( (char*)(startOfDirSlot) + sizeof(unsigned int) );

	//                                                      [offset, size]
	//   L1        L2       L3          Lk                    \        /
	//<-------> <------> <------>   <------->                  \      /
	//[key|rid][key|rid][key|rid]...[key|rid][                ][slot k]...[slot 3][slot 2][slot 1][num slots][offset to free space]
	//^                                     ^                 ^                                  ^          ^                     ^
	//|                                     |                 |                                  |          |                     |
	//+-------------------------------------+-----------------+----------------------------------+----------+---------------------+
	//          list of tuples               free space if any        page directory slots

	//1. determine number of slots to be erased/shifted_out from this page
	//	1.1 measure size of the incoming data. If the size of this data exceeds the page, then fail (cannot shift in more than page size)

	unsigned int szOfDataToShiftIn = 0;
	map<void*, unsigned int>::iterator shiftInIter = slotsToShiftFromPriorPage.begin(), shiftInMax = slotsToShiftFromPriorPage.end();
	for( ; shiftInIter != shiftInMax; shiftInIter++ )
	{
		szOfDataToShiftIn += shiftInIter->second;
	}

	//update by the amount of slots to be inserted in
	unsigned int szForExtraSlots = sizeof(PageDirSlot) * slotsToShiftFromPriorPage.size();

	//	1.2 determine number of existing records that has to be erased to fit new data

	//determine amount of free space in this page
	//....[key|rid][                        ][slot k]....
	//             ^                         ^
	//             |                         |
	// offset to free space            ptrEndOfDirSlot
	unsigned int freeSpaceInPage = (unsigned int)( (char*)ptrEndOfDirSlot - (char*)((char*)_buffer + (unsigned int)*ptrVarForFreeSpace) );

	unsigned int amountOfSpaceToBeFreedUp = 0;
	unsigned int numOfSlotsToBeErased = 0;
	PageDirSlot* eraseSlotsTillThisOne = ptrEndOfDirSlot, *currentSlot = NULL;

	//if existing free space is not enough, then
	if( freeSpaceInPage < szOfDataToShiftIn + szForExtraSlots )
	{
		//iteratively go thru slots (from end to start) to determine the amount of records that need to be erased
		currentSlot = ptrEndOfDirSlot;
		while( currentSlot != startOfDirSlot )
		{
			//sum up the space of current slot
			amountOfSpaceToBeFreedUp += currentSlot->_szRecord;
			numOfSlotsToBeErased++;
			//update by the amount of freed up slot
			if( numOfSlotsToBeErased <= slotsToShiftFromPriorPage.size() )
				szForExtraSlots -= sizeof(PageDirSlot);
			//check if this space is enough
			if( amountOfSpaceToBeFreedUp + freeSpaceInPage >= szOfDataToShiftIn + szForExtraSlots )
				break;
			//go to the next slot
			currentSlot = currentSlot + 1;
		}
		//sanity check - determine if we have found enough space
		if( amountOfSpaceToBeFreedUp + freeSpaceInPage < szOfDataToShiftIn + szForExtraSlots )
		{
			return -51;	//cannot shift too much data into page
		}
		eraseSlotsTillThisOne = currentSlot + 1;	//currentSlot points at the last slot that still to be erased, so increment to next one
	}

	//	1.3 allocate for each erased slot a separate small buffer to hold its copy and insert each such buffer into a map data-structure
	//		it will then be passed to a recursive function for shifting into the next page

	std::map<void*, unsigned int> shiftRecordsToNextPage;
	currentSlot = ptrEndOfDirSlot;
	while( currentSlot != eraseSlotsTillThisOne )
	{
		//determine size of the record
		unsigned int szOfRecord = currentSlot->_szRecord;
		//allocate buffer
		void* buffer = malloc(szOfRecord);
		//copy record into the buffer
		memcpy(buffer, (char*)_buffer + currentSlot->_offRecord, szOfRecord);
		//insert new entry into map for this record
		shiftRecordsToNextPage.insert( std::pair<void*, unsigned int>(buffer, szOfRecord) );
		//next
		currentSlot++;
	}

	//2. shift array of records from the designated one (i.e. startingFromSlotNumber) to the one that precedes the first item that will
	//	 be erased (determined in step 1)

	//                       to be deleted
	//
	//      shift by              |    free
	//    ___size X____           |  __space___
	//   |             |          v |          |
	//[ ][   ][  ][ ][  ][  ][ ][XX]            |
	//     ^                     \             /
	//     |                     shift by size X
	// insert_in
	//
	//[ ][   ][  ][ ][  ][  ][ ][XX] => shift by size 'X'
	//   ^                     ^
	//   |                     |
	// start                  end
	//star is specified by startingFromSlotNumber
	//end is right before the last item that will be deleted due to shift (on image item to be deleted has 'XX' content in it)

	PageDirSlot* ptrStartSlot = startOfDirSlot - startingFromSlotNumber;
	void* startOfShifting = NULL;
	if( startingFromSlotNumber > 0 )
		startOfShifting = (char*)_buffer + ptrStartSlot->_offRecord + ptrStartSlot->_szRecord;
	else
		startOfShifting = (char*)_buffer;

	//if it is not the last item that the data is inserted, then perform a shift (if it is the last item, do not do shifting)
	if( startingFromSlotNumber != *numSlots )
	{
		void* endOfShifting = ((char*)_buffer + eraseSlotsTillThisOne->_offRecord) + eraseSlotsTillThisOne->_szRecord;
		unsigned int amountToShift = szOfDataToShiftIn;

		if( szOfDataToShiftIn > 0 )
		{
			//shift records
			memmove((char*)startOfShifting + amountToShift, startOfShifting, (char*)endOfShifting - (char*)startOfShifting);

			//shift slots
			memmove(
				(char*)(eraseSlotsTillThisOne - slotsToShiftFromPriorPage.size()),
				(char*)(eraseSlotsTillThisOne),
				(char*)ptrStartSlot - (char*)eraseSlotsTillThisOne
			);
		}
	}

	//3. copy information into freed up array of records AND setup slot offsets and size attributes

	currentSlot = ptrStartSlot - 1;
	shiftInIter = slotsToShiftFromPriorPage.begin();
	shiftInMax = slotsToShiftFromPriorPage.end();
	for( ; shiftInIter != shiftInMax; shiftInIter++ )
	{
		//copy the data portion for this record
		memcpy( startOfShifting, shiftInIter->first, shiftInIter->second );
		//update slot information
		currentSlot->_szRecord = shiftInIter->second;
		currentSlot->_offRecord = (unsigned int)((char*)startOfShifting - (char*)_buffer);
		//update starting position of shift
		startOfShifting = (char*)startOfShifting + shiftInIter->second;
		//update current slot
		currentSlot = currentSlot - 1;
	}

	//update the shifted slots' offsets (since they remained the same and now are incorrect)
	PageDirSlot* updatedEndSlot = eraseSlotsTillThisOne - slotsToShiftFromPriorPage.size() - 1;
	while( currentSlot != updatedEndSlot )
	{
		currentSlot->_offRecord = (currentSlot + 1)->_offRecord + (currentSlot + 1)->_szRecord;
		currentSlot--;
	}

	//4. update offset to free space

	//  empty space if any that goes after the last slot
	//  |
	//  v
	//[   ][slot k][slot k-1]...[slot N]...[slot 2][slot 1][ORIGINAL NUM OF SLOTS][OFFSET TO FREE SPACE]
	//^    \                   /                           ^          |
	//|     +-----------------+                            |          v
	//|          new slots                                 |          N where N < k
	//|                                                    |
	//+-- currentSlot points                               |
	//    at this position                          startOfDirSlot

	//[ ][4][3][2][1]
	//^             ^
	//current       start   (keep in mind physical address of start is greater than of the current)

	//number of extra slots
	int numExtraSlots = ( (startOfDirSlot - currentSlot) - 1) - *numSlots;

	//update offset to a free space
	//*ptrVarForFreeSpace =
	//		amountOfSpaceToBeFreedUp + freeSpaceInPage - szOfDataToShiftIn -
	//		(numExtraSlots > 0 ? numExtraSlots * sizeof(PageDirSlot) : 0);
	*ptrVarForFreeSpace = *ptrVarForFreeSpace +
			szOfDataToShiftIn - amountOfSpaceToBeFreedUp;

	//update number of slots in a page
	*numSlots += numExtraSlots;

	bool needToSave = true;

	//5, if there are items to shift to the next page
	if( shiftRecordsToNextPage.size() > 0 )
	{
		//	5.1 AND if the next page does not exist, then add a new page
		if( currentPageNumber + 1 > _handle->_info->_overflowPageIds[bkt_number].size() )
		{
			//allocate new page data buffer
			void* newPageDataBuffer = malloc(PAGE_SIZE);
			memset(newPageDataBuffer, 0, PAGE_SIZE);

			//add the page to this file
			if( (errCode = addPage(newPageDataBuffer, bkt_number)) != 0 )
			{
				free(newPageDataBuffer);
				shiftInIter = shiftRecordsToNextPage.begin();
				shiftInMax = shiftRecordsToNextPage.end();
				for( ; shiftInIter != shiftInMax; shiftInIter++ )
				{
					free( shiftInIter->first );
				}
				return errCode;
			}

			//deallocate data page buffer
			free(newPageDataBuffer);
		}

		//write the current page to some (primary or overflow depending on the current virtual page number) file
		if( (errCode = writePage(bkt_number, _curVirtualPage)) != 0 )
		{
			return errCode;
		}
		needToSave = false;

		//	5.2 call this function (recursively) on the next page with the map data-structure composed in step 1
		if( (errCode = shiftRecursivelyToEnd(
				bkt_number, currentPageNumber + 1, 0, shiftRecordsToNextPage)) != 0 )
		{
			//free the buffers
			shiftInIter = shiftRecordsToNextPage.begin();
			shiftInMax = shiftRecordsToNextPage.end();
			for( ; shiftInIter != shiftInMax; shiftInIter++ )
			{
				free( shiftInIter->first );
			}
			return errCode;
		}
	}

	//6. iterate over the map that stored <void*, unsigned int> information about shifted records => free up all void* buffers
	shiftInIter = shiftRecordsToNextPage.begin();
	shiftInMax = shiftRecordsToNextPage.end();
	for( ; shiftInIter != shiftInMax; shiftInIter++ )
	{
		free( shiftInIter->first );
	}

	if( needToSave )
	{
		//write the current page to some (primary or overflow depending on the current virtual page number) file
		if( (errCode = writePage(bkt_number, _curVirtualPage)) != 0 )
		{
			return errCode;
		}
	}

	//success
	return errCode;
}

RC PFMExtension::addPage(const void* dataPage, const BUCKET_NUMBER bkt_number)	//NOT TESTED
{
	RC errCode = 0;

	PagedFileManager* _pfm = PagedFileManager::instance();

	unsigned int headerPageId = 0, dataPageId = 0;
	//insert page into overflow data file
	if( (errCode = _pfm->getLastHeaderPage(_handle->_overBucketDataFileHandler, headerPageId)) != 0 )
	{
		return errCode;
	}

	unsigned int freeSpaceLeft = 0;

	if( (errCode = _pfm->getDataPage(
			_handle->_overBucketDataFileHandler, (unsigned int)-1, dataPageId, headerPageId, freeSpaceLeft)) != 0 )
	{
		return errCode;
	}

	if( (errCode = _handle->_overBucketDataFileHandler.writePage(dataPageId, dataPage)) != 0 )
	{
		return errCode;
	}

	int newOrderValue = 0;
	//insert entry into map with meta-data information (i.e. list of tuples for overflow page IDs)
	//but first check if there is an item inside the map corresponding to this bucket number
	if( _handle->_info->_overflowPageIds.find(bkt_number) != _handle->_info->_overflowPageIds.end() )
	{
		//check whether there are items
		if( _handle->_info->_overflowPageIds[bkt_number].size() > 0 )
		{
			//if there are then the new order is the last one + 1, or in another words the current size of the map
			newOrderValue = _handle->_info->_overflowPageIds[bkt_number].size();
		}
	}
	else
	{
		//insert an entry corresponding to this bucket number
		_handle->_info->_overflowPageIds.insert(
				std::pair<BUCKET_NUMBER, std::map<int, unsigned int> >(
						bkt_number, std::map<int, unsigned int>())
		);
	}

	//insert an entry
	_handle->_info->_overflowPageIds[bkt_number].insert( std::pair<int, unsigned int>(newOrderValue, dataPageId) );

	//success
	return errCode;
}

RC PFMExtension::shiftRecordsToEnd	//NOT TESTED
(const BUCKET_NUMBER bkt_number, const PageNum startingInPageNumber, const int startingFromSlotNumber, unsigned int szInBytes)
{
	RC errCode = 0;

	//allocate single slot
	void* singleSlotSpace = malloc(szInBytes);
	memset(singleSlotSpace, 0, szInBytes);

	//call recursive shifter
	if( (errCode = shiftRecursivelyToEnd(
			bkt_number, startingInPageNumber, startingFromSlotNumber, map<void*, unsigned int>())) != 0 )
	{
		free(singleSlotSpace);
		return errCode;
	}

	free(singleSlotSpace);

	//success
	return errCode;
}

RC PFMExtension::removePage(const BUCKET_NUMBER bkt_number, const PageNum pageNumber)
{
	RC errCode = 0;

	//remove record from the overflowPageId map
	if( _handle->_info->_overflowPageIds.find(bkt_number) == _handle->_info->_overflowPageIds.end() )
	{
		//no overflow page is found in the local map
		return -44;
	}

	//remove its record
	_handle->_info->_overflowPageIds[bkt_number].erase( pageNumber - 1 );

	return errCode;
}

RC PFMExtension::deleteTuple(const BUCKET_NUMBER bkt_number, const PageNum pageNumber, const int slotNumber, bool& lastPageIsEmpty)
{
	RC errCode = 0;

	if( (errCode = shiftRecordsToStart(bkt_number, pageNumber, slotNumber)) != 0 )
	{
		return errCode;
	}

	unsigned int lastPageNumber = _handle->_info->_overflowPageIds[bkt_number].size() - 1;

	_curVirtualPage = lastPageNumber;
	if( (errCode = getPage(bkt_number, _curVirtualPage, _buffer)) != 0 )
	{
		return errCode;
	}

	lastPageIsEmpty = (unsigned int*)((char*)_buffer + PAGE_SIZE - 2 * sizeof(unsigned int)) == 0;

	//success
	return errCode;
}

RC PFMExtension::numOfPages(const BUCKET_NUMBER bkt_number, unsigned int& numPages)
{
	RC errCode = 0;

	numPages = 1;

	//add number of overflow pages
	if( _handle->_info->_overflowPageIds.find(bkt_number) != _handle->_info->_overflowPageIds.end() )
	{
		numPages += _handle->_info->_overflowPageIds[bkt_number].size();
	}

	return errCode;
}

RC PFMExtension::getNumberOfEntriesInPage(const BUCKET_NUMBER bkt_number, const PageNum pageNumber, unsigned int& numEntries)
{
	RC errCode = 0;

	//if necessary read in the page
	if( pageNumber != _curVirtualPage )
	{
		//read data page
		if( (errCode = getPage(bkt_number, pageNumber, _buffer)) != 0 )
		{
			//read failed
			return errCode;
		}

		//update
		_curVirtualPage = pageNumber;
	}

	//get pointer to the end of directory slots
	PageDirSlot* startOfDirSlot = (PageDirSlot*)((char*)_buffer + PAGE_SIZE - 2 * sizeof(unsigned int));

	//find out number of directory slots
	numEntries = ((unsigned int*)startOfDirSlot)[0];

	//success
	return errCode;
}

RC PFMExtension::insertTuple(void* tupleData, unsigned int tupleLength, const BUCKET_NUMBER bkt_number,
		const PageNum pageNumber, const int slotNumber, bool& newPage)
{
	RC errCode = 0;

	map<void*, unsigned int> slotToInsert;

	slotToInsert.insert( std::pair<void*, unsigned int>(tupleData, tupleLength) );

	unsigned int numPagesInOverflowFile = _handle->_overBucketDataFileHandler.getNumberOfPages();

	if( pageNumber + 1 > _handle->_overBucketDataFileHandler.getNumberOfPages() )
	{
		//newPageIsCreated = true;

		//allocate page buffer
		void* dataPage = malloc(PAGE_SIZE);

		//add the page
		if( (errCode = addPage(dataPage, bkt_number)) != 0 )
		{
			free(dataPage);
			return errCode;
		}

		//deallocate buffer
		free(dataPage);
	}

	if( (errCode = shiftRecursivelyToEnd(
			bkt_number, pageNumber, slotNumber, slotToInsert)) != 0 )
	{
		return errCode;
	}

	if( numPagesInOverflowFile < _handle->_overBucketDataFileHandler.getNumberOfPages() )
	{
		newPage = true;
	}
	else
	{
		newPage = false;
	}

	//success
	return errCode;
}

//PFM EXTENSION CLASS METHODS -- END
MetaDataSortedEntries::MetaDataSortedEntries(IXFileHandle& ixfilehandle, BUCKET_NUMBER bucket_number, const Attribute& attr, void* key, const void* entry, unsigned int entryLength)
	:_ixfilehandle(&ixfilehandle),
	_bktNumber(bucket_number),
	_attr(attr),
	_key((void*)key),
	_entryData(entry),
	_curPageData(malloc(PAGE_SIZE)),
	_entryLength(entryLength),
	_curPageNum(0)
{
	pfme= new PFMExtension(ixfilehandle, _bktNumber);
}

MetaDataSortedEntries::~MetaDataSortedEntries()
{
	free(_curPageData);
}



RC MetaDataSortedEntries::searchEntry(RID& position, void* entry)
{
	RC errCode = 0;

	unsigned int numOfPages = 0;

	if( (errCode = pfme->numOfPages(_bktNumber, numOfPages)) != 0 )
	{
		return errCode;
	}

	bool success_flag = searchEntryInArrayOfPages(position, _curPageNum, numOfPages - 1);

	if( success_flag == false )
		return -50;

	//get the tuple from PFMExtension
	//if( (errCode = pfme.getTuple(entry, _bktNumber, position.pageNum, position.slotNum)) != 0 )
	//{
	//	return errCode;
	//}

	//success
	return 0;
}

//pageNumber is virtual
int MetaDataSortedEntries::searchEntryInPage(RID& result, const PageNum& pageNumber, const int indexStart, const int indexEnd)
{
	//binary search algorithm adopted from: Data Abstraction and Problem Solving with C++ (published 2005) by Frank Carrano, page 87

	if( indexStart > indexEnd )
	{
		result.pageNum = pageNumber;
		result.slotNum = indexStart == 0 ? indexStart : indexEnd;
		return indexStart == 0 ? -1 : 1;	//-1 means looking for item less than those presented in this page
											//+1 means looking for item greater than those presented in this page
	}

	int middle = (indexStart + indexEnd) / 2;

	//allocate buffer for storing the retrieved entry
	void* midValue = malloc(PAGE_SIZE);
	memset(midValue, 0, PAGE_SIZE);

	RC errCode = 0;

	//retrieve middle value
	if( (errCode = pfme->getTuple(midValue, _bktNumber, pageNumber, middle)) != 0 )
	{
		free(midValue);
		return errCode;
	}

	if( _key == midValue )
	{
		result.pageNum = (PageNum)_curPageNum;
		result.slotNum = middle;
	}
	else if( _key < midValue )
	{
		return searchEntryInPage(result, pageNumber, indexStart, middle - 1);
	}
	else
	{
		return searchEntryInPage(result, pageNumber, middle + 1, indexEnd);
	}

	//success
	return 0;	//0 means that the item is found in this page
}

//startPageNumber and endPageNumber are virtual page indexes
//(because it is possible for them to become less than zero, so type has to be kept as integer)
bool MetaDataSortedEntries::searchEntryInArrayOfPages(RID& position, const int startPageNumber, const int endPageNumber)
{
	bool success_flag = false;

	//idea of binary search is extended to a array of pages
	//binary search algorithm adopted from: Data Abstraction and Problem Solving with C++ (published 2005) by Frank Carrano, page 87

	//seeking range gets smaller with every iteration by a approximately half, and if there is no requested item, then we will get
	//range down to a zero, i.e. when end is smaller than the start. At this instance, return failure (i.e. false)
	if( startPageNumber > endPageNumber )
		return success_flag;

	//determine the middle entry inside this page
	PageNum middlePageNumber = (startPageNumber + endPageNumber) / 2;

	if( _curPageNum != (int)middlePageNumber )
	{
		_curPageNum = middlePageNumber;
		//load the middle page
		RC errCode = 0;
		if( (errCode = pfme->getPage(_bktNumber, _curPageNum, _curPageData)) != 0 )
		{
			IX_PrintError(errCode);
			exit(errCode);
		}
	}

    /*
	 * data page has a following format:
	 * [list of records without any spaces in between][free space for records][list of directory slots][(number of slots):unsigned int][(offset from page start to the start of free space):unsigned int]
	 * ^                                                                      ^                       ^                                                                                                 ^
	 * start of page                                                          start of dirSlot        end of dirSlot                                                                          end of page
	 */

	//get number of items stored in a page
	//first integer in a (overflow or primary) page represents number of items
	//second integer, whether the page is used or not (I have not fully incorporated isUsed in the algorithm right now, just reserved the space)
    unsigned int numSlots = ( (unsigned int*)( (char*)_curPageData + PAGE_SIZE - 2 * sizeof(unsigned int) ) )[0];

	//determine if the requested key is inside this page
	int result = searchEntryInPage(position, middlePageNumber, 0, numSlots - 1);

	//if it is inside this page, then return success
	if( result == 0 )
	{
		//match is found
		success_flag = true;
	}
	else if( result < 0 )
	{
		//if keys inside this page are too high, then check pages to the left
		//keep in mind that checking is not linear, but instead a current range
		//[start, end] is divided into a half [start, middle - 1] and then
		//this function is called recursively on this range of pages
		return searchEntryInArrayOfPages(position, startPageNumber, middlePageNumber - 1);
	}
	else
	{
		//if keys inside this page are too low, then check pages to the right
		//similar idea is used over here, except new range is [middle + 1, end]
		return searchEntryInArrayOfPages(position, middlePageNumber + 1, endPageNumber);
	}

	//success
	return success_flag;
}





void MetaDataSortedEntries::insertEntry()
{
	RID position = (RID){0, 0};
	RC errCode = 0;

	unsigned int maxPages = 0;

	if( (errCode = pfme->numOfPages(_bktNumber, maxPages)) != 0 )
	{
		IX_PrintError(errCode);
		exit(errCode);
	}

	//find the position in the array of pages (primary and overflow) where requested item needs to be inserted
	searchEntryInArrayOfPages(position, _curPageNum, maxPages - 1);

	_curPageNum = position.pageNum;

	//"allocate space for new entry" by shifting the data (to the "right" of the found position) by a size of of meta-data entry

	//since there could be duplicates, we need to find the "right-most" entry with this data
	unsigned int numOfEntriesInPage = ((unsigned int*)_curPageData)[0];
	if( position.slotNum < numOfEntriesInPage )	//of course, providing that this page has some entries
	{
		//keep looping while finding duplicates (i.e. entries with the same key, with the assumption of different RIDs)
		BucketDataEntry entry;
		if((errCode=pfme->getTuple(&entry, _bktNumber, position.pageNum, position.slotNum))!=0){
			IX_PrintError(errCode);
			exit(errCode);
		}
		while( entry._key <= _key )
		{
			position.slotNum++;
			//if we go beyond the page boundaries then, go to the next page
			if( position.slotNum >= numOfEntriesInPage )
			{
				//check if next page exists
				if( position.pageNum + 1 >= (unsigned int)maxPages )
				{
					break;
				}

				//increment to next page
				position.pageNum++;
				_curPageNum = position.pageNum;

				//reset slot number
				position.slotNum = 0;

				//read in the page
				if( (errCode = pfme->getPage(_bktNumber, _curPageNum, _curPageData)) != 0 )
				{
					IX_PrintError(errCode);
					exit(errCode);
				}

				//reset number of entries in the page
				numOfEntriesInPage = ((unsigned int*)_curPageData)[0];
			}

			if((errCode = pfme->getTuple(&entry, _bktNumber, position.pageNum, position.slotNum))!=0){
				IX_PrintError(errCode);
				exit(errCode);
			}
		}
	}

	bool newPage;
	//with the final position call insertTuple (PFMExtension)
	if( (errCode = pfme->insertTuple( (void *)_entryData, _entryLength,_bktNumber, position.pageNum, position.slotNum, newPage)) != 0 )
	{
		IX_PrintError(errCode);
		exit(errCode);
	}

	// if new page was added, perform split
	if( newPage )
	{


		//process split
		_bktNumber = _ixfilehandle->_info->Next;
		memset(_curPageData, 0, PAGE_SIZE);
		_curPageNum = 0;
		if( (errCode = splitBucket()) != 0 )
		{
			IX_PrintError(errCode);
			exit(errCode);
		}


		//check if we need to increment level
		if( _ixfilehandle->_info->Next == _ixfilehandle->_info->N * (int)pow(2.0, (int)_ixfilehandle->_info->Level) )
		{
			_ixfilehandle->_info->Level++;
			_ixfilehandle->_info->Next = 0;
		}
		else
		{
			_ixfilehandle->_info->Next++;
		}

		//update IX header and fileHeader info
		if( (errCode = _ixfilehandle->_metaDataFileHandler.readPage(1, _curPageData)) != 0 )
		{
			//return error code
			IX_PrintError(errCode);
			exit(errCode);
		}

		((unsigned int*)_curPageData)[1] = _ixfilehandle->_info->Level;
		((unsigned int*)_curPageData)[2] = _ixfilehandle->_info->Next;

		if( (errCode = _ixfilehandle->_metaDataFileHandler.writePage(1, _curPageData)) != 0 )
		{
			//return error code
			IX_PrintError(errCode);
			exit(errCode);
		}
	}
}

RC MetaDataSortedEntries::deleteEntry(const RID& rid)
{
	RC errCode = 0;

	RID position = (RID){0, 0};

	unsigned int maxPages = 0;

	if( (errCode = pfme->numOfPages(_bktNumber, maxPages)) != 0 )
	{
		return errCode;
	}

	//find position of this item
	if( searchEntryInArrayOfPages(position, _curPageNum, maxPages - 1) == false )
	{
		return -43;	//attempting to delete index-entry that does not exist
	}

	_curPageNum = position.pageNum;

	//linearly search among duplicates until a requested item is found
	unsigned int numOfEntriesInPage = ((unsigned int*)_curPageData)[0];
	if( position.slotNum < numOfEntriesInPage )	//of course, providing that this page has some entries
	{
		//in the presence of duplicates, we may arrive at any random spot within the list duplicates tuples (i.e. tuples with the same key)
		//but in order to make sure that the item exists or does not exist, we need to scan the whole list of duplicates

		//first, however, need to find the start of the duplicate list
		while(true)
		{
			//check if the current item has a different key (less than the key of interest)
			BucketDataEntry entry;
			if((errCode=pfme->getTuple(&entry, _bktNumber, position.pageNum, position.slotNum))!=0)
			{
				IX_PrintError(errCode);
				exit(errCode);
			}
			if( entry._key < _key )
			{
				//if so, then quit => start is found
				break;
			}

			//check if the next index is outside of the page
			if( position.slotNum == 0 )
			{
				//check if this is a primary page
				if( position.pageNum == 0 )
				{
					//if so, there are no pages in front of it => start is found
					break;
				}
				//otherwise, go to the previous page
				position.pageNum--;
				_curPageNum = position.pageNum;

				//reset slot number
				//position.slotNum = MAX_BUCKET_ENTRIES_IN_PAGE - 1;

				//read in the page
				if( (errCode = pfme->getPage(_bktNumber, _curPageNum, _curPageData)) != 0 )
				{
					IX_PrintError(errCode);
					exit(errCode);
				}

				continue;
			}

			//decrement index
			position.slotNum--;
		}

		//now linearly scan thru the list of duplicates until either:
		//	1. item is found
		//	2. or, items with the given key are exhausted => no specified item exists => fail
		BucketDataEntry *entry;
		if((errCode=pfme->getTuple(&entry, _bktNumber, position.pageNum, position.slotNum))!=0)
		{
			IX_PrintError(errCode);
			exit(errCode);
		}
		while(entry->_key < _key || (entry->_key == _key &&	(entry->_rid.pageNum != rid.pageNum ||	entry->_rid.slotNum != rid.slotNum	) ) )
		{
			position.slotNum++;
			//if we go beyond the page boundaries then, go to the next page
			if( position.slotNum >= numOfEntriesInPage )
			{
				//check if next page exists
				if( position.pageNum + 1 >= (unsigned int)maxPages )
				{
					break;
				}

				//increment to next page
				position.pageNum++;
				_curPageNum = position.pageNum;

				//reset slot number
				position.slotNum = 0;

				//read in the page
				if( (errCode = pfme->getPage(_bktNumber, _curPageNum, _curPageData)) != 0 )
				{
					IX_PrintError(errCode);
					exit(errCode);
				}

				//reset number of entries in the page
				numOfEntriesInPage = ((unsigned int*)_curPageData)[0];
			}

			if((errCode=pfme->getTuple(entry, _bktNumber, position.pageNum, position.slotNum))!=0)
			{
				IX_PrintError(errCode);
				exit(errCode);
			}
		}

		//check that the item is found (it should be pointed now by slotNum)
		if( entry->_rid.pageNum != rid.pageNum || entry->_rid.slotNum != rid.slotNum )
		{
			//if it is not the item, then it means we have looped thru all duplicate entries and still have not found the right one with given RID
			return -43;	//attempting to delete index-entry that does not exist
		}
	}
	else
	{
		//again, item to be deleted is not found
		return -43;
	}


	//delete entry from PFMExtension by shifting
	bool lastPageIsEmpty;
	if((errCode=pfme->deleteTuple(_bktNumber, position.pageNum, position.slotNum, lastPageIsEmpty))!=0)
	{
		IX_PrintError(errCode);
		exit(errCode);
	}


	if( lastPageIsEmpty )
	{
		//then perform merge
		_curPageNum = maxPages - 1;
		if( _curPageNum > 0 )
		{
			//if it happens to be the overflow page, then remove record about it from the overflowPageId map and the overflow PFM header
			if( (errCode = pfme->removePage(_bktNumber, _curPageNum)) != 0 )
			{
				return errCode;
			}
		}

		unsigned int savedBucketNumber = _bktNumber;

		if( _ixfilehandle->_info->Next == 0 )
		{
			_ixfilehandle->_info->Next =
					(unsigned int)( _ixfilehandle->_info->N * (unsigned int)pow(2.0, (int)(_ixfilehandle->_info->Level - 1)) );//- 1 );	//-1 will be taken out later
			_ixfilehandle->_info->Level--;
		}

		//if it is a last primary bucket, then "merge it with its image"
		if( _ixfilehandle->_info->Next > 0 )
		{
			_ixfilehandle->_info->Next--;
		}

		//process merge
		_bktNumber = _ixfilehandle->_info->Next;
		_curPageNum = 0;
		memset(_curPageData, 0, PAGE_SIZE);
		if( (errCode = mergeBuckets(_bktNumber)) != 0 )
		{
			return errCode;
		}

		//update IX header and fileHeader info
		if( (errCode = _ixfilehandle->_metaDataFileHandler.readPage(1, _curPageData)) != 0 )
		{
			//return error code
			IX_PrintError(errCode);
			exit(errCode);
		}

		((unsigned int*)_curPageData)[1] = _ixfilehandle->_info->Level;
		((unsigned int*)_curPageData)[2] = _ixfilehandle->_info->Next;

		if( (errCode = _ixfilehandle->_metaDataFileHandler.writePage(1, _curPageData)) != 0 )
		{
			//return error code
			IX_PrintError(errCode);
			exit(errCode);
		}

		//remove overflow pages for the image of the merged bucket
		_bktNumber = _ixfilehandle->_info->Next + _ixfilehandle->N_Level();

		//re-determine number of pages
		if( (errCode = pfme->numOfPages(_bktNumber, maxPages)) != 0 )
		{
			return errCode;
		}

		_curPageNum = -1;
		for( int i = 1; i < (int)maxPages; i++ )
		{
			_curPageNum = i;

			if( (errCode = pfme->removePage(_bktNumber, _curPageNum)) != 0 )
			{
				return errCode;
			}
		}

		//erase higher bucket's primary page
		/*_curPageNum = _bktNumber + 1;
		if( (errCode = erasePageFromHeader(_ixfilehandle->_primBucketDataFileHandler)) != 0 )
		{
			//return error code
			IX_PrintError(errCode);
			exit(errCode);
		}*/

		//restore bucket number
		_bktNumber = savedBucketNumber;
	}

	//success
	return errCode;
}

// implement with the new aproach
RC MetaDataSortedEntries::splitBucket()
{
	int errCode=0;

	return errCode;
}

// implement with the new aproach
RC MetaDataSortedEntries::mergeBuckets(BUCKET_NUMBER lowBucket)
{
	int errCode=0;

	return errCode;
}

//_curPageNum should be physical page number not virtual inside this function
/*RC MetaDataSortedEntries::erasePageFromHeader(FileHandle& fileHandle)
{
	//find header page that contains the record about the data page to be removed
	PageNum headerPageId = 0;

	RC errCode = 0;

	//pointer to the header page
	Header* hPage = NULL;

	//allocate temporary buffer for page
	void* data = malloc(PAGE_SIZE);

	bool found = false;

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

		//loop thru PageInfo tuples
		for( unsigned int i = 0; i < NUM_OF_PAGE_IDS; i++ )
		{
			if( hPage->_arrOfPageIds[i]._pageid == (unsigned int)_curPageNum )
			{
				//found the page record
				//remove record from the PFM page header
				hPage->_arrOfPageIds[i]._numFreeBytes = (unsigned int)-1;
				found = true;
				//decrease number of pages in the file
				//_ixfilehandle._overBucketDataFileHandler._info->_numPages--;	//statement deleted, caused to error at allocation of new page,
																				//since it was considering wrong number of pages inside the file
				break;
			}
		}

		if( found )
			break;

		//go to next header page
		headerPageId = hPage->_nextHeaderPageId;

	} while(headerPageId > 0);

	if( found == false )
	{
		//have not found the appropriate page
		return -45;
	}

	//write back the header page
	if( (errCode = fileHandle.writePage((PageNum)headerPageId, data)) != 0 )
	{
		//deallocate data
		free(data);

		//return error
		return errCode;
	}

	//now, go ahead and null the contents of the erased page
	memset(data, 0, PAGE_SIZE);
	if( (errCode = fileHandle.writePage((PageNum)_curPageNum, data)) != 0 )
	{
		//deallocate data
		free(data);

		//return error
		return errCode;
	}

	//deallocate temporary buffer for header page
	free(data);

	//success
	return errCode;
}
*/

/*
//functions for the SortedEntries class
MetaDataSortedEntries::MetaDataSortedEntries(
		IXFileHandle& ixfilehandle, BUCKET_NUMBER bucket_number, const Attribute& attr, const void* key, const void* entry)
: _ixfilehandle(ixfilehandle), _key(key), _attr(attr), _entryData(entry), _curPageNum(0), _curPageData(malloc(PAGE_SIZE)),
  _bktNumber(bucket_number)
{
	getPage();
}

//_curPageNum should be physical page number not virtual inside this function
RC MetaDataSortedEntries::erasePageFromHeader(FileHandle& fileHandle)
{
	//find header page that contains the record about the data page to be removed
	PageNum headerPageId = 0;

	RC errCode = 0;

	//pointer to the header page
	Header* hPage = NULL;

	//allocate temporary buffer for page
	void* data = malloc(PAGE_SIZE);

	bool found = false;

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

		//loop thru PageInfo tuples
		for( unsigned int i = 0; i < NUM_OF_PAGE_IDS; i++ )
		{
			if( hPage->_arrOfPageIds[i]._pageid == (unsigned int)_curPageNum )
			{
				//found the page record
				//remove record from the PFM page header
				hPage->_arrOfPageIds[i]._numFreeBytes = (unsigned int)-1;
				found = true;
				//decrease number of pages in the file
				//_ixfilehandle._overBucketDataFileHandler._info->_numPages--;	//statement deleted, caused to error at allocation of new page,
																				//since it was considering wrong number of pages inside the file
				break;
			}
		}

		if( found )
			break;

		//go to next header page
		headerPageId = hPage->_nextHeaderPageId;

	} while(headerPageId > 0);

	if( found == false )
	{
		//have not found the appropriate page
		return -45;
	}

	//write back the header page
	if( (errCode = fileHandle.writePage((PageNum)headerPageId, data)) != 0 )
	{
		//deallocate data
		free(data);

		//return error
		return errCode;
	}

	//now, go ahead and null the contents of the erased page
	memset(data, 0, PAGE_SIZE);
	if( (errCode = fileHandle.writePage((PageNum)_curPageNum, data)) != 0 )
	{
		//deallocate data
		free(data);

		//return error
		return errCode;
	}

	//deallocate temporary buffer for header page
	free(data);

	//success
	return errCode;
}*/

//assuming that _curPageNum is a virtual page number
/*RC MetaDataSortedEntries::removePageRecord()
{
	//remove record from the overflowPageId map
	if( _ixfilehandle._info->_overflowPageIds.find(_bktNumber) == _ixfilehandle._info->_overflowPageIds.end() )
	{
		//no overflow page is found in the local map
		return -44;
	}

	//determine physical page number of the one to be removed
	PageNum physPageNumber = _ixfilehandle._info->_overflowPageIds[_bktNumber][_curPageNum - 1];

	//remove its record
	_ixfilehandle._info->_overflowPageIds[_bktNumber].erase( _curPageNum - 1 );

	//assign a physical page number to _curPageNum
	_curPageNum = physPageNumber;

	//std::map<int, PageNum>::iterator
	//	i = _ixfilehandle._info->_overflowPageIds[_bktNumber].begin(),
	//	max = _ixfilehandle._info->_overflowPageIds[_bktNumber].end();

	//for( ; i != max; i++ )
	//{
	//	if( i->second == (unsigned int)_curPageNum )
	//	{
	//		_ixfilehandle._info->_overflowPageIds[_bktNumber].erase(i);
	//		break;
	//	}
	//}

	erasePageFromHeader(_ixfilehandle._overBucketDataFileHandler);

	//success
	return 0;
}*/

/*void MetaDataSortedEntries::addPage()
{
	RC errCode = 0;

	PagedFileManager* _pfm = PagedFileManager::instance();

	unsigned int headerPageId = 0, dataPageId = 0;
	//insert page into overflow data file
	if( (errCode = _pfm->getLastHeaderPage(_ixfilehandle._overBucketDataFileHandler, headerPageId)) != 0 )
	{
		IX_PrintError(errCode);
		exit(errCode);
	}

	unsigned int freeSpaceLeft = 0;

	if( (errCode = _pfm->getDataPage(_ixfilehandle._overBucketDataFileHandler, (unsigned int)-1, dataPageId, headerPageId, freeSpaceLeft)) != 0 )
	{
		IX_PrintError(errCode);
		exit(errCode);
	}
	if( (errCode = _ixfilehandle._overBucketDataFileHandler.writePage(dataPageId, _curPageData)) != 0 )
	{
		IX_PrintError(errCode);
		exit(errCode);
	}

	int newOrderValue = 0;
	//insert entry into map with meta-data information (i.e. list of tuples for overflow page IDs)
	//but first check if there is an item inside the map corresponding to this bucket number
	if( _ixfilehandle._info->_overflowPageIds.find(_bktNumber) != _ixfilehandle._info->_overflowPageIds.end() )
	{
		//check whether there are items
		if( _ixfilehandle._info->_overflowPageIds[_bktNumber].size() > 0 )
		{
			//if there are then the new order is the last one + 1, or in another words the current size of the map
			newOrderValue = _ixfilehandle._info->_overflowPageIds[_bktNumber].size();
		}
	}
	else
	{
		//insert an entry corresponding to this bucket number
		_ixfilehandle._info->_overflowPageIds.insert( std::pair<BUCKET_NUMBER, std::map<int, unsigned int> >(_bktNumber, std::map<int, unsigned int>()) );
	}

	//insert an entry
	_ixfilehandle._info->_overflowPageIds[_bktNumber].insert( std::pair<int, unsigned int>(newOrderValue, dataPageId) );
}*/
/*
RC MetaDataSortedEntries::getPage()
{
	RC errCode = 0;

	PageNum actualPageNumber = _bktNumber + 1;	//setup by default to point at primary page
												//primary file has first page reserved for PFM header, so bucket # 0 starts at page # 1, which
												//is why actualPageNumber is bucket number + 1

	//current page number is a virtual page index, it goes from 0 and up (continuously)
	//in reality, pages are spread thru files:
	//	1. primary page (page index = 0) is inside the primary file
	//	2. other pages (with page index > 0) are inside the overflow file

	if( _curPageNum > 0 )	//if inside the overflow file
	{
		//then, consult with information about overflow page locations stored inside the file handler to
		//determine which page to load

		PageNum overFlowPageId = _curPageNum - 1;

		//first check whether the page exists inside overflow file (i.e. is virtual page points beyond the file)
		if( overFlowPageId >= _ixfilehandle._info->_overflowPageIds[_bktNumber].size() )
		{
			return -42;	//accessing a page beyond bucket's data
		}

		//get physical overflow page number corresponding to the "virtual page number"
		actualPageNumber = _ixfilehandle._info->_overflowPageIds[_bktNumber][overFlowPageId];

		//retrieve the data and store in the current buffer
		if( (errCode = _ixfilehandle._overBucketDataFileHandler.readPage(actualPageNumber, _curPageData)) != 0 )
		{
			return errCode;
		}
	}
	else
	{
		//then, inside the "primary page"
		//read the page from the primary file
		if( (errCode = _ixfilehandle._primBucketDataFileHandler.readPage(actualPageNumber, _curPageData)) != 0 )
		{
			return errCode;
		}
	}

	return errCode;
}

unsigned int MetaDataSortedEntries::numOfPages()
{
	unsigned int result = 1;

	//add number of overflow pages
	if( _ixfilehandle._info->_overflowPageIds.find(_bktNumber) != _ixfilehandle._info->_overflowPageIds.end() )
	{
		result += _ixfilehandle._info->_overflowPageIds[_bktNumber].size();
	}

	return result;
}

MetaDataSortedEntries::~MetaDataSortedEntries()
{
	free(_curPageData);
}

RC MetaDataSortedEntries::translateToPageNumber(const PageNum& pagenumber, PageNum& result)
{
	result = _bktNumber + 1;	//setup by default to point at primary page
								//primary file has first page reserved for PFM header, so bucket # 0 starts at page # 1, which
								//is why actualPageNumber is bucket number + 1

	if( _curPageNum > 0 )	//if inside the overflow file
	{
		//then, consult with information about overflow page locations stored inside the file handler to
		//determine which page to load

		PageNum overFlowPageId = _curPageNum - 1;

		//first check whether the page exists inside overflow file (i.e. is virtual page points beyond the file)
		if( overFlowPageId >= _ixfilehandle._info->_overflowPageIds[_bktNumber].size() )
		{
			return -42;	//accessing a page beyond bucket's data
		}

		//get physical overflow page number corresponding to the "virtual page number"
		result = _ixfilehandle._info->_overflowPageIds[_bktNumber][overFlowPageId];
	}
}

//pageNumber is virtual
RC MetaDataSortedEntries::getTuple(const PageNum pageNumber, const unsigned int slotNumber, void* entry)
{
	RC errCode = 0;

	vector<Attribute> descriptor;
	descriptor.push_back(_attr);

	//determine the page number, since position.pageNum is a "virtual page index"
	PageNum actualPageNumber = 0;
	if( (errCode = translateToPageNumber(pageNumber, actualPageNumber)) != 0 )
	{
		return errCode;
	}

	FileHandle handle;

	//determine which handle to use to read the page
	if( pageNumber == 0 )
	{
		handle = _ixfilehandle._primBucketDataFileHandler;
	}
	else
	{
		handle = _ixfilehandle._overBucketDataFileHandler;
	}

	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();

	//copy the result into entry buffer
	if( (errCode = rbfm->readRecord(handle, descriptor, (RID){actualPageNumber, slotNumber}, entry)) != 0 )
	{
		return errCode;
	}

	//success
	return errCode;
}

RC MetaDataSortedEntries::searchEntry(RID& position, void* entry)
{
	bool success_flag = searchEntryInArrayOfPages(position, _curPageNum, numOfPages() - 1);

	if( success_flag == false )
		return -50;

	RC errCode = 0;

	if( (errCode = getTuple(position.pageNum, position.slotNum, entry)) != 0 )
	{
		return errCode;
	}

	//success
	return 0;
}

//pageNumber is virtual
int MetaDataSortedEntries::searchEntryInPage(RID& result, const PageNum& pageNumber, const int indexStart, const int indexEnd)
{
	//binary search algorithm adopted from: Data Abstraction and Problem Solving with C++ (published 2005) by Frank Carrano, page 87

	if( indexStart > indexEnd )
	{
		result.pageNum = pageNumber;
		result.slotNum = indexStart == 0 ? indexStart : indexEnd;
		return indexStart == 0 ? -1 : 1;	//-1 means looking for item less than those presented in this page
											//+1 means looking for item greater than those presented in this page
	}

	int middle = (indexStart + indexEnd) / 2;

	//allocate buffer for storing the retrieved entry
	void* midValue = malloc(PAGE_SIZE);
	memset(midValue, 0, PAGE_SIZE);

	RC errCode = 0;

	//retrieve middle value
	if( (errCode = getTuple(pageNumber, middle, midValue)) != 0 )
	{
		free(midValue);
		return errCode;
	}

	if( _key == midValue )
	{
		result.pageNum = (PageNum)_curPageNum;
		result.slotNum = middle;
	}
	else if( _key < midValue )
	{
		return searchEntryInPage(result, pageNumber, indexStart, middle - 1);
	}
	else
	{
		return searchEntryInPage(result, pageNumber, middle + 1, indexEnd);
	}

	//success
	return 0;	//0 means that the item is found in this page
}

//startPageNumber and endPageNumber are virtual page indexes
//(because it is possible for them to become less than zero, so type has to be kept as integer)
bool MetaDataSortedEntries::searchEntryInArrayOfPages(RID& position, const int startPageNumber, const int endPageNumber)
{
	bool success_flag = false;

	//idea of binary search is extended to a array of pages
	//binary search algorithm adopted from: Data Abstraction and Problem Solving with C++ (published 2005) by Frank Carrano, page 87

	//seeking range gets smaller with every iteration by a approximately half, and if there is no requested item, then we will get
	//range down to a zero, i.e. when end is smaller than the start. At this instance, return failure (i.e. false)
	if( startPageNumber > endPageNumber )
		return success_flag;

	//determine the middle entry inside this page
	PageNum middlePageNumber = (startPageNumber + endPageNumber) / 2;

	if( _curPageNum != (int)middlePageNumber )
	{
		_curPageNum = middlePageNumber;
		//load the middle page
		RC errCode = 0;
		if( (errCode = getPage()) != 0 )
		{
			IX_PrintError(errCode);
			exit(errCode);
		}
	}

    //
	// data page has a following format:
	// [list of records without any spaces in between][free space for records][list of directory slots][(number of slots):unsigned int][(offset from page start to the start of free space):unsigned int]
	// ^                                                                      ^                       ^                                                                                                 ^
	// start of page                                                          start of dirSlot        end of dirSlot                                                                          end of page
	//

	//get number of items stored in a page
	//first integer in a (overflow or primary) page represents number of items
	//second integer, whether the page is used or not (I have not fully incorporated isUsed in the algorithm right now, just reserved the space)
    unsigned int numSlots = ( (unsigned int*)( (char*)_curPageData + PAGE_SIZE - 2 * sizeof(unsigned int) ) )[0];

	//determine if the requested key is inside this page
	int result = searchEntryInPage(position, middlePageNumber, 0, numSlots - 1);

	//if it is inside this page, then return success
	if( result == 0 )
	{
		//match is found
		success_flag = true;
	}
	else if( result < 0 )
	{
		//if keys inside this page are too high, then check pages to the left
		//keep in mind that checking is not linear, but instead a current range
		//[start, end] is divided into a half [start, middle - 1] and then
		//this function is called recursively on this range of pages
		return searchEntryInArrayOfPages(position, startPageNumber, middlePageNumber - 1);
	}
	else
	{
		//if keys inside this page are too low, then check pages to the right
		//similar idea is used over here, except new range is [middle + 1, end]
		return searchEntryInArrayOfPages(position, middlePageNumber + 1, endPageNumber);
	}

	//success
	return success_flag;
}

RC MetaDataSortedEntries::mergeBuckets()
{
	RC errCode = 0;

	//[            ]........[            ]
	//[  bucket k  ]........[ bucket k+N ]
	//[            ]........[            ]
	//      |                     |
	//      v                     v
	//[1,7,19,22,23]		[2,3,4,5,6   ]
	//                      [100,101,200 ]

	//perform a 1-pass merge page-by-page, since we are merging two sorted arrays

	//[1,7,19,22,23] []
	//[2,3,4,5,6]    [100,101,200]
	//[1,2,3,4,5,6] => [7,19,22,23,100] => [101,200]

	//indexes for page
	int low_curPage = 0, high_curPage = 0,
		low_max = 1 + (int)_ixfilehandle._info->_overflowPageIds[_bktNumber].size(),
		high_max = 1 + (int)_ixfilehandle._info->_overflowPageIds[_bktNumber + _ixfilehandle.N_Level()].size();

	//buffers for pages
	void* data[2];
	data[0] = malloc(PAGE_SIZE);
	data[1] = malloc(PAGE_SIZE);

	//clear up buffers
	memset(data[0], 0, PAGE_SIZE);
	memset(data[1], 0, PAGE_SIZE);

	//setup indexes for looping inside the pages
	int low_entryIndex = 0, high_entryIndex = 0,
		low_maxIndex = -1,
		high_maxIndex = -1;

	//collect output pages
	std::vector<char*> outputPageList;

	unsigned int bucketIds[2] = { _bktNumber, _bktNumber + _ixfilehandle.N_Level() };

	//iterate over the two buckets
	do
	{
		//read in the pages
		for( int i = 0; i < 2; i++ )
		{
			//check whether a new page is necessary
			if( (i == 0 && low_entryIndex < low_maxIndex) ||
				(i == 1 && high_entryIndex < high_maxIndex) )
				continue;

			//getPage uses _curPageNum to denote index of required page
			if( i == 0 )
			{
				_curPageNum = low_curPage;
				low_curPage++;
			}
			else
			{
				_curPageNum = high_curPage;
				high_curPage++;
			}

			//assign bucket id
			_bktNumber = bucketIds[i];

			//if one bucket gets exhausted, then keep looping thru the second
			if( low_curPage > low_max || high_curPage > high_max )
			{
				continue;
			}

			//load the page
			if( (errCode = getPage()) != 0 )
			{
				free(data[0]);
				free(data[1]);
				return errCode;
			}

			//left and right pages are maintained in the respective buffers
			memcpy( data[i], _curPageData, PAGE_SIZE );
		}

		//check if page counters are within the bucket boundaries
		if( low_curPage > low_max && high_curPage > high_max )
		{
			//if both are beyond, then quit
			break;
		}

		//setup maximum entries inside the page
		if( low_curPage <= low_max )
			low_maxIndex = (int)((unsigned int*)data[0])[0];
		else
		{
			low_maxIndex = MAX_BUCKET_ENTRIES_IN_PAGE + 1;
			low_entryIndex = 0;
		}
		if( high_curPage <= high_max )
			high_maxIndex = (int)((unsigned int*)data[1])[0];
		else
		{
			high_maxIndex = MAX_BUCKET_ENTRIES_IN_PAGE + 1;
			high_entryIndex = 0;
		}

		void* outputData = malloc(PAGE_SIZE);
		memset(outputData, 0, PAGE_SIZE);

		//iterate thru two pages (side-by-side) and compose the merged version
		int i = 0;
		int max = (low_maxIndex == 0 && high_maxIndex == 0 ? 0 : MAX_BUCKET_ENTRIES_IN_PAGE);
		for( ; i < max; i++ )
		{
			BucketDataEntry *low_entry, *high_entry;
			//setup pointers to the entries
			if( low_entryIndex < low_maxIndex )
				low_entry = (BucketDataEntry*)((char*)data[0] + 2 * sizeof(unsigned int) + low_entryIndex * SZ_OF_BUCKET_ENTRY);
			else
				low_entry = NULL;
			if( high_entryIndex < high_maxIndex )
				high_entry = (BucketDataEntry*)((char*)data[1] + 2 * sizeof(unsigned int) + high_entryIndex * SZ_OF_BUCKET_ENTRY);
			else
				high_entry = NULL;

			//compare two entries and copy the smallest entry to the result set
			if( low_curPage <= low_max && high_curPage <= high_max )
			{
				if( low_entry != NULL && high_entry != NULL )
				{
					if( low_entry->_key <= high_entry->_key )
					{
						memcpy
						(
							(BucketDataEntry*)((char*)outputData + 2 * sizeof(unsigned int) + i * (int)SZ_OF_BUCKET_ENTRY),
							low_entry,
							SZ_OF_BUCKET_ENTRY
						);
						low_entryIndex++;
					}
					else
					{
						memcpy
						(
							(BucketDataEntry*)((char*)outputData + 2 * sizeof(unsigned int) + i * (int)SZ_OF_BUCKET_ENTRY),
							high_entry,
							SZ_OF_BUCKET_ENTRY
						);
						high_entryIndex++;
					}
				}
				else if( low_entry != NULL )
				{
					memcpy
					(
						(BucketDataEntry*)((char*)outputData + 2 * sizeof(unsigned int) + i * (int)SZ_OF_BUCKET_ENTRY),
						low_entry,
						SZ_OF_BUCKET_ENTRY
					);
					low_entryIndex++;
				}
				else
				{
					memcpy
					(
						(BucketDataEntry*)((char*)outputData + 2 * sizeof(unsigned int) + i * (int)SZ_OF_BUCKET_ENTRY),
						high_entry,
						SZ_OF_BUCKET_ENTRY
					);
					high_entryIndex++;
				}
			}
			else if( low_curPage <= low_max )
			{
				//low is still needs to be transfered
				memcpy
				(
					(BucketDataEntry*)((char*)outputData + 2 * sizeof(unsigned int) + i * (int)SZ_OF_BUCKET_ENTRY),
					low_entry,
					SZ_OF_BUCKET_ENTRY
				);
				low_entryIndex++;
			}
			else
			{
				//high is still needs to be transfered
				memcpy
				(
					(BucketDataEntry*)((char*)outputData + 2 * sizeof(unsigned int) + i * (int)SZ_OF_BUCKET_ENTRY),
					high_entry,
					(unsigned int)SZ_OF_BUCKET_ENTRY
				);
				high_entryIndex++;
			}

			//check if low/high index is within boundaries
			if( low_entryIndex >= low_maxIndex && high_entryIndex >= high_maxIndex )
			{
				i++;
				break;
			}
		}

		//write number of entries
		((unsigned int*)outputData)[0] = i;
		//collect output page
		outputPageList.push_back((char*)outputData);

	} while( low_curPage <= low_max && high_curPage <= high_max );

	_curPageNum = 0;
	_bktNumber = bucketIds[0];

	//write list of output pages to the appropriate bucket AND free the list of pages
	for( int k = 0; k < (int)outputPageList.size(); k++ )
	{
		//check that the page exists
		if( _curPageNum >= low_max )
		{
			addPage();
			low_max++;
		}

		void* ptr = (void*)outputPageList[k];

		//write page to the bucket
		if( _curPageNum == 0 )
		{
			//write page to "primary file"
			if( (errCode = _ixfilehandle._primBucketDataFileHandler.writePage(_bktNumber + 1, ptr)) != 0 )
			{
				IX_PrintError(errCode);
				exit(errCode);
			}
		}
		else
		{
			//write page to "overflow file"

			//determine physical page number
			PageNum actualPageNumber = _ixfilehandle._info->_overflowPageIds[_bktNumber][_curPageNum - 1];

			if( (errCode = _ixfilehandle._overBucketDataFileHandler.writePage(actualPageNumber, ptr)) != 0 )
			{
				IX_PrintError(errCode);
				exit(errCode);
			}
		}

		//free page
		free(ptr);
		_curPageNum++;
	}

	free(data[0]);
	free(data[1]);

	//success
	return errCode;
}

RC MetaDataSortedEntries::splitBucket()
{
	RC errCode = 0;

	int numberOfPages = (int)numOfPages();

	//assumption is that the _bktNumber is lower bucket, while the higher is the
	//one that got created and is participating in re-distribution of elements
	unsigned int higherBucketNumber = _bktNumber + _ixfilehandle.N_Level();

	//maintain two maps for two buckets, among which the data must be spread
	std::vector< std::vector< BucketDataEntry > > buckets[2];

	//reset
	_curPageNum = -1;

	//loop thru pages on the bucket
	for( int pageIndex = 0; pageIndex < numberOfPages; pageIndex++ )
	{
		//get the page <i> if it is not already read-in the buffer
		if( pageIndex != _curPageNum )
		{
			//update current page number
			_curPageNum = pageIndex;

			//read in the page
			if( (errCode = getPage()) != 0 )
			{
				IX_PrintError(errCode);
				exit(errCode);
			}
		}

		//determine number of entries in a current page
		int numEntriesInPage = (int)( ((unsigned int*)_curPageData)[0] );

		//over-write number of entries in the page
		//((unsigned int*)_curPageData)[0] = 0;
		//may need to declare buckets as not used here (isUsed can be set to zero, but need to modify code that allocates pages, i.e. setting isUsed to one there)

		//loop thru the entries of the page and apply hash function (Level + 1) to determine to which bucket does the current entry belongs to
		for( int entryIndex = 0; entryIndex < numEntriesInPage; entryIndex++ )
		{
			//get pointer to this entry
			BucketDataEntry* ptrOfEntry = (BucketDataEntry*)( (char*)_curPageData + 2 * sizeof(unsigned int) + entryIndex * SZ_OF_BUCKET_ENTRY );

			//apply a hash function with LEVEL + 1 to determine hashed key
			unsigned int hashedKey = IndexManager::instance()->hash_at_specified_level( _ixfilehandle._info->N, _ixfilehandle._info->Level + 1, ptrOfEntry->_key );

			unsigned int key = ptrOfEntry->_key;

			int bucketIndex = 0;
			if( hashedKey == _bktNumber )
			{
				buckets[0].push_back( std::vector< BucketDataEntry >() );
				bucketIndex = buckets[0].size() - 1;
			}
			else
			{
				buckets[1].push_back( std::vector< BucketDataEntry >() );
				bucketIndex = buckets[1].size() - 1;
			}

			//now identify which bucket is pointed by this hashed key
			while( ptrOfEntry->_key == key )
			{
				if( hashedKey == _bktNumber )
				{
					//lower bucket

					//insert item
					buckets[0][bucketIndex].push_back(
							(BucketDataEntry)
							{
								ptrOfEntry->_key,
								(RID){ptrOfEntry->_rid.pageNum, ptrOfEntry->_rid.slotNum}
							}
					);
				}
				else if( hashedKey == higherBucketNumber )
				{
					//higher bucket

					//insert item
					buckets[1][bucketIndex].push_back(
							(BucketDataEntry)
							{
								ptrOfEntry->_key,
								(RID){ptrOfEntry->_rid.pageNum, ptrOfEntry->_rid.slotNum}
							}
					);
				}
				else
				{
					//error, neither lower nor higher bucket is chosen by the hash function (-46)
					return -46;
				}

				//go to the next element
				entryIndex++;
				//check if the entry index is still within data-page boundaries
				if( entryIndex >= numEntriesInPage )
				{
					//if not, quit the two inner loops that iterates over the duplicates and items inside the given page
					break;
				}
				ptrOfEntry = (BucketDataEntry*)( (char*)_curPageData + 2 * sizeof(unsigned int) + entryIndex * SZ_OF_BUCKET_ENTRY );
			}

			//go to the previous item, because loop will increment it back
			entryIndex--;
		}

		//clear out the page
		memset(_curPageData, 0, PAGE_SIZE);

		//write back the page
		if( _curPageNum == 0 )
		{
			//write page to "primary file"
			if( (errCode = _ixfilehandle._primBucketDataFileHandler.writePage(_bktNumber + 1, _curPageData)) != 0 )
			{
				IX_PrintError(errCode);
				exit(errCode);
			}
		}
		else
		{
			//write page to "overflow file"

			//determine physical page number
			PageNum actualPageNumber = _ixfilehandle._info->_overflowPageIds[_bktNumber][_curPageNum - 1];

			if( (errCode = _ixfilehandle._overBucketDataFileHandler.writePage(actualPageNumber, _curPageData)) != 0 )
			{
				IX_PrintError(errCode);
				exit(errCode);
			}
		}
	}

	//now write the entries into lower and then higher buckets
	memset(_curPageData, 0, PAGE_SIZE);
	for( int i = 0; i < 2; i++ )
	{
		//setup parameters
		_curPageNum = 0;	//start from primary pages
		if( i == 1 )		//when i == 1 => higher bucker, so change bucket number appropriately
			_bktNumber = _bktNumber + _ixfilehandle.N_Level();

		//iterate over the elements of lower/higher buckets
		std::vector< std::vector< BucketDataEntry > >::iterator it = buckets[i].begin(), it_max = buckets[i].end();
		int index = 0;
		for( ; it != it_max; it++ )
		{
			//loop thru duplicates
			std::vector< BucketDataEntry >::iterator jt = it->begin(), jt_max = it->end();
			for( ; jt != jt_max; jt++ )
			{
				//compose page for the given bucket or its image
				BucketDataEntry* ptrEntry =
						(BucketDataEntry*)( (char*)_curPageData + 2 * sizeof(unsigned int) + index * SZ_OF_BUCKET_ENTRY );

				//copy data entry
				ptrEntry->_key = jt->_key;
				ptrEntry->_rid.pageNum = jt->_rid.pageNum;
				ptrEntry->_rid.slotNum = jt->_rid.slotNum;

				//increase index
				index++;
				if( index >= (int)MAX_BUCKET_ENTRIES_IN_PAGE )
				{
					break;
				}
			}

			//check if index within page boundaries
			if( index >= (int)MAX_BUCKET_ENTRIES_IN_PAGE || (it + 1) == it_max )
			{
				//write number of entries in the page
				((unsigned int*)_curPageData)[0] = index;

				if( numberOfPages < _curPageNum )
				{
					//need to add a new overflow page
					addPage();
				}

				//write out the composed page to an appropriate file
				if( _curPageNum == 0 )
				{
					//write page to "primary file"
					if( (errCode = _ixfilehandle._primBucketDataFileHandler.writePage(_bktNumber + 1, _curPageData)) != 0 )
					{
						IX_PrintError(errCode);
						exit(errCode);
					}
				}
				else
				{
					//write page to "overflow file"

					//determine physical page number
					PageNum actualPageNumber = _ixfilehandle._info->_overflowPageIds[_bktNumber][_curPageNum - 1];

					if( (errCode = _ixfilehandle._overBucketDataFileHandler.writePage(actualPageNumber, _curPageData)) != 0 )
					{
						IX_PrintError(errCode);
						exit(errCode);
					}
				}

				//reset
				index = 0;
				_curPageNum++;
				memset(_curPageData, 0, PAGE_SIZE);
			}
		}
	}

	//free unused pages
	_bktNumber -= _ixfilehandle.N_Level();
	_curPageNum = -1;
	for( int i = 1; i < numberOfPages; i++ )
	{
		//read page
		if( i != _curPageNum )
		{
			_curPageNum = i;

			if( (errCode = getPage()) != 0 )
			{
				IX_PrintError(errCode);
				exit(errCode);
			}
		}

		//determine number of entries in the page
		int numEntriesInPage = (int)((unsigned int*)_curPageData)[0];

		//if number of entries is zero, then
		if( numEntriesInPage == 0 )
		{
			//remove the page record
			removePageRecord();
		}
	}

	//success
	return errCode;
}

RC MetaDataSortedEntries::deleteEntry(const RID& rid)
{
	RC errCode = 0;

	RID position = (RID){0, 0};

	int maxPages = numOfPages();

	//find position of this item
	if( searchEntryInArrayOfPages(position, _curPageNum, maxPages - 1) == false )
	{
		return -43;	//attempting to delete index-entry that does not exist
	}

	_curPageNum = position.pageNum;

	//linearly search among duplicates until a requested item is found
	unsigned int numOfEntriesInPage = ((unsigned int*)_curPageData)[0];
	if( position.slotNum < numOfEntriesInPage )	//of course, providing that this page has some entries
	{
		//in the presence of duplicates, we may arrive at any random spot within the list duplicates tuples (i.e. tuples with the same key)
		//but in order to make sure that the item exists or does not exist, we need to scan the whole list of duplicates

		//first, however, need to find the start of the duplicate list
		while(true)
		{
			//check if the current item has a different key (less than the key of interest)
			if( ((BucketDataEntry*)((char*)_curPageData + 2 * sizeof(unsigned int) + position.slotNum * SZ_OF_BUCKET_ENTRY))->_key < _key )
			{
				//if so, then quit => start is found
				break;
			}

			//check if the next index is outside of the page
			if( position.slotNum == 0 )
			{
				//check if this is a primary page
				if( position.pageNum == 0 )
				{
					//if so, there are no pages in front of it => start is found
					break;
				}
				//otherwise, go to the previous page
				position.pageNum--;
				_curPageNum = position.pageNum;

				//reset slot number
				position.slotNum = MAX_BUCKET_ENTRIES_IN_PAGE - 1;

				//read in the page
				if( (errCode = getPage()) != 0 )
				{
					IX_PrintError(errCode);
					exit(errCode);
				}

				continue;
			}

			//decrement index
			position.slotNum--;
		}

		//now linearly scan thru the list of duplicates until either:
		//	1. item is found
		//	2. or, items with the given key are exhausted => no specified item exists => fail
		while
		(
			((BucketDataEntry*)((char*)_curPageData + 2 * sizeof(unsigned int) + position.slotNum * SZ_OF_BUCKET_ENTRY))->_key < _key ||
			(
				((BucketDataEntry*)((char*)_curPageData + 2 * sizeof(unsigned int) + position.slotNum * SZ_OF_BUCKET_ENTRY))->_key == _key &&
				(
					((BucketDataEntry*)((char*)_curPageData + 2 * sizeof(unsigned int) + position.slotNum * SZ_OF_BUCKET_ENTRY))->_rid.pageNum != rid.pageNum ||
					((BucketDataEntry*)((char*)_curPageData + 2 * sizeof(unsigned int) + position.slotNum * SZ_OF_BUCKET_ENTRY))->_rid.slotNum != rid.slotNum
				)
			)
		)
		{
			position.slotNum++;
			//if we go beyond the page boundaries then, go to the next page
			if( position.slotNum >= numOfEntriesInPage )
			{
				//check if next page exists
				if( position.pageNum + 1 >= (unsigned int)maxPages )
				{
					break;
				}

				//increment to next page
				position.pageNum++;
				_curPageNum = position.pageNum;

				//reset slot number
				position.slotNum = 0;

				//read in the page
				if( (errCode = getPage()) != 0 )
				{
					IX_PrintError(errCode);
					exit(errCode);
				}

				//reset number of entries in the page
				numOfEntriesInPage = ((unsigned int*)_curPageData)[0];
			}
		}

		//check that the item is found (it should be pointed now by slotNum)
		if( ((BucketDataEntry*)((char*)_curPageData + 2 * sizeof(unsigned int) + position.slotNum * SZ_OF_BUCKET_ENTRY))->_rid.pageNum != rid.pageNum ||
			((BucketDataEntry*)((char*)_curPageData + 2 * sizeof(unsigned int) + position.slotNum * SZ_OF_BUCKET_ENTRY))->_rid.slotNum != rid.slotNum )
		{
			//if it is not the item, then it means we have looped thru all duplicate entries and still have not found the right one with given RID
			return -43;	//attempting to delete index-entry that does not exist
		}
	}
	else
	{
		//again, item to be deleted is not found
		return -43;
	}
	//                  +-----------------------------+
	//                  |                             |
	//                  |                       ______|______
	//                  |          carry in to "previous page"
	//                  v                       |
	// *----------------------------------*     |
	// |                                   \    v
	// [{}()()()...(X)(a)(b)(c)(d)(e)(f)(g)][{}(h)(i)...(z)()()()]	shifting_item = (h)
	//              ^
	//              |
	//       item be deleted
	//[{}()()()...(a)(b)(c)(d)(e)(f)(g)(h)][{}(i)....(z)()()()()]
	//
	//OR
	//
	//[{}()()()...(X)(a)(b)(c)(d)(e)(f)(g)()()()]	shifting_item = None, i.e. (), since there is no next page (and also because items end before the page)
	//             ^
	//             |
	//      item be deleted
	//[{}()()()...(a)(b)(c)(d)(e)(f)(g)()()()()]

	//shift all items that exist afterwards by one element
	//	it is easier to perform shift if it is started from the end
	BucketDataEntry shiftingEntry = (BucketDataEntry){0, (RID){0, 0}};
	//{
	//	((BucketDataEntry*)_entryData)->_key,
	//	(RID){ ((BucketDataEntry*)_entryData)->_rid.pageNum, ((BucketDataEntry*)_entryData)->_rid.slotNum }
	//};

	//the last page may happen to be full by itself, and shifting a new entry into it will result into insertion of new page
	bool removePage = false;

	//save slot number
	unsigned int slotNumber = position.slotNum;

	//loop thru array of pages, starting from the one where position was found by the search function
	for( int pageNum = maxPages - 1; pageNum >= (int)position.pageNum; pageNum-- )	//starting "backwards", i.e. from the last to the one where item to be deleted resides
	{
		//read the current page
		if( pageNum != _curPageNum )
		{
			//update current page number
			_curPageNum = pageNum;

			//read in the page
			if( (errCode = getPage()) != 0 )
			{
				IX_PrintError(errCode);
				exit(errCode);
			}
		}

		//the first item is one to be shifted to the "previous page"
		BucketDataEntry firstItem;
		memcpy
		(
			&firstItem,
			(BucketDataEntry*)((char*)_curPageData + 2 * sizeof(unsigned int) ),
			SZ_OF_BUCKET_ENTRY
		);

		//[{}()()()...(X)(a)(b)(c)(d)(e)(f)(g)][{}(h)(i)....(z)()()()]
		//                ^                 ^         ^      ^
		//                |                 |         |      |
		//                +-----------------+         +------+
		//                      case # 2              case # 1

		//now determine the starting position and the size of the data segment within this page that is to be shifted
		//(two cases considered, for description see image below)
		//	case 1: shift starting from the 2nd slot in the page
		//	case 2: shift starting from the item right before the deleted one
		unsigned int start = 0;
		int size = 0;

		//determine the start index
		if( pageNum == (int)position.pageNum )
			start = slotNumber + 1;	//case 2
		else
			start = 1;	//case 1

		//BEFORE SHIFT:
		//                         number of elements in each page
		//  +--------------------------------------+--------------------------------------+
		//  |                                      |                                      |
		//  v   0  1  2  3  4  5  6  7  8  9 10    v   0  1  2  3  4  5  6  7  8  9 10    v  0  1  2  3
		//[{11}(A)(B)(C)(X)(a)(b)(c)(d)(e)(f)(g)][{11}(h)(i)(j)(k)(l)(m)(n)(o)(p)(q)(r)][{4}(s)(t)(u)(v)()()()()()()]
		//                  ^                 ^           ^                          ^          ^     ^
		//                  |                 |           |                          |          |     |
		//                  +-----------------+           +--------------------------+          +-----+
		//                      7 = 11 - 4                         10 = 11 - 1                 3 = 4 - 1
		//                       \                                 |                          /
		//                        +--------------------------------+-------------------------+
		//                                     size of each segment to be shifted
		//AFTER SHIFT:
		//[{11}(A)(B)(C)(a)(b)(c)(d)(e)(f)(g)(h)][{11}(i)(j)(k)(l)(m)(n)(o)(p)(q)(r)(s)][{3}(t)(u)(v)()()()()()()()]
		//                                    ^                                      ^    ^
		//                                    |                                      |    |
		//                                    +--------------------------------------+    |
		//                                                elements shifted                |
		//                                                                                only the last page's size is changed
		size = ((unsigned int*)_curPageData)[0] - start;

		//perform the shift, but only if it is necessary, i.e. if number of elements to be shifted > 0
		if( size > 0 )
		{
			memmove(
				((char*)_curPageData + 2 * sizeof(unsigned int) + (start - 1) * SZ_OF_BUCKET_ENTRY),
				((char*)_curPageData + 2 * sizeof(unsigned int) + start * SZ_OF_BUCKET_ENTRY),
				size * SZ_OF_BUCKET_ENTRY );
		}

		//copy the previously saved entry from the last page (or if it is the first page, then the entry to be inserted)
		//copy in the element "(#)" into position 8 from which element "(a)" was moved
		memcpy(
			(char*)_curPageData + 2 * sizeof(unsigned int) +  ( ((unsigned int*)_curPageData)[0] - 1 ) * sizeof(BucketDataEntry),
			&shiftingEntry,
			SZ_OF_BUCKET_ENTRY);

		//copy the first entry saved in this page into the variable shiftingEntry
		shiftingEntry._key = firstItem._key;
		shiftingEntry._rid.pageNum = firstItem._rid.pageNum;
		shiftingEntry._rid.slotNum = firstItem._rid.slotNum;

		//change page size of the "last page" (i.e. page from which we started the shifting)
		if( pageNum == maxPages - 1 )
		{
			((unsigned int*)_curPageData)[0]--;

			//determine if the last page needs to be deleted
			if( ((unsigned int*)_curPageData)[0] == 0 )
			{
				removePage = true;
			}
		}

		//write back the page to an appropriate file
		if( _curPageNum == 0 )
		{
			//write page to "primary file"
			if( (errCode = _ixfilehandle._primBucketDataFileHandler.writePage(_bktNumber + 1, _curPageData)) != 0 )
			{
				IX_PrintError(errCode);
				exit(errCode);
			}
		}
		else
		{
			//write page to "overflow file"

			//determine physical page number
			PageNum actualPageNumber = _ixfilehandle._info->_overflowPageIds[_bktNumber][_curPageNum - 1];

			if( (errCode = _ixfilehandle._overBucketDataFileHandler.writePage(actualPageNumber, _curPageData)) != 0 )
			{
				IX_PrintError(errCode);
				exit(errCode);
			}
		}
	}

	if( removePage )
	{
		_curPageNum = maxPages - 1;
		if( _curPageNum > 0 )
		{
			//if it happens to be the overflow page, then remove record about it from the overflowPageId map and the overflow PFM header
			if( removePageRecord() != 0 )
			{
				return errCode;
			}
		}

		unsigned int savedBucketNumber = _bktNumber;

		if( _ixfilehandle._info->Next == 0 )
		{
			_ixfilehandle._info->Next =
					(unsigned int)( _ixfilehandle._info->N * (unsigned int)pow(2.0, (int)(_ixfilehandle._info->Level - 1)) );//- 1 );	//-1 will be taken out later
			_ixfilehandle._info->Level--;
		}

		//if it is a last primary bucket, then "merge it with its image"
		if( _ixfilehandle._info->Next > 0 )
		{
			_ixfilehandle._info->Next--;
		}

		//process merge
		_bktNumber = _ixfilehandle._info->Next;
		_curPageNum = 0;
		memset(_curPageData, 0, PAGE_SIZE);
		if( (errCode = mergeBuckets()) != 0 )
		{
			return errCode;
		}

		//update IX header and fileHeader info
		if( (errCode = _ixfilehandle._metaDataFileHandler.readPage(1, _curPageData)) != 0 )
		{
			//return error code
			IX_PrintError(errCode);
			exit(errCode);
		}

		((unsigned int*)_curPageData)[1] = _ixfilehandle._info->Level;
		((unsigned int*)_curPageData)[2] = _ixfilehandle._info->Next;

		if( (errCode = _ixfilehandle._metaDataFileHandler.writePage(1, _curPageData)) != 0 )
		{
			//return error code
			IX_PrintError(errCode);
			exit(errCode);
		}

		//remove overflow pages for the image of the merged bucket
		_bktNumber = _ixfilehandle._info->Next + _ixfilehandle.N_Level();
		maxPages = numOfPages();
		_curPageNum = -1;
		for( int i = 1; i < maxPages; i++ )
		{
			_curPageNum = i;

			removePageRecord();
		}

		//erase higher bucket's primary page
		_curPageNum = _bktNumber + 1;
		if( (errCode = erasePageFromHeader(_ixfilehandle._primBucketDataFileHandler)) != 0 )
		{
			//return error code
			IX_PrintError(errCode);
			exit(errCode);
		}

		//restore bucket number
		_bktNumber = savedBucketNumber;
	}

	//success
	return errCode;
}

void MetaDataSortedEntries::insertEntry()
{
	RID position = (RID){0, 0};
	RC errCode = 0;

	int maxPages = numOfPages();

	//find the position where requested item needs to be inserted
	searchEntryInArrayOfPages(position, _curPageNum, maxPages - 1);

	_curPageNum = position.pageNum;

	//"allocate space for new entry" by shifting the data (to the "right" of the found position) by a size of of meta-data entry

	//since there could be duplicates, we need to find the "right-most" entry with this data
	unsigned int numOfEntriesInPage = ((unsigned int*)_curPageData)[0];
	if( position.slotNum < numOfEntriesInPage )	//of course, providing that this page has some entries
	{
		//keep looping while finding duplicates (i.e. entries with the same key, with the assumption of different RIDs)
		//while( ((BucketDataEntry*)_curPageData)[position.slotNum]._key <= _key )
		while( ((BucketDataEntry*)((char*)_curPageData + 2 * sizeof(unsigned int) + position.slotNum * SZ_OF_BUCKET_ENTRY))->_key <= _key )
		{
			position.slotNum++;
			//if we go beyond the page boundaries then, go to the next page
			if( position.slotNum >= numOfEntriesInPage )
			{
				//check if next page exists
				if( position.pageNum + 1 >= (unsigned int)maxPages )
				{
					break;
				}

				//increment to next page
				position.pageNum++;
				_curPageNum = position.pageNum;

				//reset slot number
				position.slotNum = 0;

				//read in the page
				if( (errCode = getPage()) != 0 )
				{
					IX_PrintError(errCode);
					exit(errCode);
				}

				//reset number of entries in the page
				numOfEntriesInPage = ((unsigned int*)_curPageData)[0];
			}
		}
	}

	//shift the chunk of data starting from position and till the rest of the file page-by-page
	//               position
	//                  |
	//                  v------------------------------------------------------> shift by 1 entry
	//[{}()()()()()()()(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)(l)][{}(m)(n)(o)(p)()()()()()()()()()()]
	//                 \                             /
	//                  *---------------------------*
	//                    shiftingEntity = (#), i.e. item that is inserted
	//[{}()()()()()()()(#)(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)][{}(m)(n)(o)(p)()()()()()()()()()()]
	//               =>    \                            /       ^
	//                      *--------------------------*        |
	//					  shiftingEntry = (l)-------------------*
	//[{}()()()()()()()(#)(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)][{}(l)(m)(n)(o)(p)()()()()()()()()()]

	//shifting entry stores the item which was either:
	//	1. the very item that needs to be inserted into the list, like item "(#)"
	//	2. or, the last item from the prior page shifting, like item "(l)" from the picture above
	BucketDataEntry shiftingEntry = (BucketDataEntry)
	{
		((BucketDataEntry*)_entryData)->_key,
		(RID){ ((BucketDataEntry*)_entryData)->_rid.pageNum, ((BucketDataEntry*)_entryData)->_rid.slotNum }
	};

	//the last page may happen to be full by itself, and shifting a new entry into it will result into insertion of new page
	bool newPage = false;

	//loop thru array of pages, starting from the one where position was found by the search function
	for( int pageNum = _curPageNum; pageNum < maxPages; pageNum++ )
	{
		//read the current page
		if( pageNum != _curPageNum )
		{
			//update current page number
			_curPageNum = pageNum;

			//read in the page
			if( (errCode = getPage()) != 0 )
			{
				IX_PrintError(errCode);
				exit(errCode);
			}
		}

		//first read in the item that will be overwritten by the data-shift operation (i.e. last item in this page)
		BucketDataEntry lastEntry;
		memcpy(
			&lastEntry,
			(BucketDataEntry*)((char*)_curPageData + 2 * sizeof(unsigned int) + (MAX_BUCKET_ENTRIES_IN_PAGE - 1) * SZ_OF_BUCKET_ENTRY ),
			SZ_OF_BUCKET_ENTRY);

		//now determine the starting position and the size of the data segment within this page that is to be shifted
		//image (above) depicts several of these cases:
		//	1. start from some position inside the page and shift to the end									(first page)
		//	2. start from the beginning of the page and shift to the end										(middle pages)
		//	3. start from the beginning of the page and shift to the specified position, where the data ends	(last page)
		unsigned int start = 0;
		int size = 0;

		//determine the start index = either the position found (for the first page) OR slot number # 1 (for all further pages)
		if( pageNum == (int)position.pageNum )
			start = position.slotNum;	//first page
		else
			start = 0;	//middle or last page

		//determine number of elements of the moving data
		//   0 1 2 3 4 5 6  7  8  9 10 11 12 13 14 15 16 17 18 => number of elements=19
		//[{}()()()()()()()(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)(l)][(m)(n)(o)(p)()()()()()()()()()()]
		//                  \                             /  ^
		//                  ^*---------------------------*   |
		//					|                                leave this entry for shifting
		//               position=7
		//	number of elements to shift is (19 - 1) - 8 = 11 (i.e., a,b,c,d,e,f,g,h,i,j,k which are 11 elements)
		//BUT
		//   0 1 2 3 4 5 6  7  8  9 10 11 12 13 14 15 => number of elements=16
		//[{}()()()()()()()(a)(b)(c)(d)(e)(f)(g)(h)(i)()()()]
		//                  \                      /
		//                   *--------------------* => no entry is necessary for shifting, since after the shift there is still space in this page
		// number of elements to shift is (16 - 1) - 7 = 15 - 7 = 8
		//size = ((unsigned int*)_curPageData)[0] + ( ((unsigned int*)_curPageData)[0] == MAX_META_ENTRIES_IN_PAGE ? 0 : 1 ) - start;
		size = ((unsigned int*)_curPageData)[0] - ( ((unsigned int*)_curPageData)[0] == MAX_BUCKET_ENTRIES_IN_PAGE ? 1 : 0 ) - start;

		if( size > 0 )
		{
			if( size + start + ( ((unsigned int*)_curPageData)[0] == MAX_BUCKET_ENTRIES_IN_PAGE ? 1 : 0 ) == MAX_BUCKET_ENTRIES_IN_PAGE )
				newPage = true;
			else
				newPage = false;

			//shift data to the "right" by 1 element within the iterated page
			//destination = start + 1 => 9
			//source = start = 8
			//size = 11 elements * size of meta-data element
			memmove(
				((char*)_curPageData + 2 * sizeof(unsigned int) + (start + 1) * SZ_OF_BUCKET_ENTRY),
				((char*)_curPageData + 2 * sizeof(unsigned int) + start * SZ_OF_BUCKET_ENTRY),
				size * SZ_OF_BUCKET_ENTRY );
		}
		else
		{
			newPage = false;	//if newPage was inserted in the last iteration, we need to reset it to false during this one
		}

		//if this is not the page
		if( start >= MAX_BUCKET_ENTRIES_IN_PAGE )
		{
			if( pageNum + 1 >= maxPages )
			{
				newPage = true;
			}

			continue;
		}

		//copy the previously saved entry from the last page (or if it is the first page, then the entry to be inserted)
		//copy in the element "(#)" into position 8 from which element "(a)" was moved
		memcpy(
			(char*)_curPageData + 2 * sizeof(unsigned int) + start * sizeof(BucketDataEntry),
			&shiftingEntry,
			SZ_OF_BUCKET_ENTRY);

		//copy the last saved entry into the variable shiftingEntry
		//that is place element "(l)" into shifting entry variable (i.e. replace existing value "(#)")
		shiftingEntry._key = lastEntry._key;
		shiftingEntry._rid.pageNum = lastEntry._rid.pageNum;
		shiftingEntry._rid.slotNum = lastEntry._rid.slotNum;

		//increment the number of elements in the last page (all other pages before should have the size equal to maximum already).
		if( newPage == false && ((unsigned int*)_curPageData)[0] < MAX_BUCKET_ENTRIES_IN_PAGE )
			((unsigned int*)_curPageData)[0]++;

		//write back the page to an appropriate file
		if( _curPageNum == 0 )
		{
			//write page to "primary file"
			if( (errCode = _ixfilehandle._primBucketDataFileHandler.writePage(_bktNumber + 1, _curPageData)) != 0 )
			{
				IX_PrintError(errCode);
				exit(errCode);
			}
		}
		else
		{
			//write page to "overflow file"

			//determine physical page number
			PageNum actualPageNumber = _ixfilehandle._info->_overflowPageIds[_bktNumber][_curPageNum - 1];

			if( (errCode = _ixfilehandle._overBucketDataFileHandler.writePage(actualPageNumber, _curPageData)) != 0 )
			{
				IX_PrintError(errCode);
				exit(errCode);
			}
		}

	}

	if( newPage )
	{
		//prepare new meta-data page
		//its structure is as follows: [number of elements, i.e. integer 1][integer 0]<[bucket number],[overflow page number]>
		memset(_curPageData, 0, PAGE_SIZE);
		((unsigned int*)_curPageData)[0] = 1;	//number of elements
		((unsigned int*)_curPageData)[1] = 0;	//isUsed (only reserving space, but not incorporated into the algorithm yet)
		((BucketDataEntry*)((char*)_curPageData + 2 * sizeof(unsigned int)))[0]._key = shiftingEntry._key;
		((BucketDataEntry*)((char*)_curPageData + 2 * sizeof(unsigned int)))[0]._rid.pageNum = shiftingEntry._rid.pageNum;
		((BucketDataEntry*)((char*)_curPageData + 2 * sizeof(unsigned int)))[0]._rid.slotNum = shiftingEntry._rid.slotNum;

		//append new page
		addPage();

		PagedFileManager* _pfm = PagedFileManager::instance();

		unsigned int dataPageId = 0, headerPageId = 0, freeSpaceLeft = 0;

		//add bucket page
		//if( (errCode = handle._primBucketDataFileHandler.appendPage(data)) != 0 )
		if( (errCode = _pfm->getDataPage(_ixfilehandle._primBucketDataFileHandler, (unsigned int)-1, dataPageId, headerPageId, freeSpaceLeft)) != 0 )
		{
			//return error code
			IX_PrintError(errCode);
			exit(errCode);
		}

		memset(_curPageData, 0, PAGE_SIZE);

		if( (errCode = _ixfilehandle._primBucketDataFileHandler.writePage(dataPageId, _curPageData)) != 0 )
		{
			//return error code
			IX_PrintError(errCode);
			exit(errCode);
		}

		//process split
		_bktNumber = _ixfilehandle._info->Next;
		memset(_curPageData, 0, PAGE_SIZE);
		_curPageNum = 0;
		if( (errCode = splitBucket()) != 0 )
		{
			IX_PrintError(errCode);
			exit(errCode);
		}

		//check if we need to increment level
		if( _ixfilehandle._info->Next == _ixfilehandle._info->N * (int)pow(2.0, (int)_ixfilehandle._info->Level) )
		{
			_ixfilehandle._info->Level++;
			_ixfilehandle._info->Next = 0;
		}
		else
		{
			_ixfilehandle._info->Next++;
		}

		//update IX header and fileHeader info
		if( (errCode = _ixfilehandle._metaDataFileHandler.readPage(1, _curPageData)) != 0 )
		{
			//return error code
			IX_PrintError(errCode);
			exit(errCode);
		}

		((unsigned int*)_curPageData)[1] = _ixfilehandle._info->Level;
		((unsigned int*)_curPageData)[2] = _ixfilehandle._info->Next;

		if( (errCode = _ixfilehandle._metaDataFileHandler.writePage(1, _curPageData)) != 0 )
		{
			//return error code
			IX_PrintError(errCode);
			exit(errCode);
		}
	}
}*/
