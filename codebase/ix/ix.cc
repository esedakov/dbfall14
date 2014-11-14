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
	if( (errCode = _pfm->insertPage(handle._metaDataFileHandler, headerPageId, dataPageId, data)) != 0 )
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
		if( (errCode = _pfm->insertPage(handle._primBucketDataFileHandler, headerPageId, dataPageId, data)) != 0 )
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

					if( (errCode = PagedFileManager::instance()->insertPage(ixfileHandle._metaDataFileHandler, headerPageId, dataPageId, buffer)) != 0 )
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

			if( (errCode = PagedFileManager::instance()->insertPage(ixfileHandle._metaDataFileHandler, headerPageId, dataPageId, buffer)) != 0 )
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
	unsigned int hkey = hash_at_specified_level(ixfileHandle._info->Level, hash(attribute, key));

	//	=> find a bucket that contains the given key
	//		~ find page inside this bucket
	//			+ use binary search
	//	   	~ determine if there is a space left in it
	//			+ NOTES:
	//				1. assume if overflow page exists, then the primary page is completely full
	//				2. also, if there are several overflow pages, then all except the last will be full, too
	//	=> if the page selected is full => perform SPLIT
	//		~ split bucket pointed by Next
	//		~ add a page to the primary bucket file to represent a new "image" bucket
	//		~ if
	//			Next == ( (N * 2^Level) - 1 ):
	//			+ set Next = 0
	//			+ Level++
	//		  else
	//			+ Next++
	//	=> in case of split
	//		~ if
	//			NEXT-1 == our bucket
	//			+ then we need to choose between two buckets and then again choose the page
	//		  else
	//			+ add overflow page (add record to meta-data file AND a page to the overflow-file if there is no vacant page, left from the previous deletions)
	//	=> insert <key, rid> record into the page (primary or overflow)
	//		~ keep in mind that the tuples are sorted through out the primary and overflow pages, so insert into appropriate spot (shift the later records by 1)

	//success
	return errCode;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	RC errCode = 0;

	//assume that file handle, attribute, key, and rid are correct

	//hashed key
	unsigned int hkey = hash_at_specified_level(ixfileHandle._info->Level, hash(attribute, key));

	//TODO:
	//	=> find page (primary bucket page or overflow page) that has the given key
	// 		~ use binary search to determine where the entry stored
	//	=> remove the entry
	//	=> if the given page becomes free, then
	//		~ if this is an overflow page => remove the record about this page from the meta-data file (PFM does not provide ability to remove pages, so leave the page)
	//		~ if this is a primary page
	//			+ if it's bucket number == (N * 2^Level) - 1 (I am not 100% sure about correctness of this step, please check me!!!)
	//				-> merge the bucket with its image
	//				   but that will not change anything,
	//				   since one of the merging buckets
	//				   is empty (the bucket that is to be removed)!
	//			+ Level--
	//			+ if Next == 0
	//				-> Next = (N * 2^Level) / 2 - 1
	//			  else
	//			  	-> Next--

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

unsigned int IndexManager::hash_at_specified_level(const int level, const unsigned int hashed_key)
{
	//take a modulo
	return hashed_key % (int)( pow((double)2.0, level) );
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
	}
	//print message
	std::cout << "component: " << compName << " => " << errMsg;
}

//functions for the SortedEntries class
MetaDataSortedEntries::MetaDataSortedEntries(IXFileHandle ixfilehandle, BUCKET_NUMBER bucket_number, unsigned int key, const void* entry)
: _ixfilehandle(ixfilehandle), _key(key), _entryData(entry), _curPageNum(0), _curPageData(malloc(PAGE_SIZE)), _bktNumber(bucket_number)
{
	getPage();
}

void MetaDataSortedEntries::addPage()
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

	if( (errCode = _pfm->insertPage(_ixfilehandle._overBucketDataFileHandler, headerPageId, dataPageId, _curPageData)) != 0 )
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
}

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

bool MetaDataSortedEntries::searchEntry(RID& position, BucketDataEntry& entry)
{
	bool success_flag = searchEntryInArrayOfPages(position, _curPageNum, numOfPages() - 1);

	if( success_flag == false )
		return false;	//failure

	//copy result
	memcpy(&entry, (BucketDataEntry*)((char*)_curPageData + 2 * sizeof(unsigned int) + position.slotNum * SZ_OF_BUCKET_ENTRY), SZ_OF_BUCKET_ENTRY);

	//success
	return success_flag;
}

int MetaDataSortedEntries::searchEntryInPage(RID& result, int indexStart, int indexEnd)
{
	//binary search algorithm adopted from: Data Abstraction and Problem Solving with C++ (published 2005) by Frank Carrano, page 87

	if( indexStart > indexEnd )
	{
		result.pageNum = _curPageNum;
		result.slotNum = indexStart == 0 ? indexStart : indexEnd;
		return indexStart == 0 ? -1 : 1;	//-1 means looking for item less than those presented in this page
											//+1 means looking for item greater than those presented in this page
	}

	int middle = (indexStart + indexEnd) / 2;
	unsigned int midValue = ((BucketDataEntry*)((char*)_curPageData + sizeof(unsigned int) * 2 + sizeof(BucketDataEntry) * middle))[0]._key;

	if( _key == midValue )
	{
		result.pageNum = (PageNum)_curPageNum;
		result.slotNum = middle;
	}
	else if( _key < midValue )
	{
		return searchEntryInPage(result, indexStart, middle - 1);
	}
	else
	{
		return searchEntryInPage(result, middle + 1, indexEnd);
	}

	//success
	return 0;	//0 means that the item is found in this page
}

bool MetaDataSortedEntries::searchEntryInArrayOfPages(RID& position, int start, int end)
{
	bool success_flag = false;

	//idea of binary search is extended to a array of pages
	//binary search algorithm adopted from: Data Abstraction and Problem Solving with C++ (published 2005) by Frank Carrano, page 87

	//seeking range gets smaller with every iteration by a approximately half, and if there is no requested item, then we will get
	//range down to a zero, i.e. when end is smaller than the start. At this instance, return failure (i.e. false)
	if( start > end )
		return success_flag;

	//determine the middle entry inside this page
	PageNum middlePageNumber = (start + end) / 2;

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

	//get number of items stored in a page
	//first integer in a (overflow or primary) page represents number of items
	//second integer, whether the page is used or not (I have not fully incorporated isUsed in the algorithm right now, just reserved the space)
	unsigned int num_entries = ((unsigned int*)_curPageData)[0];

	//determine if the requested key is inside this page
	int result = searchEntryInPage(position, 0, num_entries - 1);

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
		return searchEntryInArrayOfPages(position, start, middlePageNumber - 1);
	}
	else
	{
		//if keys inside this page are too low, then check pages to the right
		//similar idea is used over here, except new range is [middle + 1, end]
		return searchEntryInArrayOfPages(position, middlePageNumber + 1, end);
	}

	//success
	return success_flag;
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
	//               =>    \                            /         ^
	//                      *--------------------------*          |
	//					  shiftingEntry = (l)---------------------*
	//[{}()()()()()()()(#)(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)][{}(l)(m)(n)(o)(p)()()()()()()()()()]

	//shifting entry stores the item which was either:
	//	1. the very item that needs to be inserted into the list, like item "(#)"
	//	2. or, the last item from the prior page shifting, like item "(l)" from the picture above
	BucketDataEntry shiftingEntry =
			(BucketDataEntry)
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
		if( start >= MAX_META_ENTRIES_IN_PAGE )
		{
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
	}
}
