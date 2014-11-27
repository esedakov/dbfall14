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
 * -52 = attempting to close iterator for more than once
 * -53 = iterator could not go to the previous bucket without bucket merge operation
 * -54 = attempting to merge existing and non-existing buckets
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
		free(metaPageData);
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
	if( numPages < it->second.N )
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

	unsigned int general_hash = hash(attribute, key);

	//hashed key
	unsigned int hkey = hash_at_specified_level(ixfileHandle._info->N, ixfileHandle._info->Level, general_hash);

	if( hkey < (unsigned int)ixfileHandle._info->Next )
	{
		hkey = hash_at_specified_level(ixfileHandle._info->N, ixfileHandle._info->Level + 1, general_hash);
	}

	MetaDataSortedEntries mdse(ixfileHandle, hkey, attribute, key);

	if( (errCode = mdse.insertEntry(rid)) != 0 )
	{
		IX_PrintError(errCode);
		return errCode;
	}

	//success
	return errCode;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	RC errCode = 0;

	//assume that file handle, attribute, key, and rid are correct

	unsigned int general_hash = hash(attribute, key);

	//hashed key
	unsigned int hkey = hash_at_specified_level(ixfileHandle._info->N, ixfileHandle._info->Level, general_hash);

	if( hkey < (unsigned int)ixfileHandle._info->Next )
	{
		hkey = hash_at_specified_level(ixfileHandle._info->N, ixfileHandle._info->Level + 1, general_hash);
	}

	MetaDataSortedEntries mdse(ixfileHandle, hkey, attribute, key);

	if( (errCode = mdse.deleteEntry(rid)) != 0 )
	{
		IX_PrintError(errCode);
		return errCode;
	}

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
		free(carr);
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
	RC errCode = 0;

	PFMExtension pfme(ixfileHandle, primaryPageNumber);

	if( (errCode = pfme.printBucket(primaryPageNumber, attribute)) != 0 )
	{
		IX_PrintError(errCode);
		return errCode;
	}

	return errCode;
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
	ix_ScanIterator.reset();
	//memcpy(&ix_ScanIterator._attr, &attribute, sizeof(Attribute));
	ix_ScanIterator._isReset = false;
	ix_ScanIterator._attr.length = attribute.length;
	ix_ScanIterator._attr.name = attribute.name;
	ix_ScanIterator._attr.type = attribute.type;
	ix_ScanIterator._fileHandle = &ixfileHandle;
	ix_ScanIterator._pfme = new PFMExtension(ixfileHandle, 0);
	ix_ScanIterator._lowKey = lowKey;
	ix_ScanIterator._lowKeyInclusive = lowKeyInclusive;
	ix_ScanIterator._highKey = highKey;
	ix_ScanIterator._highKeyInclusive = highKeyInclusive;
	_iterators.push_back(&ix_ScanIterator);

	return 0;
}

void IX_ScanIterator::currentPosition(BUCKET_NUMBER& bkt, PageNum& page, unsigned int& slot)
{
	bkt = _bkt;
	page = _page;
	slot = _slot;
}

void IX_ScanIterator::reset()
{
	if( _isReset == false )
	{
		_isReset = true;
		_bkt = 0;
		_maxBucket = 0;
		_page = 0;
		_slot = 0;
		_lowKey = NULL;
		_lowKeyInclusive = false;
		_highKey = NULL;
		_highKeyInclusive = false;
		_fileHandle = NULL;
		free(_pfme);
		_pfme = NULL;
		//std::vector< std::pair<void*, unsigned int> >::iterator it = _alreadyScanned.begin(), imax = _alreadyScanned.end();
		//for( ; it != imax; it++ )
		//{
		//	free(it->first);
		//}
		std::map< BUCKET_NUMBER, std::vector< std::pair< void*, unsigned int > > >::iterator
			it = _mergingItems.begin(), it_max = _mergingItems.end();
		for( ; it != it_max; it++ )
		{
			vector< std::pair< void*, unsigned int > >::iterator j = it->second.begin(), jmax = it->second.end();
			for( ; j != jmax; j++ )
			{
				free(j->first);
			}
		}

		IndexManager* ix = IndexManager::instance();

		//_alreadyScanned.clear();
		_mergingItems.clear();
		std::vector<IX_ScanIterator*>::iterator
			jt = ix->_iterators.begin(),
			jmax = ix->_iterators.end();
		for( ; jt != jmax; jt++ )
		{
			if( (char*)(*jt) == (char*)this )
			{
				ix->_iterators.erase(jt);
			}
		}
	}
}

IX_ScanIterator::IX_ScanIterator()
:  _maxBucket(0), _bkt(0), _page(0), _slot(0), _mergingItems(), _lowKey(NULL), _lowKeyInclusive(false),
   _highKey(NULL), _highKeyInclusive(false), _fileHandle(NULL), _pfme(NULL), _isReset(true)
{
}

IX_ScanIterator::~IX_ScanIterator()
{
	reset();
}

bool IX_ScanIterator::isEntryAlreadyScanned(const BUCKET_NUMBER bktNumber, const void* entry, unsigned int entryLength)
{
	bool result = true;

	//determine if the bucket was merged
	if( _mergingItems.find(bktNumber) == _mergingItems.end() )
	{
		//if it was not merged, then every item that is next is needed ti be scanned
		return false;
	}

	//if it was merged, then the list for this bucket will keep items that were not scanned in this and/or image bucket
	//scan thru the list of keys in the bucket
	std::map< BUCKET_NUMBER, std::vector< std::pair< void*, unsigned int > > >::iterator it = _mergingItems.find(bktNumber);
	std::vector< std::pair< void*, unsigned int > >::iterator j = it->second.begin(), jmax = it->second.end();
	for( ; j != jmax; j++ )
	{
		//check if the iterated item is the given item
		if( j->second == entryLength && memcmp(j->first, entry, entryLength) == 0 )
		{
			result = false;
			break;
		}
	}
	return result;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	RC errCode = 0;

	//check if scan is over or not (i.e. if maximum number of buckets has been reached)
	unsigned int numBuckets = _fileHandle->NumberOfBuckets();
	if( _bkt >= numBuckets )
	{
		return IX_EOF;
	}

	free(_pfme);
	_pfme = new PFMExtension(*_fileHandle, _bkt);

	do
	{
		//buffer for keeping the current entry
		void* entry = malloc(PAGE_SIZE);
		memset(entry, 0, PAGE_SIZE);

		//get current tuple
		if( (errCode = _pfme->getTuple(entry, _bkt, _page, _slot)) != 0 )
		{
			//go to the next item
			continue;
		}

		//if( _attr.type == TypeReal )
		//{
		//	cout << "key = " << ((float*)entry)[0] << ", bkt = " << _bkt << ", page = " << _page << ", slot = " << _slot << endl;
		//}

		//estimate size of entry
		int entryLength = estimateSizeOfEntry(_attr, entry);

		//check if this entry has already been processed
		if( isEntryAlreadyScanned(_bkt, entry, entryLength) == false )
		{
			//for function "compareEntryKeyToSeparateKey" the output follows pattern outlined below:
			//+1 entry.key is less than the another key
			//0 entry.key is equal to the another key
			//-1 entry.key is greater than the another key

			int lowRes = 0, highRes = 0;

			//if necessary perform comparison between entry and low key
			if( _lowKey != NULL )
			{
				lowRes = compareEntryKeyToSeparateKey(_attr, entry, _lowKey);
			}
			else
			{
				//entry > lowKey
				lowRes = -1;
			}

			//if necessary perform comparison between entry and high key
			if( _highKey != NULL )
			{
				highRes = compareEntryKeyToSeparateKey(_attr, entry, _highKey);
			}
			else
			{
				//entry < highKey
				highRes = 1;
			}

			int lowBoundary = _lowKeyInclusive ? 0 : -1,
				highBoundary = _highKeyInclusive ? 0 : 1;

			//check if an entry is within the given boundaries
			if( lowRes <= lowBoundary && highRes >= highBoundary )
			{
				//the key is found
				//1. copy entry payLoad to attribute RID
				memcpy(&rid, (char*)entry + entryLength - sizeof(RID), sizeof(RID));
				//2. copy entry key to attribute key
				memcpy(key, (char*)entry, entryLength - sizeof(RID));	//found error in size of the transfered key
				//3. increment slot to next
				incrementToNext();
				//success
				return errCode;
			}
		}
		else
		{
			//if entry has already been processed, then deallocate its buffer and go to the next
			free(entry);
		}
	} while( incrementToNext() == 0 );

	//return end of index
	return IX_EOF;
}

RC IX_ScanIterator::decrementToPrev()
{
	RC errCode = 0;

	//reload PFME (not a bug, do not remove)
	free(_pfme);
	_pfme = new PFMExtension(*_fileHandle, _bkt);

	_slot--;
	//	3.1 if previous slot is not in this page  => increment to next page (set slot = 0)
	if( _slot < 0 )
	{
		_page--;
	}
	//check if page has a legal value
	if( _page < 0 )
	{
		return -53;	//iterator cannot go back to the previous bucket without bucket merge operation
	}

	//now when we know that page has a legal value (in case it was reset), go for an reset slot
	if( _slot < 0 )
	{
		unsigned int numEntries = 0;
		if( (errCode = _pfme->getNumberOfEntriesInPage(_bkt, _page, numEntries)) != 0 )
		{
			return errCode;
		}
		_slot = numEntries - 1;
	}

	return errCode;
}

RC IX_ScanIterator::incrementToNext()
{
	RC errCode = 0;
	bool reloadPage = false;

	//reload PFME (not a bug, do not remove)
	free(_pfme);
	_pfme = new PFMExtension(*_fileHandle, _bkt);

	_slot++;
	//	3.1 if next slot does not exist in the given page => increment to next page (set slot = 0)
	unsigned int numEntries = 0;
	if( (errCode = _pfme->getNumberOfEntriesInPage(_bkt, _page, numEntries)) != 0 )
	{
		return errCode;
	}
	if( _slot >= (int)numEntries )
	{
		_page++;
		_slot = 0;
		reloadPage = true;
	}
	//		3.1.1 if next page does not exist => increment to next bucket (set page = 0, slot = 0,
	//			  empty vector that stores keys in the bucket)
	unsigned int numPages = 0;
	if( (errCode = _pfme->numOfPages(_bkt, numPages)) != 0 )
	{
		return errCode;
	}
	if( _page >= (int)numPages )
	{
		_page = 0;
		_slot = 0;
		reloadPage = true;
		if( _mergingItems.find(_bkt) != _mergingItems.end() )
		{
			std::map< BUCKET_NUMBER, std::vector< std::pair< void*, unsigned int > > >::iterator
				it = _mergingItems.find(_bkt);
			std::vector< std::pair<void*, unsigned int> >::iterator j = it->second.begin(), jmax = it->second.end();
			for( ; j != jmax; j++ )
			{
				free(j->first);
			}
			_mergingItems.erase(it);
		}
		//_bkt++;
		//if( _bkt == _maxBucket )
		//{
		//	_bkt++;
		//	_maxBucket++;
		//}
		//else
		//{
			//if there is an available "re-updated bucket", then go ahead and transfer to it
		if( _mergingItems.size() > 0 )
		{
			_bkt = _mergingItems.begin()->first;
		}
		else
		{
			_maxBucket++;
			_bkt = _maxBucket;
		}
			/*if( _mergingItems.size() == 0 )
			{
				//no other buckets were merged so that its high bucket would be higher than MAX and lower bucket lower than MAX
				_maxBucket++;
				_bkt = _maxBucket;
			}
			else
			{
				//there are some bucket(s) to be scanned, peek the first one
				std::map< BUCKET_NUMBER, std::vector< std::pair< void*, unsigned int > > >::iterator
					nextBkt = _mergingItems.begin();
				_bkt = nextBkt->first;
			}*/
		//}
	}

	//check if scan is over or not (i.e. if maximum number of buckets has been reached)
	unsigned int numBuckets = _fileHandle->NumberOfBuckets();
	if( _bkt >= numBuckets )
	{
		return IX_EOF;
	}

	if( reloadPage )
	{
		//read data page
		if( (errCode = _pfme->getPage(_bkt, _page)) != 0 )
		{

			//read failed
			return errCode;
		}
	}

	return errCode;
}

void IX_ScanIterator::resetToBucketStart(BUCKET_NUMBER bkt)
{
	_bkt = bkt;
	_page = 0;
	_slot = 0;
}

RC IX_ScanIterator::close()
{
	reset();
	return 0;
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

int IXFileHandle::NumberOfBuckets()
{
	return _info->Next + N_Level();
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
	case -50:
		errMsg = "key was not found";
		break;
	case -51:
		errMsg = "cannot shift from one page more data than can fit inside the next page";
		break;
	case -52:
		errMsg = "attempting to close iterator for more than once";
		break;
	case -53:
		errMsg = "iterator cannot go back to the previous bucket without bucket merge operation";
		break;
	case -54:
		errMsg = "attempting to merge existing and non-existing buckets";
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

RC PFMExtension::getPage(const BUCKET_NUMBER bkt_number, const PageNum pageNumber)
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
	if( (errCode = handle.readPage(physicalPageNumber, _buffer)) != 0 )
	{
		return errCode;
	}

	_bktNumber = bkt_number;
	_curVirtualPage = pageNumber;

	//success
	return errCode;
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
: _handle(&handle), _buffer(malloc(PAGE_SIZE)), _curVirtualPage(0), _bktNumber(bkt_number)
{
	RC errCode = 0;
	if( (errCode = getPage(_bktNumber, _curVirtualPage, _buffer)) != 0 )
	{
		IX_PrintError(errCode);
		exit(errCode);
	}
}

PFMExtension::~PFMExtension()
{
	free(_buffer);
}

RC PFMExtension::printBucket(const BUCKET_NUMBER bktNumber, const Attribute& attr)
{
	RC errCode = 0;

	//assign a starting page number, and determine total number of pages
	PageNum pageNumber = 0, maxPages = 0;
	if( (errCode = numOfPages(bktNumber, maxPages)) != 0 )
	{
		return errCode;
	}

	unsigned int totalNumberOfEntries = 0;

	//determine total number of entries
	for(pageNumber = 0; pageNumber < maxPages; pageNumber++)
	{
		//load the current page
		if( pageNumber != _curVirtualPage || _bktNumber != bktNumber )
		{
			//read data page
			if( (errCode = getPage(bktNumber, pageNumber, _buffer)) != 0 )
			{

				//read failed
				return errCode;
			}

			_curVirtualPage = pageNumber;
			_bktNumber = bktNumber;
		}

		//get pointer to the end of directory slots
		PageDirSlot* startOfDirSlot = (PageDirSlot*)((char*)_buffer + PAGE_SIZE - 2 * sizeof(unsigned int));

		//find out number of directory slots
		unsigned int* numSlots = ((unsigned int*)startOfDirSlot);

		totalNumberOfEntries += *numSlots;
	}

	PageNum prevPhysPageNumber = 0;

	std::cout << "Number of total entries in the page (+ overflow pages) : " << totalNumberOfEntries << endl;

	//print all pages in the given bucket
	for(pageNumber = 0; pageNumber < maxPages; pageNumber++)
	{
		//determine physical page number
		PageNum physPageNum = 0;
		if( (errCode = translateVirtualToPhysical(physPageNum, bktNumber, pageNumber)) != 0 )
		{
			IX_PrintError(errCode);
			return errCode;
		}

		//print page info
		if( pageNumber == 0 )
		{
			std::cout << "primary Page No." << physPageNum << endl << endl;
		}
		else
		{
			std::cout << "overflow Page No." << physPageNum << " linked to " << (pageNumber == 1 ? "primary" : "overflow") << " page " << prevPhysPageNumber << endl << endl;
		}

		//get pointer to the end of directory slots
		PageDirSlot* startOfDirSlot = (PageDirSlot*)((char*)_buffer + PAGE_SIZE - 2 * sizeof(unsigned int));

		//find out number of directory slots
		unsigned int* numSlots = ((unsigned int*)startOfDirSlot);

		//print number of entries
		std::cout << "   a. # of entries : " << *numSlots << endl;

		//load the current page
		if( pageNumber != _curVirtualPage || _bktNumber != bktNumber )
		{
			//read data page
			if( (errCode = getPage(bktNumber, pageNumber, _buffer)) != 0 )
			{

				//read failed
				return errCode;
			}

			_curVirtualPage = pageNumber;
			_bktNumber = bktNumber;
		}

		std::cout << "   b. entries: ";

		//pointer to the start of the list of directory slots
		PageDirSlot* ptrEndOfDirSlot = (PageDirSlot*)( startOfDirSlot - *numSlots - 1 );

		PageDirSlot* curSlot = startOfDirSlot - 1;

		unsigned int index = 0;

		//iterate over the slots and thus find position of each record
		while( curSlot != ptrEndOfDirSlot && index < *numSlots )
		{
			//record position
			void* record = (char*)_buffer + curSlot->_offRecord;
			int start = 0;
			char* charArr = NULL;

			//print record delimiter
			std::cout << " " << "[";

			//print key
			switch(attr.type)
			{
			case TypeInt:
				std::cout << ((unsigned int*)record)[0];
				start = sizeof(int);
				break;
			case TypeReal:
				std::cout << ((float*)record)[0];
				start = sizeof(float);
				break;
			case TypeVarChar:
				start = ((unsigned int*)record)[0];
				charArr = (char*)malloc( start + 1 );
				charArr[start] = '\0';
				memcpy( charArr, (char*)record + sizeof(int), start );
				std::cout << charArr;
				start += sizeof(int);
				free(charArr);
				break;
			}

			RID* rid = (RID*)( (char*)record + start );

			//print rid
			std::cout << "/" << rid->pageNum << "," << rid->slotNum << "] ";

			//update current slot
			curSlot--;
			index++;
		}

		std::cout << endl << endl;
	}

	//success
	return errCode;
}

//virtual page number
RC PFMExtension::getTuple(void* tuple, BUCKET_NUMBER bkt_number, const unsigned int pageNumber, const int slotNumber) //modified version of ReadRecord from RBFM
{
	RC errCode = 0;

	if( tuple == NULL )
	{
		return -11; //data is corrupted
	}

	//FileHandle handle = ( _curVirtualPage == 0 ? _handle->_primBucketDataFileHandler : _handle->_overBucketDataFileHandler );

	//if necessary read in the page
	if( pageNumber != _curVirtualPage || _bktNumber != bkt_number )
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
		_bktNumber = bkt_number;
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
	if( startingInPageNumber != _curVirtualPage || _bktNumber != bkt_number )
	{
		_curVirtualPage = startingInPageNumber;
		_bktNumber = bkt_number;

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

RC PFMExtension::determineAmountOfFreeSpace(const BUCKET_NUMBER bkt_number, const PageNum page_number, unsigned int& freeSpace)
{
	RC errCode = 0;

	//if necessary read in the page
	if( page_number != _curVirtualPage || _bktNumber != bkt_number )
	{
		//read data page
		if( (errCode = getPage(bkt_number, page_number, _buffer)) != 0 )
		{
			//read failed
			return errCode;
		}

		//update
		_curVirtualPage = page_number;
		_bktNumber = bkt_number;
	}

	//get pointer to the end of directory slots
	PageDirSlot* startOfDirSlot = (PageDirSlot*)((char*)_buffer + PAGE_SIZE - 2 * sizeof(unsigned int));

	//find out number of directory slots
	unsigned int* numSlots = ((unsigned int*)startOfDirSlot);

	//pointer to the start of the list of directory slots
	PageDirSlot* ptrEndOfDirSlot = (PageDirSlot*)( startOfDirSlot - *numSlots );

	unsigned int* ptrVarForFreeSpace = (unsigned int*)( (char*)(startOfDirSlot) + sizeof(unsigned int) );

	//determine amount of free space
	freeSpace = (unsigned int)( (char*)ptrEndOfDirSlot - (char*)((char*)_buffer + (unsigned int)*ptrVarForFreeSpace) );

	//success
	return errCode;
}

RC PFMExtension::shiftRecursivelyToEnd //NOT TESTED
	(const BUCKET_NUMBER bkt_number, const PageNum currentPageNumber, const unsigned int startingFromSlotNumber,
	map<void*, unsigned int> slotsToShiftFromPriorPage)
{
	RC errCode = 0;

	//if necessary read in the page
	if( currentPageNumber != _curVirtualPage || _bktNumber != bkt_number )
	{
		//read data page
		if( (errCode = getPage(bkt_number, currentPageNumber, _buffer)) != 0 )
		{
			//read failed
			return errCode;
		}

		//update
		_curVirtualPage = currentPageNumber;
		_bktNumber = bkt_number;
	}

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
		currentSlot = ptrEndOfDirSlot;																						//breakpoint
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
	//start is specified by startingFromSlotNumber
	//end is right before the last item that will be deleted due to shift (on image item to be deleted has 'XX' content in it)

	PageDirSlot* ptrStartSlot = startOfDirSlot - startingFromSlotNumber - 1;												//breakpoint
	void* startOfShifting = NULL;
	if( startingFromSlotNumber == 0 )
		startOfShifting = (char*)_buffer;
	else if( startingFromSlotNumber == *numSlots )
		startOfShifting = _buffer;	//do not care what value this is
	else
		startOfShifting = (char*)_buffer + ptrStartSlot->_offRecord;// + ptrStartSlot->_szRecord;
			//((startingFromSlotNumber == *numSlots && shiftRecordsToNextPage.size() > 0) ? 0 : ptrStartSlot->_szRecord);

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
				(char*)( ptrStartSlot + 1 ) - (char*)eraseSlotsTillThisOne
			);
		}
	}

	//3. copy information into freed up array of records AND setup slot offsets and size attributes

	currentSlot = ptrStartSlot;// - (szForExtraSlots == 0 ? 0 : 1);
	shiftInIter = slotsToShiftFromPriorPage.begin();
	shiftInMax = slotsToShiftFromPriorPage.end();
	//void* whereToCopyData = (char*)_buffer + (*numSlots == startingFromSlotNumber ? (currentSlot + 1) : currentSlot)->_offRecord;
	void* whereToCopyData = NULL;
	if( *numSlots == 0 || startingFromSlotNumber == 0 )
	{
		whereToCopyData = _buffer;
	}
	else if( *numSlots == startingFromSlotNumber )
	{
		whereToCopyData =
				(char*)_buffer + (currentSlot + 1 + numOfSlotsToBeErased)->_offRecord + (currentSlot + 1 + numOfSlotsToBeErased)->_szRecord;
	}
	else
	{
		whereToCopyData = (char*)_buffer + currentSlot->_offRecord;
	}
	for( ; shiftInIter != shiftInMax; shiftInIter++ )
	{
		//copy the data portion for this record
		memcpy( whereToCopyData, shiftInIter->first, shiftInIter->second );
		//update slot information
		currentSlot->_szRecord = shiftInIter->second;
		currentSlot->_offRecord = (unsigned int)((char*)whereToCopyData - (char*)_buffer);
		//update starting position of shift
		whereToCopyData = (char*)whereToCopyData + shiftInIter->second;
		//update current slot
		currentSlot = currentSlot - 1;
	}

	//update the shifted slots' offsets (since they remained the same and now are incorrect)
	PageDirSlot* updatedEndSlot =
			ptrEndOfDirSlot + numOfSlotsToBeErased - slotsToShiftFromPriorPage.size() - 1;
	if( (char*)currentSlot > (char*)updatedEndSlot )
	{
		while( currentSlot != updatedEndSlot )
		{
			currentSlot->_offRecord = (currentSlot + 1)->_offRecord + (currentSlot + 1)->_szRecord;
			currentSlot--;
		}
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
	int numExtraSlots = slotsToShiftFromPriorPage.size() - numOfSlotsToBeErased;

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

	//allocate buffer for new page
	void* buf = malloc(PAGE_SIZE);
	memset(buf, 0, PAGE_SIZE);

	if( bkt_number + 2 > _handle->_primBucketDataFileHandler._info->_numPages )
	{
		//add a page to the primary file
		_handle->_primBucketDataFileHandler.appendPage(buf);
		_handle->_primBucketDataFileHandler.writeBackNumOfPages();
	}
	else
	{
		//add a page to the overflow file
		_handle->_overBucketDataFileHandler.appendPage(buf);
		_handle->_overBucketDataFileHandler.writeBackNumOfPages();

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
		_handle->_info->_overflowPageIds[bkt_number].insert(
				std::pair<int, unsigned int>(newOrderValue, _handle->_overBucketDataFileHandler._info->_numPages - 1 ) );
	}

	//deallocate buffer
	free(buf);

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

	unsigned int numOfOverflowPages = _handle->_info->_overflowPageIds[bkt_number].size();

	//allocate additional buffer for checking whether the last page is empty or not
	void* extraBuffer = malloc(PAGE_SIZE);
	memset(extraBuffer, 0, PAGE_SIZE);

	if( (errCode = getPage(bkt_number, numOfOverflowPages, extraBuffer)) != 0 )
	{
		return errCode;
	}

	lastPageIsEmpty = (bool)( ( (unsigned int*)((char*)extraBuffer + PAGE_SIZE - 2 * sizeof(unsigned int)) )[0] == 0 );

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

RC PFMExtension::emptyOutSpecifiedBucket(const BUCKET_NUMBER bkt_number)
{
	RC errCode = 0;

	//determine maximum number of pages in the bucket
	PageNum pageNum = 0, pageMax = 1;
	if( _handle->_info->_overflowPageIds.find(bkt_number) != _handle->_info->_overflowPageIds.end() )
	{
		pageMax += _handle->_info->_overflowPageIds[bkt_number].size();
	}
	pageNum = pageMax - 1;	//start from the ending page

	//setup the page sized buffer for overwriting the contents of the pages in the bucket
	void* nullingBuffer = malloc(PAGE_SIZE);
	memset(nullingBuffer, 0, PAGE_SIZE);

	//iterate over the pages and null their contents
	while(pageNum < pageMax)
	{
		//determine physical page number of the currently iterated page
		PageNum physPageNum = 0;
		if( (errCode = translateVirtualToPhysical(physPageNum, bkt_number, pageNum)) != 0 )
		{
			return errCode;
		}

		//null out the contents
		if( pageNum == 0 )
		{
			//primary page
			if( (errCode = _handle->_primBucketDataFileHandler.writePage(physPageNum, nullingBuffer)) != 0 )
			{
				return errCode;
			}
		}
		else
		{
			//overflow page
			if( (errCode =_handle->_overBucketDataFileHandler.writePage(physPageNum, nullingBuffer)) != 0 )
			{
				return errCode;
			}
			//delete record about the page
			if( (errCode = removePage(bkt_number, pageNum)) != 0 )
			{
				return errCode;
			}
		}

		//go to the next page
		pageNum--;
	}

	//free buffer
	free(nullingBuffer);

	return errCode;
}

RC PFMExtension::getNumberOfEntriesInPage(const BUCKET_NUMBER bkt_number, const PageNum pageNumber, unsigned int& numEntries)
{
	RC errCode = 0;

	//if necessary read in the page
	if( pageNumber != _curVirtualPage || bkt_number != _bktNumber )
	{
		//read data page
		if( (errCode = getPage(bkt_number, pageNumber, _buffer)) != 0 )
		{
			//read failed
			return errCode;
		}

		//update
		_curVirtualPage = pageNumber;
		_bktNumber = bkt_number;
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

	if( pageNumber + 1 > numPagesInOverflowFile )
	{
		newPage = true;

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
MetaDataSortedEntries::MetaDataSortedEntries(IXFileHandle& ixfilehandle, BUCKET_NUMBER bucket_number, const Attribute& attr, const void* key)
	:_ixfilehandle(&ixfilehandle),
	_bktNumber(bucket_number),
	_attr(attr),
	_key(NULL)
{
	int l = 0;
	switch(_attr.type)
	{
	case TypeInt:
	case TypeReal:
		l = 4;
		_key = malloc(l);
		break;
	case TypeVarChar:
		l = ((unsigned int*)key)[0] + sizeof(unsigned int);
		_key = malloc(l + 1);
		((char*)_key)[l] = '\0';
		//key = (char*)key + sizeof(unsigned int);
		break;
	}
	memcpy(_key, key, l);
	pfme= new PFMExtension(ixfilehandle, _bktNumber);
}

MetaDataSortedEntries::~MetaDataSortedEntries()
{
	free(_key);
}

RC MetaDataSortedEntries::searchEntry(RID& position, void* entry)
{
	RC errCode = 0;

	unsigned int numOfPages = 0;

	if( (errCode = pfme->numOfPages(_bktNumber, numOfPages)) != 0 )
	{
		return errCode;
	}

	bool success_flag = searchEntryInArrayOfPages(position, 0, numOfPages - 1);

	if( success_flag == false )
		return -50;

	//success
	return 0;
}

//pageNumber is virtual
int MetaDataSortedEntries::searchEntryInPage(RID& result, const PageNum& pageNumber, const int indexStart, const int indexEnd, bool& isBounding)
//isBounding should then tell the upper level page-searcher whether to continue (if isBounding is false) or stop (if isBounding is true)
{
	RC errCode = 0;
	//binary search algorithm adopted from: Data Abstraction and Problem Solving with C++ (published 2005) by Frank Carrano, page 87

	if( indexStart > indexEnd )
	{
		result.pageNum = pageNumber;
		result.slotNum = indexStart == 0 ? indexStart : indexEnd;
		//return indexStart == 0 ? -1 : 1;	//-1 means looking for item less than those presented in this page
											//+1 means looking for item greater than those presented in this page

		//if we are on the edge (indexStart = 0 or indexEnd = numEntries - 1), then
		//need to determine if neighboring items are bounding the key or not

		//determine number of entries in the given page
		unsigned int numEntries = 0;
		if( (errCode = pfme->getNumberOfEntriesInPage(_bktNumber, pageNumber, numEntries)) != 0 )
		{
			IX_PrintError(errCode);
			exit(errCode);
		}

		//now check if this is edge-case
		if( numEntries > 0 && (indexStart == 0 || indexEnd == ((int)numEntries - 1)) )
		{
			int position = (indexStart == 0 ? 0 : numEntries - 1);
			//if it is, then determine the value of the key at the edge (position 0 or numEntries-1, depending where you landed)
			//allocate buffer for storing the retrieved entry
			void* edgeKey = malloc(PAGE_SIZE);
			memset(edgeKey, 0, PAGE_SIZE);

			//retrieve middle value
			if( (errCode = pfme->getTuple(edgeKey, _bktNumber, pageNumber, position)) != 0 )
			{
				free(edgeKey);
				exit(errCode);
			}

			//perform comparison
			int cmp = compareEntryKeyToClassKey(edgeKey);

			isBounding = false;

			//if indexStart == 0, then check if edgeKey <= ourItemKey
			if( position == 0 )
			{
				isBounding = cmp >= 0;	//_key >= edgeKey
			}
			//if indexEnd == numEntries-1, then check if edgeKey >= outItemKey
			else
			{
				isBounding = cmp <= 0;
			}

			free(edgeKey);
		}
		else
		{
			isBounding = numEntries > 0;
		}

		return indexStart == 0 ? -1 : 1;	//-1 means looking for item less than those presented in this page
											//+1 means looking for item greater than those presented in this page
	}

	int middle = (indexStart + indexEnd) / 2;

	//allocate buffer for storing the retrieved entry
	void* midValue = malloc(PAGE_SIZE);
	memset(midValue, 0, PAGE_SIZE);


	//retrieve middle value
	if( (errCode = pfme->getTuple(midValue, _bktNumber, pageNumber, middle)) != 0 )
	{
		free(midValue);
		exit(errCode);
	}

	//perform comparison
	int comp_res = compareEntryKeyToClassKey(midValue);

	//if( _key == midValue )
	if( comp_res == 0 )
	{
		result.pageNum = (PageNum)pageNumber;
		result.slotNum = middle;
		isBounding = true;
	}
	//else if( _key < midValue )
	else if( comp_res < 0 )
	{
		free(midValue);
		isBounding = false;
		return searchEntryInPage(result, pageNumber, indexStart, middle - 1, isBounding);
	}
	else
	{
		free(midValue);
		isBounding = false;
		return searchEntryInPage(result, pageNumber, middle + 1, indexEnd, isBounding);
	}

	//deallocate buffer for holding middle value
	free(midValue);

	//success
	return 0;	//0 means that the item is found in this page
}

//startPageNumber and endPageNumber are virtual page indexes
//(because it is possible for them to become less than zero, so type has to be kept as integer)
bool MetaDataSortedEntries::searchEntryInArrayOfPages(RID& position, const int startPageNumber, const int endPageNumber)
{
	bool success_flag = false;
	RC errCode = 0;

	//idea of binary search is extended to a array of pages
	//binary search algorithm adopted from: Data Abstraction and Problem Solving with C++ (published 2005) by Frank Carrano, page 87

	//seeking range gets smaller with every iteration by a approximately half, and if there is no requested item, then we will get
	//range down to a zero, i.e. when end is smaller than the start. At this instance, return failure (i.e. false)
	if( startPageNumber > endPageNumber )
		return success_flag;

	//determine the middle entry inside this page
	PageNum middlePageNumber = (startPageNumber + endPageNumber) / 2;

	unsigned int numEntries = 0;
	if( (errCode = pfme->getNumberOfEntriesInPage(_bktNumber, middlePageNumber, numEntries)) != 0 )
	{
		IX_PrintError(errCode);
		exit(errCode);
	}

	bool isBounding = false;

	//determine if the requested key is inside this page
	int result = searchEntryInPage(position, middlePageNumber, 0, numEntries - 1, isBounding);

	//if it is inside this page, then return success
	if( result == 0 )
	{
		//match is found
		success_flag = true;
	}
	else
	{
		if( isBounding )
		{
			//match is not found, but may be it is not required by the upper level function (in case this is an insert command)
			success_flag = false;
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
	}

	//success
	return success_flag;
}

//+1 entry.key is less than the class key
//0 entry.key is equal to the class key
//-1 entry.key is greater than the class key
int compareEntryKeyToSeparateKey(const Attribute& attr, const void* entry, const void* key)
{
	int result = 0;

	int ikey = 0;
	float fkey = 0.0f;
	char* charArray = NULL;
	int max = 0;

	//depending on the type of class key, perform a different comparison
	//note: assuming that the class key shares type with the entry key, or else impossible to keep consistent behavior of algorithm
	switch( attr.type )
	{
	case TypeInt:
		ikey = ((int*)entry)[0];
		if( ikey < ((int*)key)[0] )
			result = 1;	//entry.key < class key
		else if( ikey == ((int*)key)[0] )
			result = 0;	//entry.key == class key
		else
			result = -1; //entry.key > class key
		break;
	case TypeReal:
		fkey = ((float*)entry)[0];
		if( fkey < ((float*)key)[0] )
			result = 1;
		else if( fkey == ((float*)key)[0] )
			result = 0;
		else
			result = -1;
		break;
	case TypeVarChar:
		max = ((unsigned int*)entry)[0] + sizeof(unsigned int);
		charArray = (char*)malloc( max + 1 );
		memcpy( charArray, (char*)entry, max );
		charArray[max] = '\0';	//need to convert key to char array in constructor if attr.type == VarChar
		result = strcmp((char*)key, charArray);
		//class key < entry.key => -1
		//class key == entry.key => 0
		//class key > entry.key => +1
		free(charArray);
		break;
	}

	return result;
}

int MetaDataSortedEntries::compareEntryKeyToClassKey(const void* entry)
{
	return compareEntryKeyToSeparateKey(_attr, entry, _key);
}

int MetaDataSortedEntries::compareTwoEntryKeys(const void* entry1, const void* entry2)
{
	int result = 0;

	int ikey1 = 0, ikey2 = 0;
	float fkey1 = 0.0f, fkey2 = 0.0f;
	char *charArray1 = NULL, *charArray2 = NULL;
	int max1 = 0, max2 = 0;

	//depending on the type of class key, perform a different comparison
	//note: assuming that the class key shares type with the entry key, or else impossible to keep consistent behavior of algorithm
	switch( _attr.type )
	{
	/*
	 * if( ikey < ((int*)key)[0] )
			result = 1;	//entry.key < class key
		else if( ikey == ((int*)key)[0] )
			result = 0;	//entry.key == class key
		else
			result = -1; //entry.key > class key
	 */
	case TypeInt:
		ikey1 = ((int*)entry1)[0];
		ikey2 = ((int*)entry2)[0];
		if( ikey1 < ikey2 )
			result = -1;	//entry1.key < entry2.key
		else if( ikey1 == ikey2 )
			result = 0;		//entry1.key == entry2.key
		else
			result = 1;		//entry1.key > entry2.key
		break;
	case TypeReal:
		fkey1 = ((float*)entry1)[0];
		fkey2 = ((float*)entry2)[0];
		if( fkey1 < fkey2 )
			result = -1;	//entry1.key < entry2.key
		else if( fkey1 == fkey2 )
			result = 0;		//entry1.key == entry2.key
		else
			result = 1;		//entry1.key > entry2.key
		break;
	case TypeVarChar:
		max1 = ((unsigned int*)entry1)[0] + sizeof(unsigned int);
		max2 = ((unsigned int*)entry2)[0] + sizeof(unsigned int);
		charArray1 = (char*)malloc( max1 + 1 );
		charArray2 = (char*)malloc( max2 + 1 );
		memcpy( charArray1, (char*)entry1 + sizeof(unsigned int), max1 );
		memcpy( charArray2, (char*)entry2 + sizeof(unsigned int), max2 );
		charArray1[max1] = '\0';	//need to convert key to char array in constructor if attr.type == VarChar
		charArray2[max2] = '\0';	//need to convert key to char array in constructor if attr.type == VarChar
		result = strcmp(charArray1, charArray2);
		//entry1.key < entry2.key => -1
		//entry1.key == entry2.key => 0
		//entry1.key > entry2.key => +1
		free(charArray1);
		free(charArray2);
		break;
	}

	return result;
}

RC MetaDataSortedEntries::insertEntry(const RID& rid)
{
	RID position = (RID){0, 0};
	RC errCode = 0;
	void* entry = malloc(PAGE_SIZE);
	memset(entry, 0, PAGE_SIZE);

	unsigned int maxPages = 0;

	if( (errCode = pfme->numOfPages(_bktNumber, maxPages)) != 0 )
	{
		free(entry);
		return errCode;
	}

	//find the position in the array of pages (primary and overflow) where requested item needs to be inserted
	searchEntryInArrayOfPages(position, 0, maxPages - 1);

	//"allocate space for new entry" by shifting the data (to the "right" of the found position)

	//since there could be duplicates, we need to find the "right-most" entry with this data
	unsigned int numOfEntriesInPage = 0;
	if( (errCode = pfme->getNumberOfEntriesInPage(_bktNumber, position.pageNum, numOfEntriesInPage)) != 0 )
	{
		free(entry);
		return errCode;
	}

	if( position.slotNum < numOfEntriesInPage )	//of course, providing that this page has some entries
	{
		//keep looping while finding duplicates (i.e. entries with the same key, with the assumption of different RIDs)
		//BucketDataEntry entry;	//cannot use BucketDataEntry since this was a fixed-length structure for the key

		if((errCode=pfme->getTuple(entry, _bktNumber, position.pageNum, position.slotNum))!=0){
			free(entry);
			return errCode;
		}

		int compResult = compareEntryKeyToClassKey(entry);
		//+1 entry.key is less than the class key
		//0 entry.key is equal to the class key
		//-1 entry.key is greater than the class key

		//while( entry <= _key )
		while( compResult >= 0 )
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

				//reset slot number
				position.slotNum = 0;

				//reset number of entries in the page
				if( (errCode = pfme->getNumberOfEntriesInPage(_bktNumber, position.pageNum, numOfEntriesInPage)) != 0 )
				{
					free(entry);
					return errCode;
				}
			}

			//get next entry
			if((errCode = pfme->getTuple(entry, _bktNumber, position.pageNum, position.slotNum))!=0){
				free(entry);
				return errCode;
			}

			//determine how new entry.key matches up with the class key
			compResult = compareEntryKeyToClassKey(entry);
		}
	}

	unsigned int dataEntryLength = 0;
	memset(entry, 0, PAGE_SIZE);

	//compose entry, i.e. <key, RID>
	//first, depending on the type determine the size of the key
	switch(_attr.type)
	{
	case TypeInt:
		dataEntryLength = sizeof(int);
		break;
	case TypeReal:
		dataEntryLength = sizeof(float);
		break;
	case TypeVarChar:
		dataEntryLength = ((unsigned int*)_key)[0] + sizeof(unsigned int);
		break;
	}
	//secondly, copy in the key
	memcpy( entry, _key, dataEntryLength );
	//lastly, copy over the RID
	memcpy( (char*)entry + dataEntryLength, (const void*)&rid, sizeof(RID) );
	//update size of entry
	dataEntryLength += sizeof(RID);

	bool newPage = false;
	//with the final position call insertTuple (PFMExtension)
	if( (errCode = pfme->insertTuple( (void *)entry, dataEntryLength,_bktNumber, position.pageNum, position.slotNum, newPage)) != 0 )
	{
		free(entry);
		return errCode;
	}

	// if new page was added, perform a split
	if( newPage )
	{
		//std::cout << endl << "primary data file:" << endl;
		//printFile(_ixfilehandle->_primBucketDataFileHandler);
		//std::cout << endl << "overflow data file:" << endl;
		//printFile(_ixfilehandle->_overBucketDataFileHandler);

		//process split
		_bktNumber = _ixfilehandle->_info->Next;
		if( (errCode = splitBucket()) != 0 )
		{
			free(entry);
			return errCode;
		}

		//std::cout << endl << "primary data file:" << endl;
		//printFile(_ixfilehandle->_primBucketDataFileHandler);
		//std::cout << endl << "overflow data file:" << endl;
		//printFile(_ixfilehandle->_overBucketDataFileHandler);

		//check if we need to increment level
		if( _ixfilehandle->_info->Next == _ixfilehandle->N_Level() )
		{
			_ixfilehandle->_info->Level++;
			_ixfilehandle->_info->Next = 0;
		}
		else
		{
			_ixfilehandle->_info->Next++;
		}

		//update IX header
		void* dataBuffer = malloc(PAGE_SIZE);
		if( (errCode = _ixfilehandle->_metaDataFileHandler.readPage(1, dataBuffer)) != 0 )
		{
			//return error code
			free(dataBuffer);
			free(entry);
			return errCode;
		}

		((unsigned int*)dataBuffer)[1] = _ixfilehandle->_info->Level;
		((unsigned int*)dataBuffer)[2] = _ixfilehandle->_info->Next;

		if( (errCode = _ixfilehandle->_metaDataFileHandler.writePage(1, dataBuffer)) != 0 )
		{
			//return error code
			free(dataBuffer);
			free(entry);
			return errCode;
		}

		//free buffer
		free(dataBuffer);
	}

	free(entry);

	//success
	return errCode;
}

bool MetaDataSortedEntries::compareEntryRidToAnotherRid(const void* entry, const RID& anotherRid)
{
	bool result = false;

	//determine offset where RID starts inside entry
	unsigned int start = 0;
	switch(_attr.type)
	{
	case TypeInt:
		start = sizeof(float);
		break;
	case TypeReal:
		start = sizeof(float);
		break;
	case TypeVarChar:
		start = ((unsigned int*)entry)[0] + sizeof(unsigned int);
		break;
	}

	//calculate pointer to the RID
	RID* entryRid = (RID*) ((char*)entry + start);

	//perform comparison
	result = entryRid->pageNum == anotherRid.pageNum && entryRid->slotNum == anotherRid.slotNum;
	return result;
}

void getKeyFromEntry(const Attribute& attr, const void* entry, void* key, int& key_length)
{
	//determine length and starting position for the given type of key
	switch(attr.type)
	{
	case TypeInt:
		key_length = sizeof(int);
		break;
	case TypeReal:
		key_length = sizeof(float);
		break;
	case TypeVarChar:
		key_length = ((unsigned int*)entry)[0] + sizeof(unsigned int);
		break;
	}

	//copy key
	memcpy(key, (char*)entry, key_length);
}

RC MetaDataSortedEntries::deleteEntry(const RID& rid)
{
	RC errCode = 0;
	unsigned int compResult = 0;
	RID position = (RID){0, 0};

	//allocate buffer for entry
	void* entry = malloc(PAGE_SIZE);
	memset(entry, 0, PAGE_SIZE);

	//get number of pages in a bucket
	unsigned int maxPages = 0;
	if( (errCode = pfme->numOfPages(_bktNumber, maxPages)) != 0 )
	{
		free(entry);
		return errCode;
	}

	//find position of this item
	if( searchEntryInArrayOfPages(position, 0, maxPages - 1) == false )
	{
		free(entry);
		return -43;	//attempting to delete index-entry that does not exist
	}

	//linearly search among duplicates until a requested item is found
	unsigned int numOfEntriesInPage = 0;
	if( (errCode = pfme->getNumberOfEntriesInPage(_bktNumber, position.pageNum, numOfEntriesInPage)) != 0 )
	{
		free(entry);
		return errCode;
	}
	if( position.slotNum < numOfEntriesInPage )	//of course, providing that this page has some entries
	{
		//in the presence of duplicates, we may arrive at any random spot within the list duplicates tuples (i.e. tuples with the same key)
		//but in order to make sure that the item exists or does not exist, we need to scan the whole list of duplicates

		//first, however, need to find the start of the duplicate list
		while(true)
		{
			//check if the current item has a different key (less than the key of interest)
			if((errCode=pfme->getTuple(entry, _bktNumber, position.pageNum, position.slotNum))!=0)
			{
				free(entry);
				return errCode;
			}

			//determine how new entry.key matches up with the class key
			compResult = compareEntryKeyToClassKey(entry);

			//if( entry._key < _key )
			if( compResult > 0 )
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

				//reset slot number
				//position.slotNum = MAX_BUCKET_ENTRIES_IN_PAGE - 1;
				if( (errCode = pfme->getNumberOfEntriesInPage(_bktNumber, position.pageNum, position.slotNum)) != 0 )
				{
					free(entry);
					return errCode;
				}
				numOfEntriesInPage = position.slotNum;
			}

			//decrement index
			position.slotNum--;
		}

		//now linearly scan thru the list of duplicates until either:
		//	1. item is found
		//	2. or, items with the given key are exhausted => no specified item exists => fail
		if((errCode=pfme->getTuple(entry, _bktNumber, position.pageNum, position.slotNum))!=0)
		{
			free(entry);
			return errCode;
		}

		//determine how new entry.key matches up with the class key
		compResult = compareEntryKeyToClassKey(entry);
		bool areRidsEqual = compareEntryRidToAnotherRid(entry, rid);

		//while( entry->_key < _key || (entry->_key == _key &&	(entry->_rid.pageNum != rid.pageNum ||	entry->_rid.slotNum != rid.slotNum)) )
		while
			(
				compResult > 0 ||
				( compResult == 0 && !areRidsEqual )
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

				//reset slot number
				position.slotNum = 0;

				//reset variable for number of entries inside the page
				if( (errCode = pfme->getNumberOfEntriesInPage(_bktNumber, position.pageNum, position.slotNum)) != 0 )
				{
					free(entry);
					return errCode;
				}
			}

			//get next tuple
			if((errCode=pfme->getTuple(entry, _bktNumber, position.pageNum, position.slotNum))!=0)
			{
				free(entry);
				return errCode;
			}

			//reset variables maintaining information about the page
			compResult = compareEntryKeyToClassKey(entry);
			areRidsEqual = compareEntryRidToAnotherRid(entry, rid);
		}

		//check that the item is found (it should be pointed now by slotNum)
		//if( entry->_rid.pageNum != rid.pageNum || entry->_rid.slotNum != rid.slotNum )
		if( compareEntryRidToAnotherRid(entry, rid) == false )
		{
			//if it is not the item, then it means we have looped thru all duplicate entries and still have not found the right one with given RID
			free(entry);
			return -43;	//attempting to delete index-entry that does not exist
		}
	}
	else
	{
		//again, item to be deleted is not found
		free(entry);
		return -43;
	}

	//if an item is deleted and it has already been scanned (i.e. it is positioned to the left of scanning marker) then
	//after deletion all items after deleted item are shifted to the left. That creates a problem for scanning iterator
	//since it now points at the next item, rather than the one that it would scan under normal considerations
	//[a][b][c][d][e]...
	// X     ^
	// |     |
	// |     iterator points at item 'c'
	// |
	// item 'a' is to be deleted before getNextEntry is going to be called
	//so, after deletion
	//[b][c][d][e]...
	//       ^
	//       |
	//       iterator did not change its position, but because of deletion it now points at the next item 'd' rather than 'c'
	//so, iterator essentially skipped 'c'!
	//So whenever, item deleted is to the left of scanning position (current marker) then after deletion, decrease scanning
	//position by number of deleted items (if 1 item is deleted, then decrease by
	std::vector<IX_ScanIterator*>::iterator
		l = IndexManager::instance()->_iterators.begin(),
		lmax = IndexManager::instance()->_iterators.end();
	for( ; l != lmax; l++ )
	{
		BUCKET_NUMBER itBucket = 0;
		PageNum itPage = 0;
		unsigned int itSlot = 0;
		(*l)->currentPosition(itBucket, itPage, itSlot);
		if( itPage == position.pageNum && itBucket == _bktNumber && itSlot > position.slotNum )
		{
			if( (errCode = (*l)->decrementToPrev()) != 0 )
			{
				free(entry);
				return errCode;
			}
		}
	}

	//delete entry using PFMExtension (by shifting entries to the start of the bucket)
	bool lastPageIsEmpty;
	if((errCode=pfme->deleteTuple(_bktNumber, position.pageNum, position.slotNum, lastPageIsEmpty))!=0)
	{
		free(entry);
		return errCode;
	}

	if( lastPageIsEmpty )
	{
		//first deal with the cause of the intended merge, i.e. with the page that got emptied
		if( (maxPages - 1) > 0 )
		{
			//if it happens to be the overflow page, then remove record about it from the overflowPageId map
			if( (errCode = pfme->removePage(_bktNumber, maxPages - 1)) != 0 )
			{
				free(entry);
				return errCode;
			}
		}

		unsigned int savedBucketNumber = _bktNumber;

		//change next appropriately
		if( _ixfilehandle->_info->Next == 0 )
		{
			//if next is already 0, then reset next to point at the ending bucket (calculated with Level-1) AND decrease level and
			_ixfilehandle->_info->Next =
					(unsigned int)
					(
						_ixfilehandle->_info->N * (unsigned int)pow(2.0, (int)(_ixfilehandle->_info->Level - 1))
					);
			_ixfilehandle->_info->Level--;
			if( _ixfilehandle->_info->Level < 0 )
			{
				_ixfilehandle->_info->Level = 0;
			}
		}

		//if it is a last primary bucket, then "merge it with its image"
		//(this is NOT a bug, intended to get inside this condition when next is reset)
		if( _ixfilehandle->_info->Next > 0 )
		{
			_ixfilehandle->_info->Next--;
		}

		//process merge
		_bktNumber = _ixfilehandle->_info->Next;
		if( (errCode = mergeBuckets()) != 0 )
		{
			free(entry);
			return errCode;
		}

		//if any of the pages belongs that belong to this or the image bucket are being scanned right now,
		//then reset the iterator to the beginning of the low bucket
		l = IndexManager::instance()->_iterators.begin();
		lmax = IndexManager::instance()->_iterators.end();
		for( ; l != lmax; l++ )
		{
			BUCKET_NUMBER itBkt = 0;
			PageNum itPage = 0;
			unsigned int itSlot = 0;
			(*l)->currentPosition(itBkt, itPage, itSlot);
			if( itBkt == _bktNumber )
			{
				(*l)->resetToBucketStart(_bktNumber);
			}
		}

		//allocate buffer for meta-data page
		void* metaDataBuffer = malloc(PAGE_SIZE);

		//update IX header and fileHeader info
		if( (errCode = _ixfilehandle->_metaDataFileHandler.readPage(1, metaDataBuffer)) != 0 )
		{
			//return error code
			free(entry);
			return errCode;
		}

		//copy in the information about current status of the linear hashing
		((unsigned int*)metaDataBuffer)[1] = _ixfilehandle->_info->Level;
		((unsigned int*)metaDataBuffer)[2] = _ixfilehandle->_info->Next;

		if( (errCode = _ixfilehandle->_metaDataFileHandler.writePage(1, metaDataBuffer)) != 0 )
		{
			//return error code
			free(entry);
			return errCode;
		}

		//deallocate buffer for holding meta-data page
		free(metaDataBuffer);

		//remove overflow pages for the image of the merged bucket
		_bktNumber = _ixfilehandle->_info->Next + _ixfilehandle->N_Level();	//Next + N*2^Level

		//determine number of pages inside the image bucket
		if( (errCode = pfme->numOfPages(_bktNumber, maxPages)) != 0 )
		{
			free(entry);
			return errCode;
		}

		for( int i = 1; i < (int)maxPages; i++ )
		{
			if( (errCode = pfme->removePage(_bktNumber, i)) != 0 )
			{
				free(entry);
				return errCode;
			}
		}

		//restore bucket number
		_bktNumber = savedBucketNumber;
	}

	//deallocate entry buffer
	free(entry);

	//success
	return errCode;
}

int estimateSizeOfEntry(const Attribute& attr, const void* entry)
{
	int sz = sizeof(RID);

	switch(attr.type)
	{
	case TypeInt:
		sz += sizeof(int);
		break;
	case TypeReal:
		sz += sizeof(float);
		break;
	case TypeVarChar:
		sz += ((unsigned int*)entry)[0] + sizeof(unsigned int);
		break;
	}

	//success
	return sz;
}

RC MetaDataSortedEntries::splitBucket()
{
	RC errCode = 0;

	BUCKET_NUMBER bktNumber[2] = {_bktNumber, _bktNumber + _ixfilehandle->N_Level()};

	//add page for primary bucket, providing that the file does not have one already
	if( _ixfilehandle->_primBucketDataFileHandler._info->_numPages < bktNumber[1] + 2 )
	{
		void* bucketPage = malloc(PAGE_SIZE);
		memset(bucketPage, 0, PAGE_SIZE);

		if( (errCode = pfme->addPage(bucketPage, bktNumber[1])) != 0 )
		{
			free(bucketPage);
			return errCode;
		}

		free(bucketPage);
	}

	//determine number of pages
	unsigned int maxPages = 0;
	pfme->numOfPages(bktNumber[0], maxPages);

	//buffer for keeping the current entry
	void* entry = malloc(PAGE_SIZE);
	memset(entry, 0, PAGE_SIZE);

	//maintain the two lists of entries for two buckets
	vector< std::pair<void*, unsigned int> > output[2];

	//iterate over the lower bucket
	int pageNum = 0, slotNum = 0;
	for( pageNum = 0; pageNum < (int)maxPages; pageNum++ )
	{
		//determine number of slots in the current page
		unsigned int maxSlots = 0;
		if( (errCode = pfme->getNumberOfEntriesInPage(_bktNumber, pageNum, maxSlots)) != 0 )
		{
			free(entry);
			return errCode;
		}

		//iterate over the slots
		for( slotNum = 0; slotNum < (int)maxSlots; slotNum++ )
		{
			//get current tuple
			if( (errCode = pfme->getTuple(entry, bktNumber[0], pageNum, slotNum)) != 0 )
			{
				//increment to next page and reset slot number
				pageNum++;
				slotNum = 0;

				//quit the inner loop and let the outer to decide whether continue or not, depending if there are more pages left
				break;
			}

			//need to allocate a buffer for the key in order to perform hashing at Level+1
			void* key = malloc(PAGE_SIZE);
			memset(key, 0, PAGE_SIZE);
			int key_length = 0;

			//get key from acquired entry
			getKeyFromEntry(_attr, entry, key, key_length);

			//hash the key
			unsigned int hashed_key = IndexManager::instance()->hash(_attr, key);

			//test hashing function of Level+1 to check whether it should belong to lower or to higher bucket
			unsigned int hashedKey =
				IndexManager::instance()->hash_at_specified_level(
					_ixfilehandle->_info->N, _ixfilehandle->_info->Level + 1, hashed_key );

			//also need a separate (individual) copy of the entry
			unsigned int szOfEntryBuffer = key_length + sizeof(RID);
			void* bufForEntry = malloc(szOfEntryBuffer);
			memcpy(bufForEntry, entry, szOfEntryBuffer);

			//insert item into appropriate temporary bucket buffer
			if( hashedKey == _bktNumber )
			{
				output[0].push_back( std::pair<void*, unsigned int>( bufForEntry, szOfEntryBuffer ) );
			}
			else
			{
				output[1].push_back( std::pair<void*, unsigned int>( bufForEntry, szOfEntryBuffer ) );
			}

			//key buffer is no longer necessary, deallocate it
			free(key);
		}
	}

	//erase content from the lower bucket
	if( (errCode = pfme->emptyOutSpecifiedBucket(bktNumber[0])) != 0 )
	{
		free(entry);
		return errCode;
	}

	//two buckets will be "controlled" by an individual PFMExtension component
	PFMExtension* bucketController[2] = { NULL, NULL };
	bucketController[0] = new PFMExtension(*_ixfilehandle, bktNumber[0]);
	bucketController[1] = new PFMExtension(*_ixfilehandle, bktNumber[1]);

	//now go ahead and populate both buckets
	for( int i = 0; i < 2; i++ )
	{
		bool newPage = false;
		pageNum = 0;
		slotNum = 0;

		//iterate over the temporary bucket buffer
		std::vector< std::pair<void*, unsigned int> >::iterator it = output[i].begin(), max = output[i].end();
		for( ; it != max; it++ )
		{
			//insert current entry into the appropriate bucket
			if( (errCode = bucketController[i]->insertTuple(it->first, it->second, bktNumber[i], pageNum, slotNum, newPage)) == -23 )
			{
				IX_PrintError(errCode);
				return errCode;
			}

			//debugging
			//if( maxPages == 4 && (slotNum == 202 || slotNum == 203 || slotNum == 204) )
			//{
			////	std::cout << endl << "primary data file:" << endl;
			////	printFile(_ixfilehandle->_primBucketDataFileHandler);
			////	std::cout << endl << "overflow data file:" << endl;
			////	printFile(_ixfilehandle->_overBucketDataFileHandler);
	        //    unsigned int numberOfPagesFromFunction = 0;
	        //	// Get number of primary pages
	        //    RC rc = IndexManager::instance()->getNumberOfPrimaryPages(*_ixfilehandle, numberOfPagesFromFunction);
	        //    if(rc != 0)
	        //    {
	        //    	cout << "getNumberOfPrimaryPages() failed." << endl;
	        //    	//indexManager->closeFile(ixfileHandle);
	        //		return -1;
	        //    }

	        //	// Print Entries in each page
	        //	for (unsigned i = 0; i < numberOfPagesFromFunction; i++) {
	        //		rc = IndexManager::instance()->printIndexEntriesInAPage(*_ixfilehandle, _attr, i);
	        //		if (rc != 0) {
	        //        	cout << "printIndexEntriesInAPage() failed." << endl;
	        //			//indexManager->closeFile(ixfileHandle);
	        //			return -1;
	        //		}
	        //	}
			//}

			//update slot number
			slotNum++;

			if( newPage )
			{
				//current page is full, go to the next
				pageNum++;
				slotNum = 1;

				//check whether it is some PFME issue
				//if( it == output[i].begin() )
				//{
				//	free(entry);
				//	free(bucketController[0]);
				//	free(bucketController[1]);
				//	return -51;	//PFME is buggy, since page is not full, but it is claimed to be full
				//}
			}

			//deallocate buffer
			free(it->first);
		}
	}

	//deallocate buffer for storing retrieved entries and PFME controllers
	free(entry);
	free(bucketController[0]);
	free(bucketController[1]);

	//success
	return errCode;
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

	BUCKET_NUMBER bktNumber[2] = {_bktNumber, _bktNumber + _ixfilehandle->N_Level()};

	//check if both buckets exist
	if( _ixfilehandle->NumberOfBuckets() <= (int)bktNumber[1] )
	{
		if( _ixfilehandle->_info->Level == 0 )
			return 0;
		//return -54;
	}

	std::map< IX_ScanIterator*, std::vector< std::pair<void*, unsigned int> > > mergingIterList;

	//setup merging iterator list
	IndexManager* ixm = IndexManager::instance();
	std::vector<IX_ScanIterator*>::iterator  vIterIt = ixm->_iterators.begin(), vIterMax = ixm->_iterators.end();
	for( ; vIterIt != vIterMax; vIterIt++ )
	{
		//check whether iterator needs to know about this splitting strategy
		if( (*vIterIt)->_maxBucket <= bktNumber[1] && (*vIterIt)->_maxBucket >= bktNumber[0] )
		{
			mergingIterList.insert
			(
				std::pair<IX_ScanIterator*, std::vector< std::pair<void*, unsigned int> > >
				(
					*vIterIt, std::vector< std::pair<void*, unsigned int> >()
				)
			);
		}
	}

	// Print two buckets
	/*errCode = IndexManager::instance()->printIndexEntriesInAPage(*_ixfilehandle, _attr, bktNumber[0]);
	if (errCode != 0) {
		cout << "printIndexEntriesInAPage() failed." << endl;
		//indexManager->closeFile(ixfileHandle);
		return -1;
	}
	errCode = IndexManager::instance()->printIndexEntriesInAPage(*_ixfilehandle, _attr, bktNumber[1]);
	if (errCode != 0) {
		cout << "printIndexEntriesInAPage() failed." << endl;
		//indexManager->closeFile(ixfileHandle);
		return -1;
	}*/

	//two buckets will be "controlled" by an individual PFMExtension component
	PFMExtension* bucketController[2] = { NULL, NULL };
	bucketController[0] = new PFMExtension(*_ixfilehandle, bktNumber[0]);
	bucketController[1] = new PFMExtension(*_ixfilehandle, bktNumber[1]);

	//maintain state for each bucket
	bool IsBucketEmpty[2] = {false, false};	//low bucket, high bucket
	int pageNumber[2] = {0, 0}; //low bucket, high bucket
	int slotNumber[2] = {0, 0};	//low bucket, high bucket

	//determine maximum number of pages in each bucket
	unsigned int maxPages[2] = {0, 0};	//low bucket, high bucket
	pfme->numOfPages(bktNumber[0], maxPages[0]);	//low bucket
	pfme->numOfPages(bktNumber[1], maxPages[1]);	//high bucket

	//allocate space for both tuples and null their contents
	void* tuples[2] = { malloc(PAGE_SIZE) , malloc(PAGE_SIZE) };	//low bucket, high bucket
	memset(tuples[0], 0, PAGE_SIZE);
	memset(tuples[1], 0, PAGE_SIZE);

	//accumulate result inside "output buffer"
	std::vector< std::pair<void*, unsigned int> > output;


	//bool debugFlag = false;

	//iterate over 2 buckets
	while( IsBucketEmpty[0] == false || IsBucketEmpty[1] == false )	//keep looping while there is at least 1 bucket non-empty
	{
		//get the current tuple from both buckets
		int i = 0;
		for( ; i < 2; i++ )
		{
			if( (errCode = bucketController[i]->getTuple( tuples[i], bktNumber[i], pageNumber[i], slotNumber[i] )) != 0 )
			{
				pageNumber[i]++;
				slotNumber[i] = 0;

				//determine if there are more pages in the given bucket
				if( pageNumber[i] >= (int)maxPages[i] )
				{
					//file is exhausted of tuples, so set this bucket to be empty
					IsBucketEmpty[i] = true;
					//go to the next bucket
					continue;
				}

				//if there are more pages, then go ahead and repeat procedure of getting a new tuple in the next page
				//re-run the getTuple on this bucket with the reset page/slot information
				i--;
				//debugFlag = true;
			}
		}

		void* buf = NULL;
		int sz_of_buf = 0;
		int selected_tuple_index = 0;

		//if both buckets are not empty, then
		if( IsBucketEmpty[0] == false && IsBucketEmpty[1] == false )
		{
			//compare keys of the two tuples
			if( compareTwoEntryKeys(tuples[0], tuples[1]) <= 0 )
			{
				//tuple_0 <= tuple_1
				//select tuple_0
				selected_tuple_index = 0;
			}
			else
			{
				//tuple_0 > tuple_1
				//select tuple_1
				selected_tuple_index = 1;
			}
		}
		else if( IsBucketEmpty[0] == false )
		{
			//if bucket # 0 is not empty
			selected_tuple_index = 0;
		}
		else if( IsBucketEmpty[1] == false )
		{
			//if bucket # 1 is not empty
			selected_tuple_index = 1;
		}
		else
		{
			break;
		}

		//allocate buffer for the tuple and copy it in
		sz_of_buf = estimateSizeOfEntry( _attr, tuples[selected_tuple_index] );
		buf = malloc( sz_of_buf );
		memcpy(buf, tuples[selected_tuple_index], sz_of_buf);

		//insert buffer into output vector
		output.push_back( std::pair<void*, unsigned int>(buf, sz_of_buf) );

		//not a bug: do not deallocate buffer 'buf'

		//update slot number for which item was selected
		slotNumber[selected_tuple_index]++;

		//loop thru iterators
		std::map< IX_ScanIterator*, std::vector< std::pair<void*, unsigned int> > >::iterator
			mi_it = mergingIterList.begin(), mi_max = mergingIterList.end();
		for( ; mi_it != mi_max; mi_it++ )
		{
			//now determine whether this item needs to be added to the current merging iterator list
			if( mi_it->first->_maxBucket == bktNumber[selected_tuple_index] )
			{
				//some elements could already be scanned => we should only add those that are not yet scanned
				if( slotNumber[selected_tuple_index] >= mi_it->first->_slot )
				{
					//then this item has not yet been scanned, go ahead and add it to the list
					void* entryToCopy = malloc(sz_of_buf);
					memcpy(entryToCopy, buf, sz_of_buf);
					mi_it->second.push_back( std::pair<void*, unsigned int>(entryToCopy, sz_of_buf) );
					//same as 'buf', do not deallocate over here
				}
			}
			if( mi_it->first->_maxBucket < bktNumber[1] )
			{
				//now if we are either in the lower bucket or in some other one but it is between
				//these lower and higher buckets, then add all elements of the higher bucket to
				//the yet to be scanned list (i.e. always add a new item)
				void* entryToCopy = malloc(sz_of_buf);
				memcpy(entryToCopy, buf, sz_of_buf);
				mi_it->second.push_back( std::pair<void*, unsigned int>(entryToCopy, sz_of_buf) );
				//same as 'buf', do not deallocate over here
			}
		}

	}

	//cout << endl;

	//if( debugFlag )
	//{
		// Print two buckets
		/*errCode = IndexManager::instance()->printIndexEntriesInAPage(*_ixfilehandle, _attr, bktNumber[0]);
		if (errCode != 0) {
			cout << "printIndexEntriesInAPage() failed." << endl;
			//indexManager->closeFile(ixfileHandle);
			return -1;
		}
		errCode = IndexManager::instance()->printIndexEntriesInAPage(*_ixfilehandle, _attr, bktNumber[1]);
		if (errCode != 0) {
			cout << "printIndexEntriesInAPage() failed." << endl;
			//indexManager->closeFile(ixfileHandle);
			return -1;
		}*/
	//}

	//empty out the lower bucket
	bucketController[0]->emptyOutSpecifiedBucket(_bktNumber);

	free(bucketController[0]);
	bucketController[0] = new PFMExtension(*_ixfilehandle, bktNumber[0]);

	bool newPage = false;
	unsigned int pageNum = 0, slotNum = 0;

	//write into lower bucket all of the entries from the output vector
	std::vector< std::pair<void*, unsigned int> >::iterator it = output.begin(), it_max = output.end();
	for( ; it != it_max; it++ )
	{

		//if( _attr.type == TypeReal )
		//{
		//	cout << "void*: " << ( (float*)it->first )[0] << "size: " << it->second << endl;
		//}

		//if( slotNum == 203 && debugFlag )
		//{
		//	cout << "slot Number : " << slotNum << endl;
		//}

		//determine free space left in the given page
		unsigned int freeSpaceLeftInPage = 0;
		if( (errCode = bucketController[0]->determineAmountOfFreeSpace(_bktNumber, pageNum, freeSpaceLeftInPage)) != 0 )
		{
			IX_PrintError(errCode);
			return errCode;
		}

		//check if new item will fit in
		if( freeSpaceLeftInPage < it->second )
		{
			//increment to next page
			pageNum++;
			slotNum = 0;

			//check whether bucket has new page, if not add it
			if( pageNum + 1 > _ixfilehandle->_info->_overflowPageIds[_bktNumber].size() )
			{
				//allocate new page data buffer
				void* newPageDataBuffer = malloc(PAGE_SIZE);
				memset(newPageDataBuffer, 0, PAGE_SIZE);

				//add the page to this file
				if( (errCode = bucketController[0]->addPage(newPageDataBuffer, _bktNumber)) != 0 )
				{
					free(newPageDataBuffer);
					return errCode;
				}

				//deallocate data page buffer
				free(newPageDataBuffer);
			}

			//check whether it is some PFME issue
			if( it == output.begin() )
			{
				free(bucketController[0]);
				free(bucketController[1]);
				free(tuples[0]);
				free(tuples[1]);
				return -51;	//PFME is buggy, since page is not full, but it is claimed to be full
			}
		}

		//now since we know that the page has enough of space, go on and insert a new item
		if( (errCode = bucketController[0]->insertTuple( it->first, it->second, _bktNumber, pageNum, slotNum, newPage )) != 0 )
		{
			IX_PrintError(errCode);
			return errCode;
		}

		//if( debugFlag )
		//{
			// Print two buckets
			/*errCode = IndexManager::instance()->printIndexEntriesInAPage(*_ixfilehandle, _attr, bktNumber[0]);
			if (errCode != 0) {
				cout << "printIndexEntriesInAPage() failed." << endl;
				//indexManager->closeFile(ixfileHandle);
				return -1;
			}
			errCode = IndexManager::instance()->printIndexEntriesInAPage(*_ixfilehandle, _attr, bktNumber[1]);
			if (errCode != 0) {
				cout << "printIndexEntriesInAPage() failed." << endl;
				//indexManager->closeFile(ixfileHandle);
				return -1;
			}*/
		//}

		//increment to next slot
		slotNum++;

		//free the entry buffer
		free(it->first);
	}

	//add merging iterator lists to the appropriate iterators
	std::map< IX_ScanIterator*, std::vector< std::pair<void*, unsigned int> > >::iterator
		milit = mergingIterList.begin(), milmax = mergingIterList.end();
	for( ; milit != milmax; milit++ )
	{
		milit->first->_mergingItems.insert
		(
			std::pair<BUCKET_NUMBER, std::vector< std::pair<void*, unsigned int> > >(bktNumber[0], milit->second)
		);

		//if this iterator is scanning the upper (higher) bucket, then transfer it immediately to the start of re-updated lower bucket
		if( milit->first->_bkt == bktNumber[1] )
		{
			milit->first->resetToBucketStart(bktNumber[0]);	//reset to the start of the lower bucket
		}
	}

	//deallocate buffers and other variables
	free(bucketController[0]);
	free(bucketController[1]);
	free(tuples[0]);
	free(tuples[1]);

	//if( debugFlag )
	//{
		// Print two buckets
		/*errCode = IndexManager::instance()->printIndexEntriesInAPage(*_ixfilehandle, _attr, bktNumber[0]);
		if (errCode != 0) {
			cout << "printIndexEntriesInAPage() failed." << endl;
			//indexManager->closeFile(ixfileHandle);
			return -1;
		}
		errCode = IndexManager::instance()->printIndexEntriesInAPage(*_ixfilehandle, _attr, bktNumber[1]);
		if (errCode != 0) {
			cout << "printIndexEntriesInAPage() failed." << endl;
			//indexManager->closeFile(ixfileHandle);
			return -1;
		}*/
	//}

	//success
	return 0;
}
