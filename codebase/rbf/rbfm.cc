
#include "rbfm.h"
#include <iostream>
#include <stdlib.h>
#include <string.h>

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;
PagedFileManager* RecordBasedFileManager::_pfm = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
    {
        _rbf_manager = new RecordBasedFileManager();

        //create an instance of PagedFileManager
        _pfm = PagedFileManager::instance();
    }

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{//did not change
}

RecordBasedFileManager::~RecordBasedFileManager()
{//did not change
}

/*
 * error codes for PagedFileManager:
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
 *
 * error codes for RecordBasedFileManager:
 * -20 = unknown type of record caught at printRecord function
 * -21 = size of record exceeds size of page (less required page meta-data)
 * -22 = could not insert record, because page header contains incorrect information about free space in the data pages
 * -23 = rid is not setup correctly OR page number exceeds the total number of pages in a file
 * -24 = directory slot stores wrong information
 * -25 = cannot update record without change of rid (no space within the given page, and cannot move to a different page!)
 * -26 = no requested attribute was found inside the record
**/

RC RecordBasedFileManager::createFile(const string &fileName) {
	//error value
	RC errCode = 0;

	//create an empty file using pfm (i.e. PagedFileManager)
	if( (errCode = _pfm->createFile( fileName.c_str() )) != 0 )
	{
		return errCode;
	}

	/*
	 * create a header a page:
	 * I use a page directory method, which implies usage of "linked list" of page headers.
	 * The content of header page is as follows:
	 *   + nextHeaderPageId:PageNum
	 *   + numUsedPageIds:PageNum (i.e. unsigned integer)
	 *   + array of <pageIds:(unsigned integer), numFreeBytes:{unsigned integer}> for the rest of header page => (unsigned integer)[PAGE_SIZE - 2*SIZEOF(pageNum)]
	**/
	//open file
	FileHandle fileHandle;
	errCode = _pfm->openFile(fileName.c_str(), fileHandle);
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

	//close file
	errCode = _pfm->closeFile(fileHandle);
	if(errCode != 0)
	{
		return errCode;
	}

	//success => return 0
	return errCode;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	RC errCode = 0;

	//delete specified file
	if( (errCode = _pfm->destroyFile( fileName.c_str() )) != 0 )
	{
		return errCode;
	}

	//success => return 0
	return errCode;
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    //error value
	RC errCode = 0;

	//open a specified file and assign a given handler to this file
	if( (errCode = _pfm->openFile( fileName.c_str(), fileHandle )) != 0 )
	{
		return errCode;
	}

    //success => return 0
	return errCode;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    //error value
	RC errCode = 0;

	//close a file
	if( (errCode = _pfm->closeFile( fileHandle )) != 0 )
	{
		return errCode;
	}

	//success => return 0
	return errCode;
}

RC RecordBasedFileManager::getDataPage(FileHandle &fileHandle, const unsigned int recordSize, PageNum& pageNum, PageNum& headerPage, unsigned int& freeSpaceLeftInPage)
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
			if( pi->_numFreeBytes > recordSize + sizeof(PageDirSlot) )
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

RC RecordBasedFileManager::findRecordSlot(FileHandle &fileHandle, PageNum pagenum, const unsigned int szRecord, PageDirSlot& slot, unsigned int& slotNum, unsigned int freeSpaceLeftInPage)
{
	RC errCode = 0;

	//prepare parameters for reading the data page
	void* data = malloc(PAGE_SIZE);
	memset(data, 0, PAGE_SIZE);

	//get content of the data page
	if( (errCode = fileHandle.readPage(pagenum, data)) != 0 )
	{
		//deallocate data
		free(data);

		//return error code
		return errCode;
	}
	/*
	 * data page has a following format:
	 * [list of records without any spaces in between][free space for records][list of directory slots][(number of slots):unsigned int][(offset from page start to the start of free space):unsigned int]
	 * ^                                                                      ^                       ^                                                                                                 ^
	 * start of page                                                          start of dirSlot        end of dirSlot                                                                          end of page
	 */
	//determine pointer to the end of the list of directory slots
	PageDirSlot* endOfDirSlot = (PageDirSlot*)((char*)data + PAGE_SIZE - sizeof(unsigned int) - sizeof(unsigned int));

	//number of slots
	unsigned int* ptrNumSlots = (unsigned int*)(endOfDirSlot);

	//pointer to the start of free space
	char* ptrToFreeSpace = (char*)((char*)data + *( (unsigned int*)( (char*)(endOfDirSlot) + sizeof(unsigned int) ) ));
	unsigned int* ptrVarForFreeSpace = (unsigned int*)( (char*)(endOfDirSlot) + sizeof(unsigned int) );

	//pointer to the start of the list of directory slots
	PageDirSlot* startOfDirSlot = (PageDirSlot*)( endOfDirSlot - (*ptrNumSlots) );

	//determine if there is available page directory slot, left from priorly removed record
	PageDirSlot* curSlot = startOfDirSlot;
	while(curSlot != endOfDirSlot)
	{
		//check if this slot is empty(unsigned int*)( (char*)(endOfDirSlot) + sizeof(unsigned int) )
		if(curSlot->_offRecord == 0 && curSlot->_szRecord == 0)
		{
			//found empty slot
			break;
		}

		//increment to the next slot
		curSlot++;
	}

	//if there is not available page directory slot, then "reserve next" (get a pointer to it)
	if( curSlot == endOfDirSlot )
	{
		//point it at the slot right before start of list of directory slots
		curSlot = (PageDirSlot*)(startOfDirSlot - 1);
		startOfDirSlot = curSlot;

		//initialize it to 0's
		curSlot->_offRecord = 0;
		curSlot->_szRecord = 0;

		//increment number of slots(unsigned int*)( (char*)(endOfDirSlot) + sizeof(unsigned int) )
		*ptrNumSlots += 1;
	}

	//determine size of free space in this page
	unsigned int szOfFreeSpace = (unsigned int)((char*)startOfDirSlot - ptrToFreeSpace);

	if( freeSpaceLeftInPage != (szOfFreeSpace - szRecord) )
	{
		freeSpaceLeftInPage++;
	}

	//if size of free space is not enough return -1 (because it was suppose to be enough, since this page was found by method getPage)
	if( szOfFreeSpace < szRecord )
	{
		//free data
		free(data);

		//could not insert record (page header contains incorrect information)
		return -22;
	}

	//update record size and offset
	curSlot->_szRecord = szRecord;
	curSlot->_offRecord = *ptrVarForFreeSpace;

	//update start of free space
	*ptrVarForFreeSpace += szRecord;

	//assign number of slots to a passed variable by reference
	slotNum = (*ptrNumSlots) - 1;

	//assign slot passed by reference
	slot = *curSlot;

	//write page to the file
	if( (errCode = fileHandle.writePage(pagenum, data)) != 0 )
	{
		//deallocate data page
		free(data);

		//return error
		return errCode;
	}

	//deallocate data
	free(data);

	//return success
	return errCode;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	RC errCode = 0;

	//check if data is not NULL
	if( data == NULL )
	{
		return -11; //data is corrupted
	}

	//determine size of given record
	unsigned int recordSize = sizeOfRecord(recordDescriptor, data);

	//check if data is not greater than a max allowed space within the page
	if( recordSize >= MAX_SIZE_OF_RECORD )
	{
		return -21;	//record exceeds page size (less required page meta-data)
	}

	unsigned int freeSpaceLeft = 0;

	//get page that contains the necessary amount of free space
	PageNum datapagenum = 0, headerpagenum = 0;
	if( (errCode = getDataPage(fileHandle, recordSize, datapagenum, headerpagenum, freeSpaceLeft)) != 0 )
	{
		//if there been error while getting the page => return this error code
		return errCode;
	}

	//get the place for the new record in the found data page
	PageDirSlot pds;
	unsigned int slotNum = 0;
	if( (errCode = findRecordSlot(fileHandle, datapagenum, recordSize, pds, slotNum, freeSpaceLeft)) != 0 )
	{
		return errCode;
	}

	//read data of the data page
	char* dataPage = (char*)malloc(PAGE_SIZE);
	memset(dataPage, 0, PAGE_SIZE);
	if( (errCode = fileHandle.readPage(datapagenum, dataPage)) != 0 )
	{
		return errCode;
	}

	//get pointer to the record
	char* ptrRecord = (char*)dataPage + pds._offRecord;

	//copy data of the record
	memcpy(ptrRecord, data, recordSize);

	//write page to the file
	if( (errCode = fileHandle.writePage(datapagenum, dataPage)) != 0 )
	{
		//deallocate data page
		free(dataPage);

		//return error
		return errCode;
	}

	//deallocate dataPage
	free(dataPage);

	//assign rid
	rid.pageNum = datapagenum;
	rid.slotNum = slotNum;

	//success return 0
	return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    RC errCode = 0;

    if( data == NULL )
    {
    	return -11; //data is corrupted
    }

    if( rid.pageNum == 0 || rid.pageNum >= fileHandle.getNumberOfPages() )
    {
    	return -23; //rid is not setup correctly
    }

    //allocate array for storing contents of the data page
    void* dataPage = malloc(PAGE_SIZE);
    memset(dataPage, 0, PAGE_SIZE);

    //read data page
    if( (errCode = fileHandle.readPage(rid.pageNum, dataPage)) != 0 )
    {
    	//deallocate dataPage
    	free(dataPage);

    	//read failed
    	return errCode;
    }

    /*
	 * data page has a following format:
	 * [list of records without any spaces in between][free space for records][list of directory slots][(number of slots):unsigned int][(offset from page start to the start of free space):unsigned int]
	 * ^                                                                      ^                       ^                                                                                                 ^
	 * start of page                                                          start of dirSlot        end of dirSlot                                                                          end of page
	 */

    //get pointer to the end of directory slots
    PageDirSlot* ptrEndOfDirSlot = (PageDirSlot*)((char*)dataPage + PAGE_SIZE - 2 * sizeof(unsigned int));

    //find out number of directory slots
    unsigned int numSlots = *((unsigned int*)ptrEndOfDirSlot);

    //check if rid is correct in terms of indexed slot
    if( rid.slotNum > numSlots )
    {
    	//deallocate data page
    	free(dataPage);

    	return -23; //rid is not setup correctly
    }

    //get slot
    PageDirSlot* curSlot = (PageDirSlot*)(ptrEndOfDirSlot - rid.slotNum - 1);

    //find slot offset
    unsigned int offRecord = curSlot->_offRecord;
    unsigned int szRecord = curSlot->_szRecord;

    //check if slot attributes make sense
    if( offRecord == 0 && szRecord == 0 )
    {
    	//deallocate data page
    	free(dataPage);

    	return -24;	//directory slot stores wrong information
    }

    //determine pointer to the record
    char* ptrRecord = (char*)(dataPage) + offRecord;

    //begin section for project 2: in case the record in this page is a TombStone
    if( szRecord == (unsigned int)-1 )
    {
    	//redirect to a different location; (page, slot) is specified in the record's body
    	RID* ptrNewRid = (RID*)ptrRecord;

    	//now go ahead and try to read this record
    	return readRecord(fileHandle, recordDescriptor, *ptrNewRid, data);
    }
    //end section for project 2

    //copy record contents
    memcpy(data, ptrRecord, szRecord);

    //deallocate data page
    free(dataPage);

    //return success
    return errCode;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    //declare constant iterator for descriptor values
	vector<Attribute>::const_iterator i = recordDescriptor.begin();

	//current pointer for iterating within data
	const void* ptr = data;

	//mark start of record with character "<"
	std::cout << "<";

	//loop through all descriptors using aforementioned iterator
	for(; i != recordDescriptor.end(); i++)
	{
		//insert commas between elements (except for the first one, that is when ptr == data)
		if( ptr != data )
		{
			std::cout << ", ";
		}

		int ival = 0;
		float fval = 0.0f;
		char* carr = NULL;

		//depending on the type of element, do a separate conversion and printing actions
		switch(i->type)
		{
		case TypeInt:
			//get integer by casting current pointer to an integer array and grabbing the first element (NOTE: ptr is the current offset within data)
			ival = ((int*)ptr)[0];

			//print out value and type
			std::cout << ival << ":Integer";

			//increment PTR by size of integer
			ptr = (void*)((char*)ptr + sizeof(int));

			break;
		case TypeReal:
			//get real (i.e. float) by casting current pointer offset of data to a float array and grabbing the first element
			fval = ((float*)ptr)[0];

			//print out value and type
			std::cout << fval << ":Real";

			//increment PTR by size of float
			ptr = (void*)((char*)ptr + sizeof(float));

			break;
		case TypeVarChar:
			//this element is slightly more complex, i.e. it is a tuple <int, char*>:
			//first element of tuple is a size of character array, which is a second element of tuple

			//get the first element, i.e. integer (size of char-array)
			ival = ((int*)ptr)[0];

			//increment PTR to the start of the second element
			ptr = (void*)((char*)ptr + sizeof(int));

			//create a character array of this size
			carr = (char*)malloc(ival + 1);
			memset(carr, 0, ival + 1);

			//copy content of the second element into this array
			memcpy((void*)carr, ptr, ival);

			//print out character array and type
			std::cout << carr << ":VarChar";

			//increment PTR by iVal (i.e. size of char-array)
			ptr = (void*)((char*)ptr + ival);

			//free char-array
			free(carr);

			break;
		}

	}

	//mark end of record with character ">"
	std::cout << ">" << std::endl;

	//success => return 0
	return 0;

}

/*
 * determine size of the record in bytes
**/
unsigned int sizeOfRecord(const vector<Attribute> &recordDescriptor, const void *data)
{
	unsigned int size = 0, szOfCharArr = 0;

	//loop through record's descriptor and identify size of each field
	vector<Attribute>::const_iterator i = recordDescriptor.begin();
	for(; i != recordDescriptor.end(); i++)
	{
		switch( i->type )
		{
		case TypeInt:
			//sum field size of int
			size += sizeof(int);

			//increment to the next field
			data = (void*)((char*)data + sizeof(int));
			break;
		case TypeReal:
			//sum field size of float
			size += sizeof(float);

			//increment to the next field
			data = (void*)((char*)data + sizeof(float));
			break;
		case TypeVarChar:
			//get integer that represent size of char-array and add it to the size
			szOfCharArr = ((unsigned int*)(data))[0];
			size += sizeof(int) + szOfCharArr;

			//increment to the next field
			data = (void*)((char*)data + sizeof(int) + szOfCharArr);
			break;
		}
	}

	//return size
	return size;
}

/////////////////////////////////PROJECT_2///RBFM_PART

RC RecordBasedFileManager::deleteRecords(FileHandle &fileHandle)
{
	RC errCode = 0;

	//allocate array for storing contents of the data page
	void* dataPage = malloc(PAGE_SIZE);
	memset(dataPage, 0, PAGE_SIZE);

	//define page index
	PageNum pagenum = 0;

	//determine max number of pages
	PageNum maxpagenum = (PageNum) fileHandle.getNumberOfPages();

	//loop thru file pages => null each one's content and write it back to file
	while(pagenum < maxpagenum)
	{

		if( (errCode = fileHandle.writePage(pagenum, dataPage)) != 0 )
		{
			//free data page
			free(dataPage);

			//return error
			return errCode;
		}

	}

	//free data page
	free(dataPage);

	//return success
	return errCode;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
	RC errCode = 0;

	//allocate buffer for storing page data
	void* dataPage = malloc(PAGE_SIZE);

	//read a data page pointed by rid
	if( (errCode = fileHandle.readPage(rid.pageNum, dataPage)) != 0 )
	{
		//free data page
		free(dataPage);

		//return error code
		return errCode;
	}

	/*
	 * data page has a following format:
	 * [list of records without any spaces in between][free space for records][list of directory slots][(number of slots):unsigned int][(offset from page start to the start of free space):unsigned int]
	 * ^                                                                      ^                       ^                                                                                                 ^
	 * start of page                                                          start of dirSlot        end of dirSlot                                                                          end of page
	 */
	//determine pointer to the end of the list of directory slots
	PageDirSlot* endOfDirSlot = (PageDirSlot*)((char*)dataPage + PAGE_SIZE - sizeof(unsigned int) - sizeof(unsigned int));

	//find proper slot number pointed by rid
	PageDirSlot* curDirSlot = endOfDirSlot - rid.slotNum;

	//null the contents of this slot
	curDirSlot->_offRecord = 0;
	curDirSlot->_szRecord = 0;

	//write back the page contents
	if( (errCode = fileHandle.writePage(rid.pageNum, dataPage)) != 0 )
	{
		//free data page
		free(dataPage);

		//return error code
		return errCode;
	}

	//free data page
	free(dataPage);

	//return success
	return errCode;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
	RC errCode = 0;

	//allocate buffer for storing page data pointed by rid
	void* dataPage = malloc(PAGE_SIZE);

	//read a data page
	if( (errCode = fileHandle.readPage(rid.pageNum, dataPage)) != 0 )
	{
		//free data page
		free(dataPage);

		//return error code
		return errCode;
	}

	/*
	 * data page has a following format:
	 * [list of records without any spaces in between][free space for records][list of directory slots][(number of slots):unsigned int][(offset from page start to the start of free space):unsigned int]
	 * ^                                                                      ^                       ^                                                                                                 ^
	 * start of page                                                          start of dirSlot        end of dirSlot                                                                          end of page
	 */
	//determine pointer to the end of the list of directory slots
	PageDirSlot* endOfDirSlot = (PageDirSlot*)((char*)data + PAGE_SIZE - sizeof(unsigned int) - sizeof(unsigned int));

	//get a pointer to the current slot
	PageDirSlot* curDirSlot = endOfDirSlot - rid.slotNum;

	//get a pointer to the "old record"
	char* oldRecord = (char*)data + curDirSlot->_offRecord;

	//determine if the sizes of the old record (stored in a file) and a new one (stored in data) are the same
	unsigned int newSize = sizeOfRecord(recordDescriptor, data), oldSize = curDirSlot->_szRecord;

	//if size is the same, then just replace content of the old record (in the file) with the newly arrived one => it should fit inside
	if( newSize == oldSize )
	{

		//copy contents of data into the "old record"
		memcpy(oldRecord, data, newSize);

		//write back the data page
		if( (errCode = fileHandle.writePage(rid.pageNum, dataPage)) != 0 )
		{
			//free data page
			free(dataPage);

			//return error code
			return errCode;
		}

		//free data page
		free(dataPage);

		//return success
		return errCode;
	}

	/*
	 * if size is not the same, then:
	 *	=> insert a new record to which TombStone will be pointing to
	 *	=> change entry in directory slot <offset remains the same, size becomes -1>, i.e. the illegal value (possibly need to change former functions to account for this)
	 * 	=> insert a TombStone into the body of record
	**/

	/*
	 * test case:
	 * 	=> check if original record is smaller in size than a TombStone
	 * 		* if larger then proceed by the plan
	 * 		* if smaller than check how much free space is in the file page
	 * 			+ if size left + size of original record is enough to store TombStone, then
	 * 				~ change the size parameter in the entry of original record to a required size
	 * 				~ reorganize page (the new size recorded in the record will ensure that after reorganization record will have sufficient space)
	 * 					=> should aside of re-organizing records to move free space to the end of page, it also should:
	 * 						update meta data in this page (start of free space should be changed)
	 * 						update header that points to this page (free space left in this page should be changed)
	 * 				~ insert TombStone into the place of the original record's body
	 * 			+ if size left + size of original record is NOT enough
	 * 				~ fail (cannot move to a different page, since that will cause change of RID)
	**/
	//if size of original record is smaller than a size of TombStone (which is 8 bytes), then
	if( oldSize < TOMBSTONE_SIZE )
	{

		//number of slots
		unsigned int* ptrNumSlots = (unsigned int*)(endOfDirSlot);

		//pointer to the start of free space
		char* ptrToFreeSpace = (char*)((char*)data + *( (unsigned int*)( (char*)(endOfDirSlot) + sizeof(unsigned int) ) ));

		//pointer to the start of the list of directory slots
		PageDirSlot* startOfDirSlot = (PageDirSlot*)( endOfDirSlot - (*ptrNumSlots) );

		//determine size of free space in this page
		unsigned int szOfFreeSpace = (unsigned int)((char*)startOfDirSlot - ptrToFreeSpace);

		//if size of free space left combined with the size of original record is still not enough, then
		if( szOfFreeSpace + oldSize < TOMBSTONE_SIZE )
		{
			//Q: should I try to reorganize page, and if the size is still small then fail?

			//free data page
			free(dataPage);

			//fail
			return -25;
		}

		//change size parameter in the entry of original record to a size of TombStone
		curDirSlot->_szRecord = TOMBSTONE_SIZE;

		//reorganize page
		reorganizePage(fileHandle, recordDescriptor, rid.pageNum);

		//update oldRecord variable, since now it is likely to be at different position within this page
		oldRecord = (char*)data + curDirSlot->_offRecord;

	}

	//insert a new record
	RID newRid = {0,0};
	if( (errCode = insertRecord(fileHandle, recordDescriptor, data, newRid)) != 0 )
	{
		//free data page
		free(dataPage);

		//return error code
		return errCode;
	}

	//change entry in directory slot <offset remains the same, size becomes -1>, i.e. the illegal value
	curDirSlot->_szRecord = (unsigned int)-1;

	//insert a TombStone into the body of record, which is a tuple <page number, slot number>, which is essentially RID
	RID* tombStone = (RID*)oldRecord;
	tombStone->pageNum = newRid.pageNum;
	tombStone->slotNum = newRid.slotNum;

	//free data page
	free(dataPage);

	//return success
	return errCode;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data)
{
	RC errCode = 0;

	//check if data is not NULL
	if( data == NULL )
	{
		return -11; //data is corrupted
	}

	//allocate buffer of the maximum record size
	char* recordBuf = malloc(MAX_SIZE_OF_RECORD);

	if( (errCode = readRecord(fileHandle, recordDescriptor, rid, recordBuf)) != 0 )
	{
		//free record
		free(recordBuf);

		//return error code
		return errCode;
	}

	//set errCode to failure, so ONLY if requested attributed is found, it would be changed to success (0), otherwise by default would be a failure (-26)
	errCode = -26;

	//loop thru record description to find the attribute with specified name
	vector<Attribute>::const_iterator i = recordDescriptor.begin(), max = recordDescriptor.end();
	char* curPtr = recordBuf;
	for( ; i != max; i++ )
	{
		//check if current vector element is the attribute we are looking for
		if( (*i).name == attributeName )
		{
			//define variable to store size
			unsigned int sz = 0;

			//determine size of requested attribute
			switch((*i).type)
			{
			case TypeInt:
				sz = sizeof(int);
				break;
			case TypeReal:
				sz = sizeof(float);
				break;
			case TypeVarChar:
				sz = (unsigned int)*curPtr;
				curPtr += sizeof(int);
				break;
			}

			//copy attribute to data
			memcpy(data, curPtr, sz);

			//change error code to success
			errCode = 0;

			break;
		}

		//increment current pointer within record buffer to the next element
		switch((*i).type)
		{
		case TypeInt:
			curPtr += sizeof(int);
			break;
		case TypeReal:
			curPtr += sizeof(float);
			break;
		case TypeVarChar:
			curPtr += sizeof(int) + ((unsigned int)*curPtr);
			break;
		}
	}

	//free record buffer
	free(recordBuf);

	//return success/failure
	return errCode;
}

RC RecordBasedFileManager::reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber)
{
	RC errCode = 0;

	/*
	 * TODO: (readRecord changes are complete)
	 * 	reorganizePage should account for two sources of "file holes":
	 * 			when record is deleted
	 * 				size = 0
	 * 			when record is moved (i.e. updated) and a TombStone is left in its place (TombStone is likely to be smaller than the record size)
	 * 				size = -1 and offset = ### (size is equal to a TOMBSTONE_SIZE)
	**/

	//check that page number has legal value
	if( pageNumber >= fileHandle.getNumberOfPages() )
	{
		return -23;	//given page number exceeds total number of pages in a file
	}

	//allocate buffer to hold a copy of page
	char* buffer = malloc(PAGE_SIZE);

	//read the page data that is to be re-organized
	if( (errCode = fileHandle.readPage(pageNumber, buffer)) != 0 )
	{
		//deallocate buffer
		free(buffer);

		//return error code
		return errCode;
	}

	/*
	 * data page has a following format:
	 * [list of records without any spaces in between][free space for records][list of directory slots][(number of slots):unsigned int][(offset from page start to the start of free space):unsigned int]
	 * ^                                                                      ^                       ^                                                                                                 ^
	 * start of page                                                          start of dirSlot        end of dirSlot                                                                          end of page
	 */
	//determine pointer to the end of the list of directory slots
	PageDirSlot* endOfDirSlot = (PageDirSlot*)((char*)buffer + PAGE_SIZE - sizeof(unsigned int) - sizeof(unsigned int));

	//number of slots
	unsigned int* ptrNumSlots = (unsigned int*)(endOfDirSlot);

	//pointer to the start of free space
	unsigned int* ptrVarForFreeSpace = (unsigned int*)( (char*)(endOfDirSlot) + sizeof(unsigned int) );
	char* ptrToFreeSpace = (char*)( (char*)buffer + (*ptrVarForFreeSpace) );

	//pointer to the start of the list of directory slots
	PageDirSlot* startOfDirSlot = (PageDirSlot*)( endOfDirSlot - (*ptrNumSlots) );

	//create duplicate page, which would store re-organized page data (a.k.a. new page buffer)
	char* reorganizedPage = new malloc(PAGE_SIZE);

	//null it
	memset(reorganizedPage, 0, PAGE_SIZE);

	//copy variable that stores number of slots
	//cannot change position of slots, since we have to retain RID consistency
	*((unsigned int*)(reorganizedPage + PAGE_SIZE - 2 * sizeof(unsigned int))) = *ptrNumSlots;

	//free space in a page may change, due to reorganization of records, so we have to compute it first and later place the value in the meta-data of new page buffer
	unsigned int freeSpace = PAGE_SIZE - 2 * sizeof(unsigned int);

	//loop thru directory slots and copy data "appropriately" into reorganizedPage buffer
	PageDirSlot *curSlot = startOfDirSlot,
			*curSlotInNewPageBuffer = (PageDirSlot*)((char*)reorganizedPage + PAGE_SIZE - 2 * sizeof(unsigned int)) - (*ptrNumSlots);

	//maintain offset of the record in the reorganized page (a.k.a. new page buffer)
	unsigned int offOfRecord = 0;

	while(curSlot != endOfDirSlot)
	{
		//size of record in the reorganized page (a.k.a new page buffer)
		//unsigned int szOfRecord = 0; //size goes into directory slot section, and it does not actually change:
		//								for deleted record it remains zero, for TombStone it remains unsigned version of -1, and for regular record it remains the size of the record

		//place record into the new page buffer (if needed)
		/*if( curSlot->_szRecord == 0 )	//if size is zero, then this record has been deleted
		{
			//leave both size and offset equal to zero
		}
		else if( curSlot->_szRecord == (unsigned int)-1 )	//if size is unsigned version of '-1', then this record has been moved (i.e. it contains a TOMBSTONE)
		{
			szOfRecord = TOMBSTONE_SIZE;
		}
		else  //regular record, just copy it
		{
			szOfRecord = curSlot->_szRecord;
		}*/

		//if record has not been deleted, then
		if( curSlot->_szRecord > 0 )
		{
			//copy contents of record
			memcpy(reorganizedPage + offOfRecord, buffer + curSlot->_offRecord, curSlot->_szRecord);
		}

		//update free space counter
		freeSpace -= curSlot->_szRecord - sizeof(PageDirSlot);

		//place page directory slot (always, even for deleted records, since this will ensure RID consistency, i.e. slot # will not be altered)
		curSlotInNewPageBuffer->_szRecord = curSlot->_szRecord;
		curSlotInNewPageBuffer->_offRecord = offOfRecord;

		//increment to the next slot
		curSlot++;

		//increment offset within reorganized page
		offOfRecord += curSlot->_szRecord;
	}

	//place free space into appropriate attribute in the meta-data
	*ptrVarForFreeSpace = freeSpace;

	//replace old page contents with reorganized copy
	if( (errCode = fileHandle.writePage(pageNumber, reorganizedPage)) != 0 )
	{
		//free both buffers (old and reorganized data)
		free(reorganizedPage);
		free(buffer);

		//return error code
		return errCode;
	}

	//free reorganized page buffer
	free(reorganizedPage);

	//free buffer that was holding a copy of page
	free(buffer);

	//return success
	return errCode;
}

//TODO: scan
//TODO: reorganizeFile
