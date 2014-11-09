
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
 * -23 = rid is not setup correctly
 * -24 = directory slot stores wrong information
 * -25 = cannot update record without change of rid (no space within the given page, and cannot move to a different page!)
 * -26 = no requested attribute was found inside the record
 * -27 = page number exceeds the total number of pages in a file
**/

RC RecordBasedFileManager::createFile(const string &fileName) {
	//error value
	RC errCode = 0;

	const char* cArrFileName = fileName.c_str();

	//create an empty file using pfm (i.e. PagedFileManager)
	if( (errCode = _pfm->createFile( cArrFileName )) != 0 )
	{
		return errCode;
	}

	//create header
	if( (errCode = _pfm->createFileHeader( cArrFileName )) != 0 )
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

	//write the number of pages back to the header
	fileHandle.writeBackNumOfPages();

	//close a file
	if( (errCode = _pfm->closeFile( fileHandle )) != 0 )
	{
		return errCode;
	}

	//success => return 0
	return errCode;
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

	//assign number of slots to a passed variable by reference
	slotNum = (*ptrNumSlots) - 1;

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

		//update slot number (going from the last to the first)
		slotNum--;
	}

	//if there is not available page directory slot, then "reserve next" (get a pointer to it)
	if( curSlot == endOfDirSlot )
	{
		//assign a new slot
		slotNum = *ptrNumSlots;

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

	//if size of free space is not enough return -22 (because it was suppose to be enough, since this page was found by method getPage)
	if( szOfFreeSpace < szRecord )
	{
		//lower number of assigned slots
		*ptrNumSlots -= 1;

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

void RecordBasedFileManager::decodeRecord(
		const vector<Attribute>& recordDescriptor,
		const void* encodedRecordData,
		unsigned int& decodedSize,
		void* decodedRecordData)
{
	//get number of fields in this record
	unsigned int numFields = *((unsigned int*)encodedRecordData);

	//calculate size of record directory
	unsigned int szOfDir = sizeof(unsigned int) * (numFields + 1);

	//loop thru record fields (both data and attributes)

	//setup parameters for the loop
	std::vector<Attribute>::const_iterator iterator = recordDescriptor.begin(), max = recordDescriptor.end();
	const void *curDir = (void*)((char*)encodedRecordData + sizeof(unsigned int)),
			   *ptrInEncodedData = (void*)((char*)encodedRecordData + szOfDir + sizeof(unsigned int));
	void* ptrInDecodedRecord = decodedRecordData;

	//loop
	for( ; iterator != max; iterator++ )
	{
		//calculate size of the record field
		unsigned int szOfField = 0;
		switch(iterator->type)
		{
		case TypeInt:
			szOfField = sizeof(unsigned int);
			break;
		case TypeReal:
			szOfField = sizeof(float);
			break;
		case TypeVarChar:
			szOfField = *( ((unsigned int*)curDir) + 1 ) - *( (unsigned int*)curDir );	//next offset - current offset

			//only write out data into the decoded buffer, if the field is not deleted
			if( iterator->length > 0 )
			{
				//place length into decoded record data
				*((unsigned int*)ptrInDecodedRecord) = szOfField;

				//update size of decoded record by size of the integer that stores field length
				decodedSize += sizeof(unsigned int);

				//update pointer
				ptrInDecodedRecord = (void*)((char*)ptrInDecodedRecord + sizeof(unsigned int));
			}

			break;
		}

		//only write out data into the decoded buffer, if the field is not deleted
		if( iterator->length > 0 )
		{
			//update size of decoded record by size of the field
			decodedSize += szOfField;

			//copy data
			memcpy(ptrInDecodedRecord, ptrInEncodedData, szOfField);

			//update decoded pointer
			ptrInDecodedRecord = (void*)((char*)ptrInDecodedRecord + szOfField);
		}

		//update pointers
		curDir = (void*)((char*)curDir + sizeof(unsigned int));
		ptrInEncodedData = (void*)((char*)ptrInEncodedData + szOfField);
	}
}

void RecordBasedFileManager::encodeRecord(
		const std::vector<Attribute> &recordDescriptor,
		const void* originalRecordData,
		const unsigned int origSize,
		unsigned int& newSzOfRecord,
		void* newRecordData)
{
	//new record structure includes 3 components
	//[number of fields:integer][directory of field offsets:List<integer>][list of fields]

	//calculate size of "record directory of offsets"
	unsigned int szOfDir = sizeof(unsigned int) * (recordDescriptor.size() + 1);	//one extra offset "points" at the end of the last record

	//loop thru record fields (both data and attributes)

	//set the new size of the record to 0
	newSzOfRecord = 0;

	//setup parameters for the loop
	std::vector<Attribute>::const_iterator iterator = recordDescriptor.begin(), max = recordDescriptor.end();
	void *ptrEncRecordData = (void*)((char*)newRecordData + szOfDir), *ptrEncDirRecord = newRecordData;
	const void *ptrOrigRecord = originalRecordData;
	unsigned int curOffset = szOfDir;

	//place integer representing number of fields into the start of the record (which is
	//pointed by ptrEncDirRecord, and then increment all pointers by the size of integer)
		//copy number of fields into the record's body as a first parameter
		*((unsigned int*)ptrEncDirRecord) = recordDescriptor.size();
		//update both pointers by the size of the first field (i.e. number of fields in this record)
		ptrEncDirRecord = (void*)((char*)ptrEncDirRecord + sizeof(unsigned int));
		ptrEncRecordData = (void*)((char*)ptrEncRecordData + sizeof(unsigned int));
		//increment size of the decoded record
		newSzOfRecord += sizeof(unsigned int);
		curOffset += sizeof(unsigned int);

	//loop
	for( ; iterator != max; iterator++ )
	{
		//determine size of the field
		unsigned int szOfField = 0;
		switch( iterator->type )
		{
		case TypeInt:
			szOfField = sizeof(unsigned int);
			break;
		case TypeReal:
			szOfField = sizeof(float);
			break;
		case TypeVarChar:
			//if the field is inside this record (i.e. not dropped) then get length from it
			if( iterator->length > 0 )
			{
				szOfField = *((unsigned int*)ptrOrigRecord);

				//update pointer of original record (so that it points at data; right now it points at its length)
				ptrOrigRecord = (void*)((char*)ptrOrigRecord + sizeof(unsigned int));
			}
			else
			{
				//otherwise, this field VarChar has been dropped, so place a "default" value => empty string
				//empty string in our record field format is [length:integer=0][empty string=""] => total size of sizeOf(integer) = 4 bytes
				szOfField = 0;
			}
			break;
		}

		//update size of the encoded record by the current field
		newSzOfRecord += sizeof(unsigned int) + szOfField;

		//write offset of field in the "record directory of offsets"
		*((unsigned int*)ptrEncDirRecord) = curOffset;

		//update offset
		curOffset += szOfField;

		//if the field has been dropped, we still want to keep it inside the record structure, but it would occupy space ONLY inside the record directory
		//no actual data will be written in at the record's data body
		if( szOfField > 0 )
		{
			//write field into appropriate spot in encoded record
			memcpy(ptrEncRecordData, ptrOrigRecord, szOfField);
		}

		//update pointers
		ptrEncRecordData = (void*)((char*)ptrEncRecordData + szOfField);
		ptrEncDirRecord = (void*)((char*)ptrEncDirRecord + sizeof(unsigned int));
		ptrOrigRecord = (void*)((char*)ptrOrigRecord + szOfField);
	}

	//write the last offset
	*((unsigned int*)ptrEncDirRecord) = curOffset;

	//update size of the current field
	newSzOfRecord += sizeof(unsigned int);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *origData, RID &rid) {
	RC errCode = 0;

	//check if data is not NULL
	if( origData == NULL )
	{
		return -11; //data is corrupted
	}

	//encode record
	void* encData = malloc(PAGE_SIZE);
	unsigned int szOfEncRecord = 0;
	encodeRecord(recordDescriptor, origData, sizeOfRecord(recordDescriptor, origData), szOfEncRecord, encData);

	//check if data is not greater than a max allowed space within the page
	if( szOfEncRecord >= MAX_SIZE_OF_RECORD )
	{
		return -21;	//record exceeds page size (less required page meta-data)
	}

	unsigned int freeSpaceLeft = 0;

	//get page that contains the necessary amount of free space
	PageNum datapagenum = 0, headerpagenum = 0;
	if( (errCode = _pfm->getDataPage(fileHandle, szOfEncRecord, datapagenum, headerpagenum, freeSpaceLeft)) != 0 )
	{
		//free buffer used for encoded record
		free(encData);

		//return error code
		return errCode;
	}

	//get the place for the new record in the found data page
	PageDirSlot pds;
	unsigned int slotNum = 0;
	if( (errCode = findRecordSlot(fileHandle, datapagenum, szOfEncRecord, pds, slotNum, freeSpaceLeft)) != 0 )
	{
		//free buffer used for encoded record
		free(encData);

		//return error code
		return errCode;
	}

	//read data of the data page
	char* dataPage = (char*)malloc(PAGE_SIZE);
	memset(dataPage, 0, PAGE_SIZE);
	if( (errCode = fileHandle.readPage(datapagenum, dataPage)) != 0 )
	{
		//free buffer used for encoded record
		free(encData);
		free(dataPage);

		//return error code
		return errCode;
	}

	//get pointer to the record
	char* ptrRecord = (char*)dataPage + pds._offRecord;

	//copy data of the record
	memcpy(ptrRecord, encData, szOfEncRecord);

	//write page to the file
	if( (errCode = fileHandle.writePage(datapagenum, dataPage)) != 0 )
	{
		//free buffer used for encoded record
		free(encData);
		free(dataPage);

		//return error code
		return errCode;
	}

	//deallocate dataPage
	free(dataPage);
	free(encData);

	//assign rid
	rid.pageNum = datapagenum;
	rid.slotNum = slotNum;

	//success return 0
	return 0;
}

RC RecordBasedFileManager::readEncodedRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    RC errCode = 0;

    if( data == NULL )
    {
    	return -11; //data is corrupted
    }

    if( rid.pageNum == 0 || rid.pageNum >= fileHandle.getNumberOfPages() )
    {
    	return -27; //rid is not setup correctly
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
    if( rid.slotNum >= numSlots )
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
    	errCode = readEncodedRecord(fileHandle, recordDescriptor, *ptrNewRid, data);

    	//free data page (fixing memory leak)
    	free(dataPage);

    	return errCode;
    }
    //end section for project 2

    //copy encoded record contents
    memcpy(data, ptrRecord, szRecord);

    //deallocate data page
    free(dataPage);

    //return success
    return errCode;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
{
	RC errCode = 0;

	//allocate buffer for storing encoded record
	void* encDataRecord = malloc(PAGE_SIZE);

	//read encoded record
	if( (errCode = readEncodedRecord(fileHandle, recordDescriptor, rid, encDataRecord)) != 0 )
	{
		return errCode;
	}

	//decode record
	unsigned int decodedSz = 0;
	decodeRecord(recordDescriptor, encDataRecord, decodedSz, data);

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
			if( i->length > 0 )
			{
				//get integer by casting current pointer to an integer array and grabbing the first element (NOTE: ptr is the current offset within data)
				ival = ((int*)ptr)[0];

				//print out value and type
				std::cout << ival << ":Integer";
			}

			//increment PTR by size of integer
			ptr = (void*)((char*)ptr + sizeof(int));

			break;
		case TypeReal:
			if( i->length > 0 )
			{
				//get real (i.e. float) by casting current pointer offset of data to a float array and grabbing the first element
				fval = ((float*)ptr)[0];

				//print out value and type
				std::cout << fval << ":Real";
			}

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

			if( i->length > 0 )
			{
				//create a character array of this size
				carr = (char*)malloc(ival + 1);
				memset(carr, 0, ival + 1);

				//copy content of the second element into this array
				memcpy((void*)carr, ptr, ival);

				//print out character array and type
				std::cout << carr << ":VarChar";

				//free char-array
				free(carr);
			}

			//increment PTR by iVal (i.e. size of char-array)
			ptr = (void*)((char*)ptr + ival);

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

		//set number of pages to be equal to 1
		if( pagenum == 0 )
		{
			((Header*)dataPage)->_totFileSize = 1;
		}

		if( (errCode = fileHandle.writePage(pagenum, dataPage)) != 0 )
		{
			//free data page
			free(dataPage);

			//return error
			return errCode;
		}

		//go to next page
		pagenum++;

	}

	//free data page
	free(dataPage);

	//set the meta info
	fileHandle._info->_numPages = 1;

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
	PageDirSlot* curDirSlot = endOfDirSlot - (rid.slotNum + 1);

	//update header page
	//define variable to store id of the header page
	PageNum headerPage = 0;

	//get header for this data page
	if( (errCode = _pfm->findHeaderPage(fileHandle, rid.pageNum, headerPage)) != 0 )
	{
		//free buffers
		free(dataPage);

		//return error code
		return errCode;
	}

	//set free size for this data page inside the appropriate header page
	void* data = malloc(PAGE_SIZE);
	memset(data, 0, PAGE_SIZE);

	//get the found header page
	if( (errCode = fileHandle.readPage((PageNum)headerPage, data)) != 0 )
	{
		//deallocate data
		free(data);

		//return error
		return errCode;
	}

	//cast data to Header
	Header* hPage = (Header*)data;

	//set amount of free space for the data page inside this header
	(hPage->_arrOfPageIds)[rid.pageNum % NUM_OF_PAGE_IDS]._numFreeBytes += sizeof(PageDirSlot);

	//write back to header
	if( (errCode = fileHandle.writePage(headerPage, data)) != 0 )
	{
		//free data page
		free(dataPage);

		//return error code
		return errCode;
	}

	//free data buffer used for header page
	free(data);

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

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *origData, const RID &rid)
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
	PageDirSlot* endOfDirSlot = (PageDirSlot*)((char*)dataPage + PAGE_SIZE - sizeof(unsigned int) - sizeof(unsigned int));

	//get a pointer to the current slot
	PageDirSlot* curDirSlot = endOfDirSlot - (rid.slotNum + 1);

	//get a pointer to the "old record"
	char* oldRecord = (char*)dataPage + curDirSlot->_offRecord;

	//encode record
	void* encRecordData = malloc(PAGE_SIZE);
	unsigned int szOfEncRecord = 0;
	encodeRecord(recordDescriptor, origData, sizeOfRecord(recordDescriptor, origData), szOfEncRecord, encRecordData);

	//newSize = sizeOfRecord(recordDescriptor, data),
	//determine if the sizes of the old record (stored in a file) and a new one (stored in data) are the same
	unsigned int oldSize = curDirSlot->_szRecord;

	//if size is the same, then just replace content of the old record (in the file) with the newly arrived one => it should fit inside
	if( szOfEncRecord <= oldSize )	//correction made: if old size is equal OR greater than go ahead and use the space of the original record
	{

		//copy contents of data into the "old record"
		memcpy(oldRecord, encRecordData, szOfEncRecord);

		//write back the data page
		if( (errCode = fileHandle.writePage(rid.pageNum, dataPage)) != 0 )
		{
			//free data page
			free(dataPage);
			free(encRecordData);

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
		char* ptrToFreeSpace = (char*)((char*)dataPage + *( (unsigned int*)( (char*)(endOfDirSlot) + sizeof(unsigned int) ) ));

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
			free(encRecordData);

			//fail
			return -25;
		}

		//change size parameter in the entry of original record to a size of TombStone
		curDirSlot->_szRecord = TOMBSTONE_SIZE;

		//reorganize page
		reorganizePage(fileHandle, recordDescriptor, rid.pageNum);

		//update oldRecord variable, since now it is likely to be at different position within this page
		oldRecord = (char*)dataPage + curDirSlot->_offRecord;

	}

	//insert a new record
	RID newRid = {0,0};
	if( (errCode = insertRecord(fileHandle, recordDescriptor, origData, newRid)) != 0 )
	{
		//free data page
		free(dataPage);
		free(encRecordData);

		//return error code
		return errCode;
	}

	//change entry in directory slot <offset remains the same, size becomes -1>, i.e. the illegal value
	curDirSlot->_szRecord = (unsigned int)-1;

	//insert a TombStone into the body of record, which is a tuple <page number, slot number>, which is essentially RID
	RID* tombStone = (RID*)oldRecord;
	tombStone->pageNum = newRid.pageNum;
	tombStone->slotNum = newRid.slotNum;

	//write back the data page
	if( (errCode = fileHandle.writePage(rid.pageNum, dataPage)) != 0 )
	{
		//free data page
		free(dataPage);
		free(encRecordData);

		//return error code
		return errCode;
	}

	//free data page
	free(dataPage);
	free(encRecordData);

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
	char* recordBuf = (char*) malloc(MAX_SIZE_OF_RECORD);

	if( (errCode = readEncodedRecord(fileHandle, recordDescriptor, rid, recordBuf)) != 0 )
	{
		//free record
		free(recordBuf);

		//return error code
		return errCode;
	}

	//set errCode to failure, so ONLY if requested attributed is found, it would be changed to success (0), otherwise by default would be a failure (-26)
	errCode = -26;
	unsigned int fieldIndex = 0;

	//loop thru record description to find the attribute with specified name
	vector<Attribute>::const_iterator i = recordDescriptor.begin(), max = recordDescriptor.end();
	//char* curPtr = recordBuf;
	for( ; i != max; i++ )
	{
		//check if current vector element is the attribute we are looking for
		if( (*i).name == attributeName )
		{
			/*
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
				sz = *((unsigned int*)curPtr) + sizeof(unsigned int);
				//curPtr += sizeof(int);
				break;
			}

			//copy attribute to data
			memcpy(data, curPtr, sz);
			*/

			//new structure of the record is as follows:
			//[number of fields:integer][directory of field offsets:List<Integers>][fields]

			//get the very first element in this record, i.e. number of the fields in this record
			unsigned int numOfFieldsInThisRecord = *((unsigned int*)recordBuf);

			//need to check if this field actually represented by the value
			if( numOfFieldsInThisRecord <= fieldIndex )
			{
				//if the field index is out of bound, then it must mean that this field was added and the record was never updated
				//this also means that the field ideally should be returned as a NULL. In our class, there is no NULL data type, so
				//my guess is to return a default value for the given data type, i.e. "" for VarChar, 0 for Integer, 0.0f for Float.
				switch( (*i).type )
				{
				case TypeInt:
					*((unsigned int*)data) = 0;
					break;
				case TypeReal:
					*((float*)data) = 0;
					break;
				case TypeVarChar:
					//write the length
					*((unsigned int*)data) = 0;
					//no character array follows, since this is an empty string (i.e. length = 0)
					break;
				}

				//free the used buffer
				free(recordBuf);

				//success
				return 0;
			}

			//determine offset from the start of the page to the requested attribute (i.e. field)
			unsigned int *curOffset = ((unsigned int*)(recordBuf)) + fieldIndex + 1;	//extra one is added because the first element in the record is the number of the fields stored within this record

			//calculate size of attribute
			unsigned int szOfAttribute = *(curOffset + 1) - *curOffset;

			//calculate start of the attribute
			void* startOfAttribute = (void*)((char*)recordBuf + *curOffset);

			//if this attribute is character array (i.e. VarChar), then need to copy length and then contents of character array
			if( (*i).type == TypeVarChar )
			{
				//copy length first
				*((unsigned int*)data) = szOfAttribute;

				//increment pointer in data
				data = (void*)((char*)data + sizeof(unsigned int));
			}

			//copy contents of attribute to the buffer
			memcpy(data, startOfAttribute, szOfAttribute);

			//change error code to success
			errCode = 0;

			break;
		}
		//increment field index
		fieldIndex++;
	}

	//free record buffer
	free(recordBuf);

	//return success/failure
	return errCode;
}

RC RecordBasedFileManager::reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber)
{
	RC errCode = 0;

	//check that page number has legal value
	if( pageNumber >= fileHandle.getNumberOfPages() )
	{
		return -27;	//given page number exceeds total number of pages in a file
	}

	//allocate buffer to hold a copy of page
	char* buffer = (char*) malloc(PAGE_SIZE);

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
	unsigned int* ptrVarForFreeSpace = (unsigned int*)( (char*)(endOfDirSlot) + sizeof(unsigned int) );	//not needed
	//char* ptrToFreeSpace = (char*)( (char*)buffer + (*ptrVarForFreeSpace) );	//not needed

	//pointer to the start of the list of directory slots
	PageDirSlot* startOfDirSlot = (PageDirSlot*)( endOfDirSlot - (*ptrNumSlots) );

	//create duplicate page, which would store re-organized page data (a.k.a. new page buffer)
	char* reorganizedPage = (char*) malloc(PAGE_SIZE);

	//null it
	memset(reorganizedPage, 0, PAGE_SIZE);

	//copy variable that stores number of slots
	//cannot change position of slots, since we have to retain RID consistency
	*((unsigned int*)(reorganizedPage + PAGE_SIZE - 2 * sizeof(unsigned int))) = *ptrNumSlots;

	//free space in a page may change, due to reorganization of records, so we have to compute it first and later place the value in the meta-data of new page buffer
	unsigned int freeSpace = PAGE_SIZE - 2 * sizeof(unsigned int);

	//loop thru directory slots and copy data "appropriately" into reorganizedPage buffer
	PageDirSlot *curSlot = endOfDirSlot,
			*curSlotInNewPageBuffer = (PageDirSlot*)((char*)reorganizedPage + PAGE_SIZE - 2 * sizeof(unsigned int));

	//maintain offset of the record in the reorganized page (a.k.a. new page buffer)
	unsigned int offOfRecord = 0;

	do
	{
		//increment to the next slot
		curSlot--;
		curSlotInNewPageBuffer--;

		//size of record in the reorganized page (a.k.a new page buffer)
		unsigned int szOfRecord = 0;

		//place record into the new page buffer (if needed)
		if( curSlot->_szRecord == 0 )	//if size is zero, then this record has been deleted
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
		}

		//if record has not been deleted, then
		if( curSlot->_szRecord > 0 )
		{
			//copy contents of record
			memcpy(reorganizedPage + offOfRecord, buffer + curSlot->_offRecord, szOfRecord);
		}

		//update free space counter
		freeSpace -= szOfRecord + sizeof(PageDirSlot);

		//place page directory slot (always, even for deleted records, since this will ensure RID consistency, i.e. slot # will not be altered)
		curSlotInNewPageBuffer->_szRecord = curSlot->_szRecord;
		curSlotInNewPageBuffer->_offRecord = offOfRecord;

		//increment offset within reorganized page
		offOfRecord += szOfRecord;
	} while(curSlot != startOfDirSlot);

	//set offset to the free space
	*( (unsigned int*)( (char*)reorganizedPage + PAGE_SIZE - sizeof(unsigned int) ) ) =
			PAGE_SIZE - freeSpace - (*ptrNumSlots) * sizeof(PageDirSlot) - 2 * sizeof(unsigned int);

	//if by reorganizing this data page, the free space increased, then it is necessary to update header page information
	if( freeSpace != *ptrVarForFreeSpace )
	{
		//variable to store id of the header page
		PageNum headerPage = 0;

		//get header for this data page
		if( (errCode = _pfm->findHeaderPage(fileHandle, pageNumber, headerPage)) != 0 )
		{
			//free buffers
			free(reorganizedPage);
			free(buffer);

			//return error code
			return errCode;
		}

		//set free size for this data page inside the appropriate header page
		void* data = malloc(PAGE_SIZE);
		memset(data, 0, PAGE_SIZE);

		//get the found header page
		if( (errCode = fileHandle.readPage((PageNum)headerPage, data)) != 0 )
		{
			//deallocate data
			free(reorganizedPage);
			free(buffer);
			free(data);

			//return error
			return errCode;
		}

		//cast data to Header
		Header* hPage = (Header*)data;

		//set amount of free space for the data page inside this header
		(hPage->_arrOfPageIds)[pageNumber % NUM_OF_PAGE_IDS]._numFreeBytes = freeSpace;

		//write back to header
		if( (errCode = fileHandle.writePage(headerPage, data)) != 0 )
		{
			//free data page
			free(reorganizedPage);
			free(buffer);
			free(data);

			//return error code
			return errCode;
		}

		//free data buffer used for header page
		free(data);
	}

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

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator)
{
	//Q: should I open a new instance of the file?, i.e. get new FileHandle

	//setup given iterator to point to the start of the file
	rbfm_ScanIterator._compO = compOp;
	rbfm_ScanIterator._conditionAttribute = conditionAttribute;
	rbfm_ScanIterator._fileHandle = fileHandle;
	rbfm_ScanIterator._pagenum = 1;
	rbfm_ScanIterator._recordDescriptor = recordDescriptor;
	rbfm_ScanIterator._slotnum = 0;
	rbfm_ScanIterator._value = value;
	//prior misunderstanding of the reason for attributeNames, lead me to belief that
	//the argument was a duplicate to recordDescriptor with the exception that it was
	//storing strings instead of attributes
	//As far as I can see, it stores a list of attributes, whose values needs to be
	//returned, so it is a subset of recordDescriptor, but it does not have to be
	//always equal to recordDescriptor.
	rbfm_ScanIterator._attributes = attributeNames;

	//return success
	return 0;
}

RC RecordBasedFileManager::reorganizeFile(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor){
	RC errCode = 0;
	const string tempFile="tempFile";

	//create an empty file using pfm
	if( (errCode = createFile(tempFile)) != 0 )
		return errCode;

	//create a handle for the file
	FileHandle tempFileHandle;
	if((errCode=openFile(tempFile.c_str(), tempFileHandle))!=0)
			return errCode;

	//create an iterator and its configuration
	RBFM_ScanIterator iterator;
	string conditionAttribute;
	CompOp compOp=NO_OP;
	vector<string> attributeNames;

	//set the attribute names
	for(unsigned int i=0; i<recordDescriptor.size(); i++)
		attributeNames.push_back(recordDescriptor[i].name);

	//set scan
	scan(fileHandle, recordDescriptor, conditionAttribute, compOp, NULL, attributeNames, iterator );


	//insert the records into temp file
	RID itRid = {0, 0};
	void* encData = malloc(PAGE_SIZE);
	memset(encData, 0, PAGE_SIZE);

	//allocate buffer for decoded data
	//void* decodedData = malloc(PAGE_SIZE);

	//create map for storing actual rids for those cases when records are tombStones
	std::map<RID, RID> tombStoneRids;

	vector<RID> rids;

	while(iterator.getNextRecord(itRid, encData) != RBFM_EOF)
	{

		//get rid (if it is a tombStone, then a returned rid is not going to equal to itRid)
		RID updatedRid = iterator.getActualRecordId();

		if( updatedRid.pageNum != 0 &&	//make sure that returned rid is actually correct
				(
				 updatedRid.pageNum != itRid.pageNum ||		//check if it got changed (i.e. a TombStone case)
				 updatedRid.slotNum != itRid.slotNum
				)
		  )
		{
			//insert rid pointed by TombStone
			tombStoneRids.insert(std::pair<RID, RID>(updatedRid, itRid));
		}

		//tombStoneRids.find(itRid);
		std::map<RID, RID>::iterator map_iter;

		//check if rid (from getNextRecord) points to the data that was already processed
		if( (map_iter = tombStoneRids.find(itRid)) != tombStoneRids.end() )
		{

			//if it was already processed, then do not include it again
			//and remove the rid from map
			tombStoneRids.erase(map_iter);
		}
		else
		{
			//due to the fact that record is encoded and insertRecord expects the data to be decoded, we have to first decode data that we want to insert
			//unsigned int szOfDecodedData = 0;
			//decodeRecord(recordDescriptor, encData, szOfDecodedData, decodedData);

			//otherwise, insert this record
			if( (errCode = insertRecord(tempFileHandle, recordDescriptor, /*decodedData*/encData, itRid)) != 0 )
			{
				//free buffer used for encoded and decoded data
				free(encData);
				//free(decodedData);

				//return error code
				return errCode;
			}

			rids.push_back(itRid);
		}

		//debug information (subject for later removal)
		//printRecord(recordDescriptor, data);

	}

	//delete records from the original file
	if((errCode=deleteRecords(fileHandle))!=0)
		return errCode;

	//bring back all records to the original file
	for(unsigned int i = 0; i < rids.size(); i++)
	{
		memset(/*decodedData*/encData, 0, PAGE_SIZE);
		if((errCode=readRecord(tempFileHandle, recordDescriptor, rids[i], /*decodedData*/encData)) != 0)
		{
			//free buffers used for encoded and decoded data
			free(encData);
			//free(decodedData);

			//return error code
			return errCode;
		}
		if((errCode=insertRecord(fileHandle, recordDescriptor, /*decodedData*/encData, itRid)) != 0)
		{
			//free buffers used for encoded and decoded data
			free(encData);
			//free(decodedData);

			//return error code
			return errCode;
		}
	}

	//close and destroy temporary file
	if((errCode=_pfm->closeFile(tempFileHandle)) != 0)
	{
		//free buffers used for encoded and decoded data
		free(encData);
		//free(decodedData);

		//return error code
		return errCode;
	}
	if((errCode=_pfm->destroyFile(tempFile.c_str())) != 0)
	{
		//free buffers used for encoded and decoded data
		free(encData);
		//free(decodedData);

		//return error code
		return errCode;
	}

	//free buffers used for encoded and decoded data
	free(encData);
	//free(decodedData);

	return errCode;

}

//RBFM_ScanIterator section of code

RBFM_ScanIterator::RBFM_ScanIterator()
{
	_compO = NO_OP;
	_conditionAttribute = "";
	_pagenum = 0;
	_recordDescriptor.clear();
	_slotnum = (unsigned int)-1;
	_value = NULL;
}

RBFM_ScanIterator::~RBFM_ScanIterator()
{
	//do nothing
}

RC RBFM_ScanIterator::close()
{
	_compO = NO_OP;
	_conditionAttribute = "";
	_pagenum = 0;
	_recordDescriptor.clear();
	_slotnum = (unsigned int)-1;
	_value = NULL;
	RC errCode = 0;
	if( (errCode = RecordBasedFileManager::instance()->closeFile(_fileHandle)) != 0 )
	{
		return errCode;
	}
	return 0;
}

RC	RBFM_ScanIterator::getNextRecord(RID &rid, void* data)
{
	RC errCode = 0;

	//check if data is setup correctly
	if( data == NULL )
	{
		return -11; //data is corrupted
	}

	//check if rid is at the end-of-file; if it is return RBFM_EOF
	if( _pagenum == 0 )
	{
		return RBFM_EOF;
	}

	//setup working instance of record based file manager
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

	//allocate space where record is stored
	void* curRecord = malloc(PAGE_SIZE);

	/*
	 * the general goal is to check whether the current record (pointed by rid) satisfies condition (given by scan function)
	 * 		=> if yes, then return data to the caller containing this record
	 * 		=> if no, then go to the next record and repeat the process
	 */

	//loop until the required record is found
	while( true )
	{
		//update rid values
		rid.pageNum = _pagenum;
		rid.slotNum = _slotnum;

		//read current record
		if( (errCode = rbfm->readRecord(_fileHandle, _recordDescriptor, rid, curRecord)) != 0 )
		{
			//if slot number in rid exceeds the maximum stored in this data page, then
			if( errCode == -23 )
			{
				//need to go to the next page
				_pagenum++;

				//set slot to the start of the page
				_slotnum = 0;

				//loop again
				continue;
			}

			//if page number exceeds the maximum stored in this file, then
			if( errCode == -27 )
			{
				//deallocate space assigned for record
				free(curRecord);

				//set internal page counter to 0, so that it this iterator is called again, it would quit immediately
				_pagenum = 0;

				//went thru entire file, i.e. cannot find the next record
				return RBFM_EOF;
			}

			//if the given slot points to the deleted record (i.e. we are stepping thru the file record-by-record, so
			//if some of them are deleted, it is possible to encounter this "error"
			if( errCode == -24 )
			{
				//go to the next slot
				_slotnum++;

				//loop again
				continue;
			}
		}

		//check whether this record satisfies given condition

		//loop thru record using stored record descriptor
		void* ptrField = curRecord;
		//std::map<string, AttrType> matchNameToSize;

		vector<Attribute>::iterator i = _recordDescriptor.begin(), max = _recordDescriptor.end();

		//loop thru record elements to determine if it is matching
		for( i = _recordDescriptor.begin(); i != max; i++ )
		{

			unsigned int szOfField = 0;

			switch( i->type )
			{
			case TypeInt:
				//setup size of field as integer
				szOfField = sizeof(int);
				break;
			case TypeReal:
				//setup size of field as float
				szOfField = sizeof(int);
				break;
			case TypeVarChar:
				//setup size of field as integer
				szOfField = ((unsigned int*)curRecord)[0];

				//skip the size and go to the character array
				ptrField = (void*)( (char*)ptrField + sizeof(int) );
				break;
			}

			//if this is a comparing field, then
			if( i->name == _conditionAttribute || _compO == NO_OP )
			{
				//define variable to store boolean result, whether condition matches
				bool isMatching = false;

				if( _compO != NO_OP )
				{
					int cmpValue = 0;
					if( i->type == TypeVarChar )
					{
						cmpValue = strncmp((char*)ptrField, (char*)_value, szOfField);
					}
					else
					{
						cmpValue = memcmp(ptrField, _value, szOfField);
					}

					//determine if condition matches
					switch(_compO)
					{
					case EQ_OP:
						isMatching = cmpValue == 0;
						break;
					case LT_OP:
						isMatching = cmpValue < 0;
						break;
					case GT_OP:
						isMatching = cmpValue > 0;
						break;
					case LE_OP:
						isMatching = cmpValue <= 0;
						break;
					case GE_OP:
						isMatching = cmpValue >= 0;
						break;
					case NE_OP:
						isMatching = cmpValue != 0;
						break;
					default:
						return -1;
					}
				}
				else
				{
					//iterate over all fields
					isMatching = true;
				}

				//if it matches, then
				if( isMatching )
				{
					//determine exact size of record
					//unsigned int szOfRecord = sizeOfRecord(_recordDescriptor, curRecord);

					//priorly misunderstood function: copy record to the data
					//memcpy(data, curRecord, szOfRecord);
					//copy only fields that are directly mentioned inside _attributes
					std::vector<string>::iterator selectAttrIter = _attributes.begin(),
							selectAttrEnd = _attributes.end();

					void* ptrOfCurRecord = curRecord;

					//loop thru fields that needs to be placed into select set
					for( ; selectAttrIter != selectAttrEnd; selectAttrIter++ )
					{
						//reset current record pointer
						curRecord = ptrOfCurRecord;

						//another alternative I can think of is to use readAttribute function which would be called from within this loop at every iteration
						//to read the record fields into the data buffer
						//loop thru all fields of record to determine proper starting offset of the selected attribute
						std::vector<Attribute>::iterator allAttrIter = _recordDescriptor.begin(), allAttrEnd = _recordDescriptor.end();
						for( ; allAttrIter != allAttrEnd; allAttrIter++ )
						{
							//add size of attribute to data, which stores the set of selected fields
							int attrSz = 0;
							switch(allAttrIter->type)
							{
							case AttrType(0):	//Integer
								attrSz = sizeof(int);
								//memcpy(data, curRecord, attrSz);
								break;
							case AttrType(1):	//Real
								attrSz = sizeof(float);
								//memcpy(data, curRecord, attrSz);
								break;
							case AttrType(2):	//VarChar
								attrSz = *((unsigned int*)curRecord) + sizeof(unsigned int);
								//memcpy(data, curRecord, attrSz);
								break;
							}

							//if the iterated attribute is the one selected, then
							if( allAttrIter->name == *selectAttrIter )
							{
								//copy it into the data buffer
								memcpy(data, curRecord, attrSz);

								//increment the data buffer pointer
								data = (void*)((char*)data + attrSz);

								//go to the next selected attribute
								break;
							}
							curRecord = (void*)((char*)curRecord + attrSz);
						}
					}

					//deallocate space for record
					free(ptrOfCurRecord);

					//increment internal slot counter to next item
					_slotnum++;

					//return success
					return 0;	//do not substitute 0 with errCode, since the later value is likely to be equal to -27 or -23 (see above)
				}

				//if it is not match, then go to next record
				break;
			}

			//increment to the next field
			ptrField = (void*)( (char*)ptrField + szOfField );
		}

		//go to the next slot
		_slotnum++;

	}

	//deallocate space for record
	free(curRecord);

	//return success
	return errCode;
}

RID RBFM_ScanIterator::getActualRecordId()
{
    RID actual_rid = {0,0};

    //check if internal data members make sense (i.e. page number is within [1, max pages-1]
    if( _pagenum == 0 || _pagenum >= _fileHandle.getNumberOfPages() )
    {
    	return actual_rid; //rid is not setup correctly
    }

    //allocate array for storing contents of the data page
    void* dataPage = malloc(PAGE_SIZE);
    memset(dataPage, 0, PAGE_SIZE);

    //read data page
    if(  _fileHandle.readPage(_pagenum, dataPage) != 0 )
    {
    	//deallocate dataPage
    	free(dataPage);

    	//read failed
    	return actual_rid;
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

    unsigned int olderSlotNumber = (_slotnum <= 0 ? 0 : _slotnum - 1);

    //check if rid is correct in terms of indexed slot
    if( olderSlotNumber >= numSlots )
    {
    	//deallocate data page
    	free(dataPage);

    	return actual_rid; //rid is not setup correctly
    }

    //get slot
    PageDirSlot* curSlot = (PageDirSlot*)(ptrEndOfDirSlot - olderSlotNumber - 1);

    //find slot offset
    unsigned int offRecord = curSlot->_offRecord;
    unsigned int szRecord = curSlot->_szRecord;

    //check if slot attributes make sense
    if( offRecord == 0 && szRecord == 0 )
    {
    	//deallocate data page
    	free(dataPage);

    	return actual_rid;	//directory slot is for the deleted record
    }

    //determine pointer to the record
    char* ptrRecord = (char*)(dataPage) + offRecord;

    //in case the record in this page is a TombStone
    if( szRecord == (unsigned int)-1 )
    {
    	//redirect to a different location; (page, slot) is specified in the record's body
    	RID* ptrNewRid = (RID*)ptrRecord;

    	//create buffer for reading the redirected page
    	void* redir_page = malloc(PAGE_SIZE);
    	memset(redir_page, 0, PAGE_SIZE);

    	//setup working instance of record based file manager
    	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    	//now go ahead and try to read this record
    	if( rbfm->readRecord(_fileHandle, _recordDescriptor, *ptrNewRid, redir_page) < 0 )
    	{
    		//free buffer for page
    		free(dataPage);
    		free(redir_page);

    		return actual_rid;
    	}

    	//check if pointer to new rid makes sense
    	if( ptrNewRid->pageNum == 0 || ptrNewRid->slotNum == (unsigned int)-1 )
    	{
    		//free buffer for page
    		free(dataPage);
    		free(redir_page);

    		return actual_rid;
    	}

    	//assign redirected rid
    	actual_rid.pageNum = ptrNewRid->pageNum;
    	actual_rid.slotNum = ptrNewRid->slotNum;
    }
    else
    {
    	actual_rid.pageNum = _pagenum;
    	actual_rid.slotNum = olderSlotNumber;
    }

    //free buffer for page
    free(dataPage);

    //return rid
    return actual_rid;
}

bool operator<(const RID& x, const RID& y)
{
	return x.pageNum < y.pageNum || x.slotNum < y.slotNum;
};

bool operator==(const RID& x, const RID& y)
{
	return x.pageNum == y.pageNum && x.slotNum == y.slotNum;
};
