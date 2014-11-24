#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"
#include "ixtest_util.h"

IndexManager *indexManager;
/*
void test(IXFileHandle fileHandle)
{
	unsigned int i = 0;
	for( ; i < 1023; i++ )
	{
		unsigned int key = i % 10;
		BucketDataEntry me = (BucketDataEntry){key, (RID){i / 10 + 1, (unsigned int)'A'}};
		MetaDataSortedEntries mdse(fileHandle, 1, (unsigned int)key, (void*)&me);
		mdse.insertEntry();

		//print file
		//if( i % 339 == 0 || i % 340 == 0 || i % 680 == 0 )
		//{
		//	std::cout << endl << "primary data file:" << endl;
		//	printFile(fileHandle._primBucketDataFileHandler);
		//	std::cout << endl << "overflow data file:" << endl;
		//	printFile(fileHandle._overBucketDataFileHandler);
		//}
	}
	i = 0;
	for( ; i < 1023; i+=10 )
	{
		unsigned int key = i % 10;
		BucketDataEntry me = (BucketDataEntry){key, (RID){i / 10 + 1, (unsigned int)'A'}};
		MetaDataSortedEntries mdse(fileHandle, 1, (unsigned int)key, (void*)&me);
		mdse.deleteEntry(me._rid);
		std::cout << endl << "primary data file:" << endl;
		printFile(fileHandle._primBucketDataFileHandler);
		std::cout << endl << "overflow data file:" << endl;
		printFile(fileHandle._overBucketDataFileHandler);
	}
}

void test2(IXFileHandle fileHandle, unsigned int N)
{
	RC errCode = 0;
	unsigned int i = 0;
	//insert entries in a bucket 0
	for( ; i < MAX_BUCKET_ENTRIES_IN_PAGE; i++ )
	{
		unsigned int key = i * N;	//try with 0
		BucketDataEntry me = (BucketDataEntry){key, (RID){i + 1, (unsigned int)'A'}};
		MetaDataSortedEntries mdse(fileHandle, 0, (unsigned int)key, (void*)&me);
		mdse.insertEntry();

	}
	i = 0;
	//insert entries in a bucket 1
	for( ; i < MAX_BUCKET_ENTRIES_IN_PAGE * 3 + 1; i++ )
	{
		unsigned int key = i * N + 1;	//try with 1
		BucketDataEntry me = (BucketDataEntry){key, (RID){i + 1, (unsigned int)'A'}};
		MetaDataSortedEntries mdse(fileHandle, 1, (unsigned int)key, (void*)&me);
		mdse.insertEntry();

		if( i == 0 ||
			i == MAX_BUCKET_ENTRIES_IN_PAGE - 1 || i == MAX_BUCKET_ENTRIES_IN_PAGE || i == MAX_BUCKET_ENTRIES_IN_PAGE + 1||
			i == MAX_BUCKET_ENTRIES_IN_PAGE * 2 - 1 || i == MAX_BUCKET_ENTRIES_IN_PAGE * 2 || i == MAX_BUCKET_ENTRIES_IN_PAGE * 2 + 1 ||
			i == MAX_BUCKET_ENTRIES_IN_PAGE * 3 - 1 || i == MAX_BUCKET_ENTRIES_IN_PAGE * 3 )
		{
			std::cout << endl << "primary data file:" << endl;
			printFile(fileHandle._primBucketDataFileHandler);
			std::cout << endl << "overflow data file:" << endl;
			printFile(fileHandle._overBucketDataFileHandler);
		}
	}
	std::cout << endl << "meta data file:" << endl;
	printFile(fileHandle._metaDataFileHandler);
	std::cout << endl << "primary data file:" << endl;
	printFile(fileHandle._primBucketDataFileHandler);
	std::cout << endl << "overflow data file:" << endl;
	printFile(fileHandle._overBucketDataFileHandler);
	i = MAX_BUCKET_ENTRIES_IN_PAGE * 3;
	for( ; i >= 1; i-- )
	{
		unsigned int key = i * N + 1;
		BucketDataEntry me = (BucketDataEntry){key, (RID){i + 1, (unsigned int)'A'}};
		MetaDataSortedEntries mdse(fileHandle, 1, (unsigned int)key, (void*)&me);
		if( (errCode = mdse.deleteEntry(me._rid)) != 0 )
		{
			cout << "error : " << errCode;
			exit(errCode);
		}
		if( i == MAX_BUCKET_ENTRIES_IN_PAGE - 1 || i == MAX_BUCKET_ENTRIES_IN_PAGE || i == MAX_BUCKET_ENTRIES_IN_PAGE + 1||
			i == MAX_BUCKET_ENTRIES_IN_PAGE * 2 - 1 || i == MAX_BUCKET_ENTRIES_IN_PAGE * 2 || i == MAX_BUCKET_ENTRIES_IN_PAGE * 2 + 1 ||
			i == MAX_BUCKET_ENTRIES_IN_PAGE * 3 - 1 || i == MAX_BUCKET_ENTRIES_IN_PAGE * 3 )
		{
			std::cout << endl << "primary data file:" << endl;
			printFile(fileHandle._primBucketDataFileHandler);
			std::cout << endl << "overflow data file:" << endl;
			printFile(fileHandle._overBucketDataFileHandler);
		}
	}
}

void test3(IXFileHandle fileHandle, unsigned int N)
{
	RC errCode = 0;
	unsigned int i = 0;
	//insert
	for( ; i < MAX_BUCKET_ENTRIES_IN_PAGE * 3; i++ )
	{
		unsigned int key = i * N + 1;	//try with 0
		BucketDataEntry me = (BucketDataEntry){key, (RID){i + 1, (unsigned int)'A'}};
		MetaDataSortedEntries mdse(fileHandle, 1, (unsigned int)key, (void*)&me);
		mdse.insertEntry();
	}
	i = 0;
	//print
	std::cout << endl << "primary data file:" << endl;
	printFile(fileHandle._primBucketDataFileHandler);
	std::cout << endl << "overflow data file:" << endl;
	printFile(fileHandle._overBucketDataFileHandler);
	//delete
	for( i = MAX_BUCKET_ENTRIES_IN_PAGE - 2; i < MAX_BUCKET_ENTRIES_IN_PAGE * 3; i+=2 )
	{
		unsigned int key = i * N + 1;
		BucketDataEntry me = (BucketDataEntry){key, (RID){i + 1, (unsigned int)'A'}};
		MetaDataSortedEntries mdse(fileHandle, 1, (unsigned int)key, (void*)&me);
		if( (errCode = mdse.deleteEntry(me._rid)) != 0 )
		{
			cout << "error : " << errCode;
			exit(errCode);
		}
		if( i % 100 == 0 )
		{
			//print
			std::cout << endl << "primary data file:" << endl;
			printFile(fileHandle._primBucketDataFileHandler);
			std::cout << endl << "overflow data file:" << endl;
			printFile(fileHandle._overBucketDataFileHandler);
		}
	}
	//print
	std::cout << endl << "primary data file:" << endl;
	printFile(fileHandle._primBucketDataFileHandler);
	std::cout << endl << "overflow data file:" << endl;
	printFile(fileHandle._overBucketDataFileHandler);
}
*/

void test1(IXFileHandle fileHandle)
{
	Attribute attr = (Attribute){"key", TypeVarChar, 80};
	void* key = malloc(80);
	memset(key, 0, 80);
	RC errCode = 0;
	RID rid = (RID){0, 0};
	int i = 0;
	int imax = 320;	//280
	for (i = 0; i < imax; i++)
	{
		unsigned int szOfCharArray = i % 60 + 6;
		((unsigned int*)key)[0] = szOfCharArray;
		for(int k = 0; k < (int)szOfCharArray; k++)
		{
			((char*)key)[4 + k] = 'a' + k;
		}
		rid.pageNum = i % 60;
		rid.slotNum = i;
		if( (errCode = indexManager->insertEntry(fileHandle, attr, key, rid)) != 0 )
		{
			IX_PrintError(errCode);
			exit(errCode);
		}
		if( i % 10 == 0 )
		{
			std::cout << endl << "primary data file:" << endl;
			printFile(fileHandle._primBucketDataFileHandler);
			std::cout << endl << "overflow data file:" << endl;
			printFile(fileHandle._overBucketDataFileHandler);
		}
	}

	//determine number of buckets
	unsigned int numOfPrimaryPages = 0;
	if( (errCode = indexManager->getNumberOfPrimaryPages(fileHandle, numOfPrimaryPages)) != 0 )
	{
		IX_PrintError(errCode);
		exit(errCode);
	}

	//print all pages in all buckets
	for(i = 0; i < (int)numOfPrimaryPages; i++)
	{
		std::cout << "=======================================\nbucket # " << i << endl;
		if( (errCode = indexManager->printIndexEntriesInAPage(fileHandle, attr, i)) != 0 )
		{
			IX_PrintError(errCode);
			exit(errCode);
		}
	}

	//delete some of the entries
	for(i = 15; i < imax-15; i++)
	{
		unsigned int szOfCharArray = i % 60 + 6;
		((unsigned int*)key)[0] = szOfCharArray;
		for(int k = 0; k < (int)szOfCharArray; k++)
		{
			((char*)key)[4 + k] = 'a' + k;
		}
		rid.pageNum = i % 60;
		rid.slotNum = i;

		if( (errCode = indexManager->deleteEntry(fileHandle, attr, key, rid)) != 0 )
		{
			IX_PrintError(errCode);
			exit(errCode);
		}
	}

	//print all pages in all buckets
	for(i = 0; i < (int)numOfPrimaryPages; i++)
	{
		std::cout << "=======================================\nbucket # " << i << endl;
		if( (errCode = indexManager->printIndexEntriesInAPage(fileHandle, attr, i)) != 0 )
		{
			IX_PrintError(errCode);
			exit(errCode);
		}
	}

	free(key);
}

void testPFME(IXFileHandle fileHandle)
{
	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	BUCKET_NUMBER bktNumber = 0;
	PFMExtension pfme = PFMExtension(fileHandle, bktNumber);

	vector<Attribute> vAttr;
	vAttr.push_back( (Attribute){"key", TypeVarChar, 10} );
	vAttr.push_back( (Attribute){"rid_page", TypeInt, 4} );
	vAttr.push_back( (Attribute){"rid_slot", TypeInt, 4} );
	void* data = malloc(100);//25);
	memset(data, 0, 100);//25);
	((unsigned int*)data)[0] = 3;
	((char*)data)[4] = 'a';
	((char*)data)[5] = 'b';
	((char*)data)[6] = 'c';
	((unsigned int*)( (char*)data + 7 ))[0] = 255;
	((unsigned int*)( (char*)data + 7 ))[1] = 255;

	RC errCode = 0;
	//vector<RID> rids;

	for( int i = 0; i < 320; i++ )
	{
		unsigned int szOfCharArray = i % 10 + 10;
		((unsigned int*)data)[0] = szOfCharArray;
		for( int k = 0; k < (int)szOfCharArray; k++ )
		{
			((char*)data)[4 + k] = 'a' + k;
		}
		//change
		((unsigned int*)( (char*)data + 4 + szOfCharArray ))[0] = i % 256;
		((unsigned int*)( (char*)data + 4 + szOfCharArray ))[1] = i;

		unsigned int length = 4 + szOfCharArray + 4 + 4;

		//insert
		//if( (errCode = rbfm->insertRecord(fileHandle._overBucketDataFileHandler, vAttr, data, rid)) != 0 )
		//PageNumbers:
		// 0 -> 9 => page 1 (slots 0 -> 9 all new, insert at the end)
		// 10 -> 19 => page 2 (slots 0 -> 9 all new, insert at the end)
		// 20 -> 29 => page 3 (slots 0 -> 9 all new, insert at the end)
		// 30 -> 39 => page 4 (slots 0 -> 9 all new, insert at the end)
		// 40 -> 49 => page 1 (slots 0 -> 9 => need to shift)
		// 50 -> 59 => page 2 (slots 0 -> 9 => need to shift)
		// 60 -> 69 => page 3 (slots 0 -> 9 => need to shift)
		// 70 -> 79 => page 4 (slots 0 -> 9 => need to shift)
		// 80 -> 89 => page 1 (slots 0 -> 9 => need to shift)
		// 90 -> 99 => page 2 (slots 0 -> 9 => need to shift)
		// 100 -> 109 => page 3 (slots 0 -> 9 => need to shift)
		// 110 -> 119 => page 4 (slots 0 -> 9 => need to shift)
		//rid = (RID){/*(i / 10)*/0 % 4, i % 10};
		if( i % 10 == 0 && i > 0 )
		{
			cout << "i = " << i;
		}

		bool newPage = false;

		if( (errCode = pfme.insertTuple(data, length, bktNumber, 0, ( i < 1 ? i : 1 ), newPage )) != 0 )//rid.pageNum, rid.slotNum)) )
		{
			cout << "error code: " << errCode;
			exit(errCode);
		}
		//rids.push_back(rid);
	}

	cout << "overflow bucket: " << endl;
	printFile(fileHandle._overBucketDataFileHandler);
	cout << "primary bucket: " << endl;
	printFile(fileHandle._primBucketDataFileHandler);

	//setup overflow map in IX fileHandler
	//fileHandle._info->_overflowPageIds.insert( std::pair<BUCKET_NUMBER, map<int, PageNum> >(bktNumber, std::map<int, PageNum>() ) );
	//fileHandle._info->_overflowPageIds[bktNumber].insert( std::pair<int, PageNum>(0, 1) );
	//fileHandle._info->_overflowPageIds[bktNumber].insert( std::pair<int, PageNum>(1, 2) );
	//fileHandle._info->_overflowPageIds[bktNumber].insert( std::pair<int, PageNum>(2, 3) );
	//fileHandle._info->_overflowPageIds[bktNumber].insert( std::pair<int, PageNum>(3, 4) );

	cout << fileHandle._info->_overflowPageIds[bktNumber].size() << endl;

	int j = 0;

	unsigned int pageNum = 0, slotNum = 0;

	for( ; j < 320; j++ )
	{
		memset(data, 0, 100);//25);

		int failures = 0;
		while(failures < 2)
		{
			if( (errCode = pfme.getTuple(data, bktNumber, pageNum, slotNum )) != 0 )
			{
				cout << endl << "next page" << endl;
				pageNum++;
				slotNum = 0;
				failures++;
				//cout << "error code: " << errCode << endl;
				//exit(errCode);
				continue;
			}
			else
			{
				failures = 0;
				slotNum++;
				break;
			}
		}
		if( failures == 2 )
			break;
		//rid.pageNum = rids[j].pageNum;
		//rid.slotNum = rids[j].slotNum;
		//if( (errCode = rbfm->readRecord(fileHandle._overBucketDataFileHandler, vAttr, rid, data)) != 0 )
		//{
		//	cout << "error code: " << errCode << endl;
		//	exit(errCode);
		//}
		if( (errCode = rbfm->printRecord(vAttr, data)) != 0 )
		{
			cout << "error code: " << errCode << endl;
			exit(errCode);
		}
	}

	cout << endl << endl << "AFTER THE READS" << endl << endl;
	cout << "overflow bucket: " << endl;
	printFile(fileHandle._overBucketDataFileHandler);
	cout << "primary bucket: " << endl;
	printFile(fileHandle._primBucketDataFileHandler);

	for( j = 0; j < 320-30; j++ )	//15
	{
		if( j == 184 || j == 185 )
		{
			cout << "j = " << j << endl;
		}

		bool lastPageIsEmpty = false;
		if( (errCode = pfme.deleteTuple(bktNumber, 0, 5, lastPageIsEmpty)) != 0 )
		{
			cout << "error code: " << errCode;
			exit(errCode);
		}

		//cout << "primary bucket: " << endl;
		//printFile(fileHandle._primBucketDataFileHandler);

		//if( (errCode = pfme.shiftRecordsToStart(0, 2, 1)) != 0 )
		//{
		//	cout << "error code: " << errCode;
		//	exit(errCode);
		//}

		//cout << "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||" << endl;
		//cout << "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||" << endl;
		//cout << "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||" << endl;
		//printFile(fileHandle._overBucketDataFileHandler);
	}

	cout << "overflow bucket: " << endl;
	printFile(fileHandle._overBucketDataFileHandler);
	cout << "primary bucket: " << endl;
	printFile(fileHandle._primBucketDataFileHandler);

	pageNum = 0;
	slotNum = 0;

	for( j = 0; j < 30; j++ )	//320 - 15
	{
		memset(data, 0, 100);//25);
		int failures = 0;
		while(failures < 2)
		{
			if( (errCode = pfme.getTuple(data, bktNumber, pageNum, slotNum )) != 0 )
			{
				cout << endl << "next page" << endl;
				pageNum++;
				slotNum = 0;
				failures++;
				//cout << "error code: " << errCode << endl;
				//exit(errCode);
				continue;
			}
			else
			{
				failures = 0;
				slotNum++;
				break;
			}
		}
		if( failures == 2 )
			break;
		//rid.pageNum = rids[j].pageNum;
		//rid.slotNum = rids[j].slotNum;
		//if( (errCode = rbfm->readRecord(fileHandle._overBucketDataFileHandler, vAttr, rid, data)) != 0 )
		//{
		//	cout << "error code: " << errCode << endl;
		//	exit(errCode);
		//}
		if( (errCode = rbfm->printRecord(vAttr, data)) != 0 )
		{
			cout << "error code: " << errCode << endl;
			exit(errCode);
		}
	}

}

int testCase_1(const string &indexFileName)
{
    // Functions tested
    // 1. Create Index File **
    // 2. Open Index File **
    // 3. Get number of Primary Pages **
    // 4. Get number of All Pages including overflow pages **
    // 5. Create Index File -- when index file is already created **
    // 6. Close Index File **
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 1****" << endl;

    RC rc;
    IXFileHandle ixfileHandle;
    unsigned numberOfPages = 4;
	unsigned numberOfPagesFromFunction = 0;
	
    // create index file
    rc = indexManager->createFile(indexFileName, numberOfPages);
    if(rc == success)
    {
        cout << "Index File Created!" << endl;
    }
    else
    {
        cout << "Failed Creating Index File..." << endl;
        return fail;
    }

    // open index file
    rc = indexManager->openFile(indexFileName, ixfileHandle);
    if(rc == success)
    {
        cout << "Index File, " << indexFileName << " Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index File..." << endl;
        return fail;
    }

    //testPFME(ixfileHandle);
    test1(ixfileHandle);

    //test3(ixfileHandle, numberOfPages);

    //test2(ixfileHandle, numberOfPages);

    /*test(ixfileHandle);

    std::cout << endl << "primary data file:" << endl;
	printFile(ixfileHandle._primBucketDataFileHandler);
	std::cout << endl << "overflow data file:" << endl;
	printFile(ixfileHandle._overBucketDataFileHandler);

    if( (rc = indexManager->closeFile(ixfileHandle)) != 0 )
    	cout << "error: " << rc;
	if( (rc = indexManager->openFile(indexFileName, ixfileHandle)) != 0 )
		cout << "error: " << rc;
	std::cout << endl << "meta data file:" << endl;
	printFile(ixfileHandle._metaDataFileHandler);
     */
    //add by me
	std::cout << "meta file:" << endl << endl;
	printFile(ixfileHandle._metaDataFileHandler);
	std::cout << "overflow file:" << endl << endl;
	printFile(ixfileHandle._overBucketDataFileHandler);
	std::cout << "primary file:" << endl << endl;
	printFile(ixfileHandle._primBucketDataFileHandler);

    rc = indexManager->getNumberOfAllPages(ixfileHandle, numberOfPagesFromFunction);
    if(rc == success)
    {
        if (numberOfPagesFromFunction < numberOfPages) {
        	cout << "Number of initially constructed pages is not correct." << endl;
        	return fail;
        }
    }
    else
    {
        cout << "Could not get the number of pages." << endl;
        return fail;
    }
    
    numberOfPagesFromFunction = 0;
    rc = indexManager->getNumberOfPrimaryPages(ixfileHandle, numberOfPagesFromFunction);
    if(rc == success)
    {
        if (numberOfPagesFromFunction != numberOfPages) {
        	cout << "Number of initially constructed pages is not correct." << endl;
        	return fail;
        }
    }
    else
    {
        cout << "Could not get the number of pages." << endl;
        return fail;
    }	
	
    // create duplicate index file
    rc = indexManager->createFile(indexFileName, numberOfPages);
    if(rc != success)
    {
        cout << "Duplicate Index File not Created -- correct!" << endl;
    }
    else
    {
        cout << "Duplicate Index File Created -- failure..." << endl;
        return fail;
    }

    //add by me
    std::cout << "meta file:" << endl << endl;
    printFile(ixfileHandle._metaDataFileHandler);
    std::cout << "overflow file:" << endl << endl;
    printFile(ixfileHandle._overBucketDataFileHandler);
    std::cout << "primary file:" << endl << endl;
    printFile(ixfileHandle._primBucketDataFileHandler);

    // close index file
    rc = indexManager->closeFile(ixfileHandle);
    if(rc == success)
    {
        cout << "Index File Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Index File..." << endl;
        return fail;
    }
    //destroy index file
    rc = indexManager->destroyFile(indexFileName);	//added by me (should be removed later)
    if( rc == success )
    {
    	cout << "Index File destroyed Successfully!" << endl;
    }
    else
    {
    	cout << "Failed destroying Index File..." << endl;
    	return fail;
    }
    return success;
}

int main()
{
    //Global Initializations
    indexManager = IndexManager::instance();

    //added by me
    Attribute attr = (Attribute){"field-name", AttrType(2), 100};
    void* key = malloc(7);	//[length:4]['a':1]['b':1]['c':1] => 7 characters
    ((unsigned int*)key)[0] = 3;
    ((char*)key)[4] = 'a';
    ((char*)key)[5] = 'b';
    ((char*)key)[6] = 'c';
    indexManager->hash(attr, key);

	const string indexFileName = "age_idx";

    RC result = testCase_1(indexFileName);
    if (result == success) {
    	cout << "IX_Test Case 1 passed" << endl;
    	return success;
    } else {
    	cout << "IX_Test Case 1 failed" << endl;
    	return fail;
    }

}

