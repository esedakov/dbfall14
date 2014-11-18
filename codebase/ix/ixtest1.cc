#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"
#include "ixtest_util.h"

IndexManager *indexManager;

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
		/*if( i % 339 == 0 || i % 340 == 0 || i % 680 == 0 )
		{
			std::cout << endl << "primary data file:" << endl;
			printFile(fileHandle._primBucketDataFileHandler);
			std::cout << endl << "overflow data file:" << endl;
			printFile(fileHandle._overBucketDataFileHandler);
		}*/
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

		//print file
		/*if( i % 339 == 0 || i % 340 == 0 || i % 680 == 0 )
		{
			std::cout << endl << "primary data file:" << endl;
			printFile(fileHandle._primBucketDataFileHandler);
			std::cout << endl << "overflow data file:" << endl;
			printFile(fileHandle._overBucketDataFileHandler);
		}*/
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

    test3(ixfileHandle, numberOfPages);

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

