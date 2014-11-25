#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <algorithm>

#include "ix.h"
#include "ixtest_util.h"

IndexManager *indexManager;

int testCase_9(const string &indexFileName, const Attribute &attribute)
{
    // Functions tested
    // 1. Create Index
    // 2. OpenIndex
    // 3. Insert entry
    // 4. Scan entries, and delete entries
    // 5. Scan close
    // 6. Insert entries again
    // 7. Scan entries
    // 8. CloseIndex
    // 9. DestroyIndex
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "****In Test Case 9****" << endl;

    RC rc;
    RID rid;
    IXFileHandle ixfileHandle;
    IX_ScanIterator ix_ScanIterator;
    int compVal;
    int numOfTuples;
    unsigned numberOfPages = 4;
    int A[30000];
    int B[20000];
    int count = 0;
    int key;

    //create index file(s)
    rc = indexManager->createFile(indexFileName, numberOfPages);
    if(rc == success)
    {
        cout << "Index Created!" << endl;
    }
    else
    {
        cout << "Failed Creating Index File..." << endl;
    	return fail;
    }

    //open index file
    rc = indexManager->openFile(indexFileName, ixfileHandle);
    if(rc == success)
    {
        cout << "Index File Opened!" << endl;
    }
    else
    {
        cout << "Failed Opening Index File..." << endl;
    	indexManager->destroyFile(indexFileName);
    	return fail;
    }

    // insert entry
    numOfTuples = 30000;
    for(int i = 0; i < numOfTuples; i++)
    {
        A[i] = i;
    }
    //random_shuffle(A, A+numOfTuples);	//for now

    for(int i = 0; i < numOfTuples; i++)
    {
        key = A[i];
        rid.pageNum = i+1;
        rid.slotNum = i+1;

        rc = indexManager->insertEntry(ixfileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        	indexManager->closeFile(ixfileHandle);
        	return fail;
        }
    }

						unsigned int numberOfPagesFromFunction = 0;
						// Get number of primary pages
						rc = indexManager->getNumberOfPrimaryPages(ixfileHandle, numberOfPagesFromFunction);
						if(rc != 0)
						{
							cout << "getNumberOfPrimaryPages() failed." << endl;
							//indexManager->closeFile(ixfileHandle);
							return -1;
						}

						// Print Entries in each page
						for (unsigned i = 0; i < numberOfPagesFromFunction; i++) {
							rc = indexManager->printIndexEntriesInAPage(ixfileHandle, attribute, i);
							if (rc != 0) {
								cout << "printIndexEntriesInAPage() failed." << endl;
								//indexManager->closeFile(ixfileHandle);
								return -1;
							}
						}

    //scan
    compVal = 20000;
    rc = indexManager->scan(ixfileHandle, attribute, NULL, &compVal, true, true, ix_ScanIterator);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else
    {
        cout << "Failed Opening Scan..." << endl;
    	indexManager->closeFile(ixfileHandle);
    	return fail;
    }

    // Test DeleteEntry in IndexScan Iterator
    					vector<int> buffer;
    count = 0;
    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
        if(count % 1000 == 0)
            cout << rid.pageNum << " " << rid.slotNum << endl;

        key = A[rid.pageNum-1];

        				cout << key << " , ";

        				buffer.push_back(key);

        rc = indexManager->deleteEntry(ixfileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed deleting entry in Scan..." << endl;
        	ix_ScanIterator.close();
        	return fail;
        }

						/*if( count % 10 == 0 )
						{
							cout << endl;
							numberOfPagesFromFunction = 0;
							// Get number of primary pages
							rc = indexManager->getNumberOfPrimaryPages(ixfileHandle, numberOfPagesFromFunction);
							if(rc != 0)
							{
								cout << "getNumberOfPrimaryPages() failed." << endl;
								//indexManager->closeFile(ixfileHandle);
								return -1;
							}

							// Print Entries in each page
							for (unsigned i = 0; i < numberOfPagesFromFunction; i++) {
								rc = indexManager->printIndexEntriesInAPage(ixfileHandle, attribute, i);
								if (rc != 0) {
									cout << "printIndexEntriesInAPage() failed." << endl;
									//indexManager->closeFile(ixfileHandle);
									return -1;
								}
							}
							cout << "end of printing" << endl;
							cout << "===========================================================" << endl;
							cout << "===========================================================" << endl;
						}*/

        count++;
    }

    					cout << "entries deleted=============================>" << endl;
    					vector<int>::iterator vit = buffer.begin(), vmax = buffer.end();
    					for( ; vit != vmax; vit++ )
    					{
    						cout << "[ " << *vit << " ]";
    					}
    					cout << "======================>end of deleted section" << endl;

						numberOfPagesFromFunction = 0;
						// Get number of primary pages
						rc = indexManager->getNumberOfPrimaryPages(ixfileHandle, numberOfPagesFromFunction);
						if(rc != 0)
						{
							cout << "getNumberOfPrimaryPages() failed." << endl;
							//indexManager->closeFile(ixfileHandle);
							return -1;
						}

						// Print Entries in each page
						for (unsigned i = 0; i < numberOfPagesFromFunction; i++) {
							rc = indexManager->printIndexEntriesInAPage(ixfileHandle, attribute, i);
							if (rc != 0) {
								cout << "printIndexEntriesInAPage() failed." << endl;
								//indexManager->closeFile(ixfileHandle);
								return -1;
							}
						}

    cout << "Number of deleted entries: " << count << endl;
    if (count != 20001)
    {
        cout << "Wrong entries output...failure" << endl;
    	ix_ScanIterator.close();
    	return fail;
    }

    //close scan
    rc = ix_ScanIterator.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Scan..." << endl;
    	indexManager->closeFile(ixfileHandle);
    	return fail;
    }

    // insert entry Again
    numOfTuples = 20000;
    for(int i = 0; i < numOfTuples; i++)
    {
        B[i] = 30000+i;
    }
    //random_shuffle(B, B+numOfTuples);	//for now

    for(int i = 0; i < numOfTuples; i++)
    {
        key = B[i];
        rid.pageNum = i+30001;
        rid.slotNum = i+30001;

        rc = indexManager->insertEntry(ixfileHandle, attribute, &key, rid);
        if(rc != success)
        {
            cout << "Failed Inserting Keys..." << endl;
        	indexManager->closeFile(ixfileHandle);
        	return fail;
        }
    }

						numberOfPagesFromFunction = 0;
						// Get number of primary pages
						rc = indexManager->getNumberOfPrimaryPages(ixfileHandle, numberOfPagesFromFunction);
						if(rc != 0)
						{
							cout << "getNumberOfPrimaryPages() failed." << endl;
							//indexManager->closeFile(ixfileHandle);
							return -1;
						}

						// Print Entries in each page
						for (unsigned i = 0; i < numberOfPagesFromFunction; i++) {
							rc = indexManager->printIndexEntriesInAPage(ixfileHandle, attribute, i);
							if (rc != 0) {
								cout << "printIndexEntriesInAPage() failed." << endl;
								//indexManager->closeFile(ixfileHandle);
								return -1;
							}
						}

    //scan
    compVal = 35000;
    rc = indexManager->scan(ixfileHandle, attribute, NULL, &compVal, true, true, ix_ScanIterator);
    if(rc == success)
    {
        cout << "Scan Opened Successfully!" << endl;
    }
    else
    {
        cout << "Failed Opening Scan..." << endl;
    	indexManager->closeFile(ixfileHandle);
    	return fail;
    }

    					buffer.clear();

    count = 0;
    while(ix_ScanIterator.getNextEntry(rid, &key) == success)
    {
        if (count % 1000 == 0)
            cout << rid.pageNum << " " << rid.slotNum << endl;

        				buffer.push_back(key);

        if(rid.pageNum > 30000 && B[rid.pageNum-30001] > 35000)
        {
            cout << "Wrong entries output...failure" << endl;
        	ix_ScanIterator.close();
        	return fail;
        }
        count ++;
    }

							cout << "entries deleted=============================>" << endl;
							vit = buffer.begin();
							vmax = buffer.end();
							for( ; vit != vmax; vit++ )
							{
								cout << "[ " << *vit << " ]";
							}
							cout << "======================>end of deleted section" << endl;

							numberOfPagesFromFunction = 0;
							// Get number of primary pages
							rc = indexManager->getNumberOfPrimaryPages(ixfileHandle, numberOfPagesFromFunction);
							if(rc != 0)
							{
								cout << "getNumberOfPrimaryPages() failed." << endl;
								//indexManager->closeFile(ixfileHandle);
								return -1;
							}

							// Print Entries in each page
							for (unsigned i = 0; i < numberOfPagesFromFunction; i++) {
								rc = indexManager->printIndexEntriesInAPage(ixfileHandle, attribute, i);
								if (rc != 0) {
									cout << "printIndexEntriesInAPage() failed." << endl;
									//indexManager->closeFile(ixfileHandle);
									return -1;
								}
							}


    cout << "Number of scanned entries: " << count << endl;

    //close scan
    rc = ix_ScanIterator.close();
    if(rc == success)
    {
        cout << "Scan Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Scan..." << endl;
    	indexManager->closeFile(ixfileHandle);
    	return fail;
    }

    //close index file(s)
    rc = indexManager->closeFile(ixfileHandle);
    if(rc == success)
    {
        cout << "Index File Closed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Closing Index File..." << endl;
    	indexManager->destroyFile(indexFileName);
    	return fail;
    }

    //destroy index file(s)
    rc = indexManager->destroyFile(indexFileName);
    if(rc == success)
    {
        cout << "Index File Destroyed Successfully!" << endl;
    }
    else
    {
        cout << "Failed Destroying Index File..." << endl;
    	return fail;
    }

    return success;

}

int main()
{
    //Global Initializations
    indexManager = IndexManager::instance();

	const string indexFileName = "age_idx";
	Attribute attrAge;
	attrAge.length = 4;
	attrAge.name = "age";
	attrAge.type = TypeInt;

	RC result = testCase_9(indexFileName, attrAge);
    if (result == success) {
    	cout << "IX_Test Case 9 passed" << endl;
    	return success;
    } else {
    	cout << "IX_Test Case 9 failed" << endl;
    	return fail;
    }

}
