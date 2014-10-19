/*#include <fstream>
#include <iostream>
#include <cassert>

#include "pfm.h"
#include "rbfm.h"

using namespace std;


void rbfTest()
{
  // PagedFileManager *pfm = PagedFileManager::instance();
  // RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

  // write your own testing cases here
}


int main() 
{
  cout << "test..." << endl;

  rbfTest();
  // other tests go here

  cout << "OK" << endl;
}*/


#include <iostream>
#include <string>
#include <cassert>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <stdexcept>
#include <stdio.h>
#include "pfm.h"
#include "rbfm.h"
using namespace std;
const int success = 0;
// Check if a file exists
bool FileExists(string fileName)
{
    struct stat stFileInfo;
    if(stat(fileName.c_str(), &stFileInfo) == 0) return true;
    else return false;
}
// Function to prepare the data in the correct form to be inserted/read
void prepareRecord(const int nameLength, const string &name, const int age, const float height, const int salary, void *buffer, int *recordSize)
{
    int offset = 0;

    memcpy((char *)buffer + offset, &nameLength, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)buffer + offset, name.c_str(), nameLength);
    offset += nameLength;

    memcpy((char *)buffer + offset, &age, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)buffer + offset, &height, sizeof(float));
    offset += sizeof(float);

    memcpy((char *)buffer + offset, &salary, sizeof(int));
    offset += sizeof(int);

    *recordSize = offset;
}
void prepareMyRecord(const int key, const int valLength, char c, void *buffer, int *recordSize)
{
    int offset = 0;

    string val;
    for(int i = 0; i < valLength; i++)
    	val.push_back(c);

    memcpy((char *)buffer + offset, &valLength, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)buffer + offset, val.c_str(), valLength);
    offset += valLength;

    memcpy((char *)buffer + offset, &key, sizeof(int));
    offset += sizeof(int);

    *recordSize = offset;
}
void prepareMyRecord2(const int ikey, const float fkey, const int valLength, char c, void *buffer, int *recordSize)
{
    int offset = 0;

    memcpy((char *)buffer + offset, &ikey, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)buffer + offset, &fkey, sizeof(float));
    offset += sizeof(float);

    string val;
    for(int i = 0; i < valLength; i++)
    	val.push_back(c);

    memcpy((char *)buffer + offset, &valLength, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)buffer + offset, val.c_str(), valLength);
    offset += valLength;

    *recordSize = offset;
}
void prepareLargeRecord(const int index, void *buffer, int *size)
{
    int offset = 0;

    // compute the count
    int count = index % 50 + 1;
    // compute the letter
    char text = index % 26 + 97;
    for(int i = 0; i < 10; i++)
    {
        memcpy((char *)buffer + offset, &count, sizeof(int));
        offset += sizeof(int);
        for(int j = 0; j < count; j++)
        {
            memcpy((char *)buffer + offset, &text, 1);
            offset += 1;
        }

        // compute the integer
        memcpy((char *)buffer + offset, &index, sizeof(int));
        offset += sizeof(int);

        // compute the floating number
        float real = (float)(index + 1);
        memcpy((char *)buffer + offset, &real, sizeof(float));
        offset += sizeof(float);
    }
    *size = offset;
}
void createRecordDescriptor(vector<Attribute> &recordDescriptor) {
    Attribute attr;
    attr.name = "EmpName";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)30;
    recordDescriptor.push_back(attr);
    attr.name = "Age";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);
    attr.name = "Height";
    attr.type = TypeReal;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);
    attr.name = "Salary";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);
}
void createLargeRecordDescriptor(vector<Attribute> &recordDescriptor)
{
    int index = 0;
    char *suffix = (char *)malloc(10);
    for(int i = 0; i < 10; i++)
    {
        Attribute attr;
        sprintf(suffix, "%d", index);
        attr.name = "attr";
        attr.name += suffix;
        attr.type = TypeVarChar;
        attr.length = (AttrLength)50;
        recordDescriptor.push_back(attr);
        index++;
        sprintf(suffix, "%d", index);
        attr.name = "attr";
        attr.name += suffix;
        attr.type = TypeInt;
        attr.length = (AttrLength)4;
        recordDescriptor.push_back(attr);
        index++;
        sprintf(suffix, "%d", index);
        attr.name = "attr";
        attr.name += suffix;
        attr.type = TypeReal;
        attr.length = (AttrLength)4;
        recordDescriptor.push_back(attr);
        index++;
    }
    free(suffix);
}
void createMyRecordDescriptor(vector<Attribute> &recordDescriptor)
{
	Attribute attr;
	attr.name = "key";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)4000;
	recordDescriptor.push_back(attr);
	attr.name = "val";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	recordDescriptor.push_back(attr);
}
void createMyRecordDescriptor2(vector<Attribute> &recordDescriptor, const int varCharLength)
{
	Attribute attr;
	attr.name = "ikey";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	recordDescriptor.push_back(attr);
	attr.name = "fkey";
	attr.type = TypeReal;
	attr.length = (AttrLength)4;
	recordDescriptor.push_back(attr);
	attr.name = "val";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)varCharLength;
	recordDescriptor.push_back(attr);
}
int RBFTest_1(PagedFileManager *pfm)
{
    // Functions Tested:
    // 1. Create File
    cout << "****In RBF Test Case 1****" << endl;
    RC rc;
    string fileName = "test";
    // Create a file named "test"
    rc = pfm->createFile(fileName.c_str());
    assert(rc == success);
    if(FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been created." << endl << endl;
    }
    else
    {
        cout << "Failed to create file!" << endl;
        cout << "Test Case 1 Failed!" << endl << endl;
        return -1;
    }
    // Create "test" again, should fail
    rc = pfm->createFile(fileName.c_str());
    assert(rc != success);
    cout << "Test Case 1 Passed!" << endl << endl;
    return 0;
}
int RBFTest_2(PagedFileManager *pfm)
{
    // Functions Tested:
    // 1. Destroy File
    cout << "****In RBF Test Case 2****" << endl;
    RC rc;
    string fileName = "test";
    rc = pfm->destroyFile(fileName.c_str());
    assert(rc == success);
    if(!FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been destroyed." << endl << endl;
        cout << "Test Case 2 Passed!" << endl << endl;
        return 0;
    }
    else
    {
        cout << "Failed to destroy file!" << endl;
        cout << "Test Case 2 Failed!" << endl << endl;
        return -1;
    }
}
int RBFTest_3(PagedFileManager *pfm)
{
    // Functions Tested:
    // 1. Create File
    // 2. Open File
    // 3. Get Number Of Pages
    // 4. Close File
    cout << "****In RBF Test Case 3****" << endl;
    RC rc;
    string fileName = "test_1";
    // Create a file named "test_1"
    rc = pfm->createFile(fileName.c_str());
    assert(rc == success);
    if(FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been created." << endl;
    }
    else
    {
        cout << "Failed to create file!" << endl;
        cout << "Test Case 3 Failed!" << endl << endl;
        return -1;
    }
    // Open the file "test_1"
    FileHandle fileHandle;
    rc = pfm->openFile(fileName.c_str(), fileHandle);
    assert(rc == success);
    // Get the number of pages in the test file
    unsigned count = fileHandle.getNumberOfPages();
    assert(count == (unsigned)0);
    // Close the file "test_1"
    rc = pfm->closeFile(fileHandle);
    assert(rc == success);
    cout << "Test Case 3 Passed!" << endl << endl;
    return 0;
}
int RBFTest_4(PagedFileManager *pfm)
{
    // Functions Tested:
    // 1. Open File
    // 2. Append Page
    // 3. Get Number Of Pages
    // 3. Close File
    cout << "****In RBF Test Case 4****" << endl;
    RC rc;
    string fileName = "test_1";
    // Open the file "test_1"
    FileHandle fileHandle;
    rc = pfm->openFile(fileName.c_str(), fileHandle);
    assert(rc == success);
    // Append the first page
    void *data = malloc(PAGE_SIZE);
    for(unsigned i = 0; i < PAGE_SIZE; i++)
    {
        *((char *)data+i) = i % 94 + 32;
    }
    rc = fileHandle.appendPage(data);
    assert(rc == success);

    // Get the number of pages
    unsigned count = fileHandle.getNumberOfPages();
    assert(count == (unsigned)1);
    // Close the file "test_1"
    rc = pfm->closeFile(fileHandle);
    assert(rc == success);
    free(data);
    cout << "Test Case 4 Passed!" << endl << endl;
    return 0;
}
int RBFTest_5(PagedFileManager *pfm)
{
    // Functions Tested:
    // 1. Open File
    // 2. Read Page
    // 3. Close File
    cout << "****In RBF Test Case 5****" << endl;
    RC rc;
    string fileName = "test_1";
    // Open the file "test_1"
    FileHandle fileHandle;
    rc = pfm->openFile(fileName.c_str(), fileHandle);
    assert(rc == success);
    // Read the first page
    void *buffer = malloc(PAGE_SIZE);
    rc = fileHandle.readPage(0, buffer);
    assert(rc == success);

    // Check the integrity of the page
    void *data = malloc(PAGE_SIZE);
    for(unsigned i = 0; i < PAGE_SIZE; i++)
    {
        *((char *)data+i) = i % 94 + 32;
    }
    rc = memcmp(data, buffer, PAGE_SIZE);
    assert(rc == success);

    // Close the file "test_1"
    rc = pfm->closeFile(fileHandle);
    assert(rc == success);
    free(data);
    free(buffer);
    cout << "Test Case 5 Passed!" << endl << endl;
    return 0;
}
int RBFTest_6(PagedFileManager *pfm)
{
    // Functions Tested:
    // 1. Open File
    // 2. Write Page
    // 3. Read Page
    // 4. Close File
    // 5. Destroy File
    cout << "****In RBF Test Case 6****" << endl;
    RC rc;
    string fileName = "test_1";
    // Open the file "test_1"
    FileHandle fileHandle;
    rc = pfm->openFile(fileName.c_str(), fileHandle);
    assert(rc == success);
    // Update the first page
    void *data = malloc(PAGE_SIZE);
    for(unsigned i = 0; i < PAGE_SIZE; i++)
    {
        *((char *)data+i) = i % 10 + 32;
    }
    rc = fileHandle.writePage(0, data);
    assert(rc == success);
    // Read the page
    void *buffer = malloc(PAGE_SIZE);
    rc = fileHandle.readPage(0, buffer);
    assert(rc == success);
    // Check the integrity
    rc = memcmp(data, buffer, PAGE_SIZE);
    assert(rc == success);

    // Close the file "test_1"
    rc = pfm->closeFile(fileHandle);
    assert(rc == success);
    free(data);
    free(buffer);
    // Destroy File
    rc = pfm->destroyFile(fileName.c_str());
    assert(rc == success);

    if(!FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been destroyed." << endl;
        cout << "Test Case 6 Passed!" << endl << endl;
        return 0;
    }
    else
    {
        cout << "Failed to destroy file!" << endl;
        cout << "Test Case 6 Failed!" << endl << endl;
        return -1;
    }
}
int RBFTest_7(PagedFileManager *pfm)
{
    // Functions Tested:
    // 1. Create File
    // 2. Open File
    // 3. Append Page
    // 4. Get Number Of Pages
    // 5. Read Page
    // 6. Write Page
    // 7. Close File
    // 8. Destroy File
    cout << "****In RBF Test Case 7****" << endl;
    RC rc;
    string fileName = "test_2";
    // Create the file named "test_2"
    rc = pfm->createFile(fileName.c_str());
    assert(rc == success);
    if(FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been created." << endl;
    }
    else
    {
        cout << "Failed to create file!" << endl;
        cout << "Test Case 7 Failed!" << endl << endl;
        return -1;
    }
    // Open the file "test_2"
    FileHandle fileHandle;
    rc = pfm->openFile(fileName.c_str(), fileHandle);
    assert(rc == success);
    // Append 50 pages
    void *data = malloc(PAGE_SIZE);
    for(unsigned j = 0; j < 50; j++)
    {
        for(unsigned i = 0; i < PAGE_SIZE; i++)
        {
            *((char *)data+i) = i % (j+1) + 32;
        }
        rc = fileHandle.appendPage(data);
        assert(rc == success);
    }
    cout << "50 Pages have been successfully appended!" << endl;

    // Get the number of pages
    unsigned count = fileHandle.getNumberOfPages();
    assert(count == (unsigned)50);
    // Read the 25th page and check integrity
    void *buffer = malloc(PAGE_SIZE);
    rc = fileHandle.readPage(24, buffer);
    assert(rc == success);
    for(unsigned i = 0; i < PAGE_SIZE; i++)
    {
        *((char *)data + i) = i % 25 + 32;
    }
    rc = memcmp(buffer, data, PAGE_SIZE);
    assert(rc == success);
    cout << "The data in 25th page is correct!" << endl;
    // Update the 25th page
    for(unsigned i = 0; i < PAGE_SIZE; i++)
    {
        *((char *)data+i) = i % 60 + 32;
    }
    rc = fileHandle.writePage(24, data);
    assert(rc == success);
    // Read the 25th page and check integrity
    rc = fileHandle.readPage(24, buffer);
    assert(rc == success);

    rc = memcmp(buffer, data, PAGE_SIZE);
    assert(rc == success);
    // Close the file "test_2"
    rc = pfm->closeFile(fileHandle);
    assert(rc == success);
    // Destroy File
    rc = pfm->destroyFile(fileName.c_str());
    assert(rc == success);
    free(data);
    free(buffer);
    if(!FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been destroyed." << endl;
        cout << "Test Case 7 Passed!" << endl << endl;
        return 0;
    }
    else
    {
        cout << "Failed to destroy file!" << endl;
        cout << "Test Case 7 Failed!" << endl << endl;
        return -1;
    }
}
int RFBTest_boundary_cases(PagedFileManager *pfm)
{
	//setup
	remove("new_file");
	//actual tests
	cout << "****In RBF Test Case 7****" << endl;
	RC rc;
	// 1. boundary test case - Create the file named ""
	rc = pfm->createFile("");
	assert(rc != success);
	// 2. boundary test case - destroy non-existing file
	rc = pfm->destroyFile("non_existing_file");
	assert(rc != success);
	// 3. destroy file that is already opened
	rc = pfm->createFile("new_file");
	assert(rc == success);
	FileHandle fileHandle;
	rc = pfm->openFile("new_file", fileHandle);
	assert(rc == success);
	rc = pfm->destroyFile("new_file");
	assert(rc != success);
	// 4. make sure that after un-successful destruction, file is still writeable and readable
	void *data = malloc(PAGE_SIZE);
	for(unsigned i = 0; i < PAGE_SIZE; i++)
	{
		*((char *)data+i) = i % 10 + 32;
	}
	//write the page
	rc = fileHandle.appendPage(data);
	assert(rc == success);
	// Read the page
	void *buffer = malloc(PAGE_SIZE);
	rc = fileHandle.readPage(0, buffer);
	assert(rc == success);
	// Check the integrity
	rc = memcmp(data, buffer, PAGE_SIZE);
	assert(rc == success);
	// 5. open a file with used handle
	rc = pfm->openFile("new_file", fileHandle);
	assert(rc != success);
	// 6. read beyond the file
	rc = fileHandle.readPage(1, buffer);
	assert(rc != success);
	// 7. write beyond the file
	rc = fileHandle.writePage(1, data);
	assert(rc != success);
	// 8. read into NULL-ed data buffer
	free(data);
	data = NULL;
	rc = fileHandle.readPage(0, data);
	assert(rc != success);
	// 9. write a NULL-ed data buffer
	rc = fileHandle.writePage(0, data);
	assert(rc != success);
	// 10. attempt to close file twice
	rc = pfm->closeFile(fileHandle);
	assert(rc == success);
	rc = pfm->closeFile(fileHandle);
	assert(rc != success);
	// 11. append to not opened file
	rc = fileHandle.appendPage(data);
	assert(rc != success);
	// 12. read from a not opened file
	rc = fileHandle.readPage(0, data);
	assert(rc != success);
	// 13. get number of pages using a handle of the closed file
	int numPages = fileHandle.getNumberOfPages();
	assert(numPages < 0);
	// 14. create an empty handler and try to close a file with it
	FileHandle emptyHandle;
	rc = pfm->closeFile(emptyHandle);
	assert(rc != success);
	return 0;
}

int RBFTest_8(RecordBasedFileManager *rbfm) {
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Record
    // 4. Read Record
    // 5. Close Record-Based File
    // 6. Destroy Record-Based File
    cout << "****In RBF Test Case 8****" << endl;

    RC rc;
    string fileName = "test_3";
    // Create a file named "test_3"
    rc = rbfm->createFile(fileName.c_str());
    assert(rc == success);
    if(FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been created." << endl;
    }
    else
    {
        cout << "Failed to create file!" << endl;
        cout << "Test Case 8 Failed!" << endl << endl;
        return -1;
    }
    // Open the file "test_3"
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName.c_str(), fileHandle);
    assert(rc == success);


    RID rid;
    int recordSize = 0;
    void *record = malloc(100);
    void *returnedData = malloc(100);
    vector<Attribute> recordDescriptor;
    createRecordDescriptor(recordDescriptor);

    // Insert a record into a file
    prepareRecord(6, "Peters", 24, 170.1, 5000, record, &recordSize);
    cout << "Insert Data:" << endl;
    rbfm->printRecord(recordDescriptor, record);

    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
    assert(rc == success);

    // Given the rid, read the record from file
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData);
    assert(rc == success);
    cout << "Returned Data:" << endl;
    rbfm->printRecord(recordDescriptor, returnedData);
    // Compare whether the two memory blocks are the same
    if(memcmp(record, returnedData, recordSize) != 0)
    {
       cout << "Test Case 8 Failed!" << endl << endl;
        free(record);
        free(returnedData);
        return -1;
    }

    // Close the file "test_3"
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success);
    // Destroy File
    rc = rbfm->destroyFile(fileName.c_str());
    assert(rc == success);

    free(record);
    free(returnedData);
    cout << "Test Case 8 Passed!" << endl << endl;

    return 0;
}

int RBFTest_9(RecordBasedFileManager *rbfm, vector<RID> &rids, vector<int> &sizes) {
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Multiple Records
    // 4. Close Record-Based File
    cout << "****In RBF Test Case 9****" << endl;

    RC rc;
    string fileName = "test_4";
    // Create a file named "test_4"
    rc = rbfm->createFile(fileName.c_str());
    assert(rc == success);
    if(FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been created." << endl;
    }
    else
    {
        cout << "Failed to create file!" << endl;
        cout << "Test Case 9 Failed!" << endl << endl;
        return -1;
    }
    // Open the file "test_4"
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName.c_str(), fileHandle);
    assert(rc == success);
    RID rid;
    void *record = malloc(1000);
    int numRecords = 2000;
    void *returnedData = malloc(1000);	//
    vector<Attribute> recordDescriptor;
    createLargeRecordDescriptor(recordDescriptor);
    for(unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        cout << "Attribute Name: " << recordDescriptor[i].name << endl;
        cout << "Attribute Type: " << (AttrType)recordDescriptor[i].type << endl;
        cout << "Attribute Length: " << recordDescriptor[i].length << endl << endl;
    }
    // Insert 2000 records into file
    for(int i = 0; i < numRecords; i++)
    {
        // Test insert Record
        int size = 0;
        memset(record, 0, 1000);
        prepareLargeRecord(i, record, &size);
        rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
        assert(rc == success);
        rids.push_back(rid);
        sizes.push_back(size);
        memset(returnedData, 0, 1000);	//
        rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData);	//
        cout << "first" << endl;	//
        rbfm->printRecord(recordDescriptor, record);	//
        cout << "second" << endl;	//
        rbfm->printRecord(recordDescriptor, returnedData);	//
        if(memcmp(returnedData, record, sizes[i]) != 0)	//
        {	//
        	cout << "not equal";	//
        }	//
    }
    // Close the file "test_4"
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success);
    free(record);
    cout << "Test Case 9 Passed!" << endl << endl;
    return 0;
}
int RBFTest_10(RecordBasedFileManager *rbfm, vector<RID> &rids, vector<int> &sizes) {
    // Functions tested
    // 1. Open Record-Based File
    // 2. Read Multiple Records
    // 3. Close Record-Based File
    // 4. Destroy Record-Based File
    cout << "****In RBF Test Case 10****" << endl;

    RC rc;
    string fileName = "test_4";
    // Open the file "test_4"
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    int numRecords = 2000;
    void *record = malloc(1000);
    void *returnedData = malloc(1000);
    vector<Attribute> recordDescriptor;
    createLargeRecordDescriptor(recordDescriptor);

    for(int i = 0; i < numRecords; i++)
    {
        memset(record, 0, 1000);
        memset(returnedData, 0, 1000);
        rc = rbfm->readRecord(fileHandle, recordDescriptor, rids[i], returnedData);
        assert(rc == success);

        cout << "Returned Data:" << endl;
        rbfm->printRecord(recordDescriptor, returnedData);
        int size = 0;
        prepareLargeRecord(i, record, &size);
        rbfm->printRecord(recordDescriptor, record);
        if(memcmp(returnedData, record, sizes[i]) != 0)
        {
            cout << "Test Case 10 Failed!" << endl << endl;
            free(record);
            free(returnedData);
            return -1;
        }
    }

    // Close the file "test_4"
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success);

    rc = rbfm->destroyFile(fileName.c_str());
    assert(rc == success);
    if(!FileExists(fileName.c_str())) {
        cout << "File " << fileName << " has been destroyed." << endl << endl;
        free(record);
        free(returnedData);
        cout << "Test Case 10 Passed!" << endl << endl;
        return 0;
    }
    else {
        cout << "Failed to destroy file!" << endl;
        cout << "Test Case 10 Failed!" << endl << endl;
        free(record);
        free(returnedData);
        return -1;
    }
}

int RBFTest_My_1(RecordBasedFileManager *rbfm, vector<RID> &rids, vector<int> &sizes) {
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Multiple Records
    // 4. Close Record-Based File
    cout << "****In RBF Test Case MY-1****" << endl;

    RC rc;
    string fileName = "test_my_1";
    // Create a file named "test_my_1"
    rc = rbfm->createFile(fileName.c_str());
    assert(rc == success);
    if(FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been created." << endl;
    }
    else
    {
        cout << "Failed to create file!" << endl;
        cout << "Test Case MY-1 Failed!" << endl << endl;
        return -1;
    }
    // Open the file "test_my_1"
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName.c_str(), fileHandle);
    assert(rc == success);
    RID rid;
    void *record = malloc(4008);
    int numRecords = 800;
    void *returnedData = malloc(4008);	//
    vector<Attribute> recordDescriptor;
    createMyRecordDescriptor(recordDescriptor);
    for(unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        cout << "Attribute Name: " << recordDescriptor[i].name << endl;
        cout << "Attribute Type: " << (AttrType)recordDescriptor[i].type << endl;
        cout << "Attribute Length: " << recordDescriptor[i].length << endl << endl;
    }
    // Insert 100 records into file
    for(int i = 0; i < numRecords; i++)
    {
        // Test insert Record
        int size = 0;
        memset(record, 0, 4008);
        prepareMyRecord(i, 4000, (char)('a' + i % 24), record, &size);
        rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
        assert(rc == success);
        rids.push_back(rid);
        sizes.push_back(size);
        memset(returnedData, 0, 4008);	//FBTest_boundary_cases()
        rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData);	//
        cout << "first" << endl;	//
        rbfm->printRecord(recordDescriptor, record);	//
        cout << "second" << endl;	//
        rbfm->printRecord(recordDescriptor, returnedData);	//
        if(memcmp(returnedData, record, sizes[i]) != 0)	//
        {	//
        	cout << "not equal";	//
        }	//
    }
    // Close the file "test_4"FBTest_boundary_cases()
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success);
    free(record);
    cout << "Test Case 9 Passed!" << endl << endl;
    return 0;
}

int RBFTest_My_2(RecordBasedFileManager *rbfm, vector<RID> &rids, vector<int> &sizes) {
    // Functions tested
    // 1. Open Record-Based File
    // 2. Read Multiple Records
    // 3. Close Record-Based File
    // 4. Destroy Record-Based File
    cout << "****In RBF Test Case MY-2****" << endl;

    RC rc;
    string fileName = "test_my_1";
    // Open the file "test_my_1"
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName.c_str(), fileHandle);
    assert(rc == success);

    int numRecords = 800;
    void *record = malloc(4008);
    void *returnedData = malloc(4008);
    vector<Attribute> recordDescriptor;
    createMyRecordDescriptor(recordDescriptor);

    for(int i = 0; i < numRecords; i++)
    {
        memset(record, 0, 4008);
        memset(returnedData, 0, 4008);
        rc = rbfm->readRecord(fileHandle, recordDescriptor, rids[i], returnedData);
        assert(rc == success);

        cout << "Returned Data:" << endl;
        rbfm->printRecord(recordDescriptor, returnedData);
        int size = 0;
        prepareMyRecord(i, 4000, (char)('a' + i % 24), record, &size);
        rbfm->printRecord(recordDescriptor, record);
        if(memcmp(returnedData, record, sizes[i]) != 0)
        {
            cout << "Test Case MY-2 Failed!" << endl << endl;
            free(record);
            free(returnedData);
            return -1;
        }
    }

    // Close the file "test_my_1"
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success);

    rc = rbfm->destroyFile(fileName.c_str());
    assert(rc == success);
    if(!FileExists(fileName.c_str())) {
        cout << "File " << fileName << " has been destroyed." << endl << endl;
        free(record);
        free(returnedData);
        cout << "Test Case MY-2 Passed!" << endl << endl;
        return 0;
    }
    else {
        cout << "Failed to destroy file!" << endl;
        cout << "Test Case MY-2 Failed!" << endl << endl;
        free(record);
        free(returnedData);
        return -1;
    }
}
int RBFTest_My_3(RecordBasedFileManager *rbfm, vector<RID> &rids, vector<int> &sizes) {
    // test plan:
	/*
	 * 1. insert a page of records with no space left inside this page
	 * 		=> record format: <int, real, varchar> of size 511 bytes: int:4, real:4, varchar:503
	 * 		=> page size is 4096 - 4*2 (for meta-data) = 4088 / 511 = 8 records in total
	 * 		=> 511 is not the actual size of record but is cumulative of record size and dir slot size
	 * 			+ record size is 511 - 8 = 503
	 * 		=> no free space left
	 * 2. update records
	 * 		=> record # 0, 3, 6 should get updated with varchar increased to size of 600 bytes, and the updated total size of record then would be 600+8 = 608 bytes
	 * 		=> could not fit within the former spots, so they will get reallocated in the second page
	 * 		=> keep in mind, when record is reallocated and TombStone is placed instead of its content, the actual size changes to unsigned version of -1
	 * 			so technically speaking, the freed up space (between end of TombStone and beginning of new record) still cannot be used
	 * 3. read records # 0, 1
	 * 		=> 0 is TombStone
	 * 		=> 1 is regular
	 * 4. read varchar attribute of record 3 and compare with the original
	 * 5. perform page reorganization
	 * 		=> 3 records would be reorganized, and cumulative freed up space should be 3 * 503 (former size of record is 511 bytes) - 3 * 8 (TombStone is 8 bytes) = 1485 bytes of free space!
	 * 6. read all records from 1st page
	 * 7. delete record # 5
	 * 8. insert a new large record of size 1485 bytes just to check whether insertion works properly
	 * 9. read again all records from 1st page
	 * 10. delete all records
	 */
    cout << "****In RBF Test Case My-3****" << endl;
    //1. insert a page of records with no space left inside this page
    RC rc;
    string fileName = "test_5";
    // Create a file named "test_5"
    rc = rbfm->createFile(fileName.c_str());
    assert(rc == success);
    if(FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been created." << endl;
    }
    else
    {
        cout << "Failed to create file!" << endl;
        cout << "Test Case My-3 Failed!" << endl << endl;
        return -1;
    }
    // Open the file "test_5"
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName.c_str(), fileHandle);
    assert(rc == success);
    RID rid;
    int size_of_record = 503;
    int numRecords = 9;	//changed to 9, so that 2 data pages are utilized (1st page with 8 records, 2nd page with 1 record)
    void *returnedData = malloc(size_of_record);
    vector<Attribute> recordDescriptor;
    createMyRecordDescriptor2(recordDescriptor, size_of_record-12);	//503-12 = 491 -> size of char-array in the record
    for(unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        cout << "Attribute Name: " << recordDescriptor[i].name << endl;
        cout << "Attribute Type: " << (AttrType)recordDescriptor[i].type << endl;
        cout << "Attribute Length: " << recordDescriptor[i].length << endl << endl;
    }
    map<int, pair<RID, char*> > buf_of_records;
    int size = 0;
    void *record = NULL;
    // Insert 9 records into file
    for(int i = 0; i < numRecords; i++)
    {
        size = 0;
        record = malloc(size_of_record);
        memset(record, 0, size_of_record);
        prepareMyRecord2(i, i*1.0f, size_of_record-12, (char)('a' + i % 24), record, &size);
        rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
        assert(rc == success);
        pair<RID, char*> inner = pair<RID, char*>(rid, (char*) record);
        pair<int, pair<RID, char*> > outter = pair<int, pair<RID, char*> >(i, inner);
        buf_of_records.insert(outter);
        rids.push_back(rid);
        sizes.push_back(size);
        memset(returnedData, 0, size_of_record);
        rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData);	//
        cout << "first" << endl;	//
        rbfm->printRecord(recordDescriptor, record);	//
        cout << "second" << endl;	//
        rbfm->printRecord(recordDescriptor, returnedData);	//
        if(memcmp(returnedData, record, sizes[i]) != 0)	//
        {	//
        	cout << "not equal => My-3 failed";	//
        	return -1;
        }	//
        				memset(returnedData, 0, size_of_record);	//
                        RID secondRid;
                        secondRid.pageNum = 1;
                        secondRid.slotNum = 0;
                        rc = rbfm->readRecord(fileHandle, recordDescriptor, secondRid, returnedData);	//
                        rbfm->printRecord(recordDescriptor, returnedData);	//
    }
    //2. update records (not fun to write long test cases...)
    //prepare arguments
    size_of_record = 600;
    int rec_to_be_updated[] = {0,3,6};
    int index_of_record = 0;
    //create new record description
    recordDescriptor.clear();
    createMyRecordDescriptor2(recordDescriptor, size_of_record-12);	//600-12 = 592 -> size of char-array in the record
    //prepare new (updated) records 0, 3, and 6
    rid.pageNum = 1;
    while(index_of_record < 3)
    {
    	//allocate new buffer
    	record = malloc(size_of_record);
    	//null record
    	memset(record, 0, size_of_record);
    	//record id
    	int id = rec_to_be_updated[index_of_record];
    	rid.slotNum = id;
    	//prepare record
    	prepareMyRecord2(id, id * 1.0f, size_of_record-12, (char)('z' - id % 24), record, &size);
    	//replace content of the old record with new and free the old buffers
		char* buf_to_free = buf_of_records.at(id).second;
		free(buf_to_free);
		buf_of_records[id].second = (char*)record;
    	//update record
    	rc = rbfm->updateRecord(fileHandle, recordDescriptor, record, rid);
    	assert(rc == success);
    	index_of_record++;
    }
    //3. read records 0 (re-allocated) and 1 (not changed)
    memset(record, 0, size_of_record);
    //read record # 0
    rid.slotNum = 0;
    rid.pageNum = 1;
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, record);
    assert(rc == success);
    cout << "read from DB zeroth:" << endl;	//
	rbfm->printRecord(recordDescriptor, record);	//
	cout << "read from map" << endl;	//
	void* zeroth_data = (void*)buf_of_records[0].second;
	rbfm->printRecord(recordDescriptor, zeroth_data);
	if(memcmp(zeroth_data, record, size_of_record) != 0)	//
	{	//
		cout << "not equal => My-3 failed";	//
		return -1;
	}
	//read record # 1
	size_of_record = 503;
	memset(returnedData, 0, size_of_record);
	rid.slotNum = 1;
	rid.pageNum = 1;
	vector<Attribute> recordDescriptor511;
	createMyRecordDescriptor2(recordDescriptor511, size_of_record-12);	//503-12 = 491 -> size of char-array in the record
	rc = rbfm->readRecord(fileHandle, recordDescriptor511, rid, returnedData);
	assert(rc == success);
	cout << "read from DB first:" << endl;	//
	rbfm->printRecord(recordDescriptor511, returnedData);	//
	cout << "read from map" << endl;	//
	void* first_data = (void*)buf_of_records[1].second;
	rbfm->printRecord(recordDescriptor511, first_data);
	if(memcmp(first_data, returnedData, size_of_record) != 0)	//
	{	//
		cout << "not equal => My-3 failed";	//
		return -1;
	}
	//4. read varchar attribute of record 3 and compare with the original
	/*
	 * 0=a
	 * 1=b
	 * 2=c
	 * 3=d
	 * 4=e
	 * 5=f
	 * 6=g
	 * 7=h
	 */
	size_of_record = 600-12;
	free(record);
	record = malloc(size_of_record + 1);
	memset(record, 0, size_of_record+1);
	rid.pageNum = 1;
	rid.slotNum = 3;
	string attrName = "val";
	rc = rbfm->readAttribute(fileHandle, recordDescriptor, rid, attrName, record);
	assert(rc == success);
	if( ((char*)record)[size_of_record] != '\0' || ((char*)record)[0] != 'w' )
	{
		cout << "read attribute is not working" << endl;
		return -1;
	}
	//5. perform page reorganization
	rc = rbfm->reorganizePage(fileHandle, recordDescriptor511, 1);
	assert(rc == success);
	//6. read all records from 1st page
	int j = 0, max = 8;
	free(record);
	record = malloc(PAGE_SIZE);
	for(; j < max; j++)
	{
		memset(record, 0, PAGE_SIZE);
		//read record # 0
		rid.slotNum = j;
		rid.pageNum = 1;
		rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, record);
		assert(rc == success);
		cout << "read item # " << j << endl;	//
		rbfm->printRecord(recordDescriptor, record);	//
	}
	//7. delete record 5
	rid.pageNum = 1;
	rid.slotNum = 5;
	rc = rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
	//8. insert a new large record of size 1485 bytes just to check whether insertion works properly
	vector<Attribute> recordDescriptor1485;
	size_of_record = 1485;
	createMyRecordDescriptor2(recordDescriptor1485, size_of_record-12);	//1485-12 = 1473 -> size of char-array in the record
	free(record);
	record = malloc(size_of_record);
	memset(record, 0, size_of_record);
	size = 0;
	prepareMyRecord2(9, 9*1.0f, size_of_record-12, (char)('K'), record, &size);
	rc = rbfm->insertRecord(fileHandle, recordDescriptor1485, record, rid);
	assert(rc == success);
	free(returnedData);
	returnedData = malloc(size_of_record);
	memset(returnedData, 0, size_of_record);	//
	rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData);	//
	cout << "local record for 1485:" << endl;	//
	rbfm->printRecord(recordDescriptor, record);	//
	cout << "db record for 1485:" << endl;	//
	rbfm->printRecord(recordDescriptor, returnedData);	//
	if(memcmp(returnedData, record, size_of_record) != 0)	//
	{	//
		cout << "not equal => My-3 failed";	//
		return -1;
	}
	//9. read again all records
	j = 0;
	max = 8;
	free(record);
	record = malloc(PAGE_SIZE);
	for(; j < max; j++)
	{
		memset(record, 0, PAGE_SIZE);
		rid.slotNum = j;
		rid.pageNum = 1;
		rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, record);
		assert(rc == success);
		cout << "read item # " << j << endl;	//
		rbfm->printRecord(recordDescriptor, record);	//
	}
	//10. delete all records
	rc = rbfm->deleteRecords(fileHandle);
	assert(rc == success);
    // Close the file "test_5"
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success);
    //free up the buffers used
    map<int, pair<RID, char*> >::iterator it = buf_of_records.begin();
    for(; it != buf_of_records.end(); it++)
    {
    	free(it->second.second);
    }
    free(record);
    free(returnedData);
    cout << "Test Case My-3 Passed!" << endl << endl;
    return 0;
}
//may be extra test for later: create very small records (just 4 bytes long) and try to update their size, so that they need to reallocate (insert a TombStone)

int RBFTest_My_4(RecordBasedFileManager *rbfm) {
	cout << "****In RBF Test Case My-4****" << endl;
	RC rc;
	string fileName = "test_6";
	// Create a file named "test_6"
	rc = rbfm->createFile(fileName.c_str());
	assert(rc == success);
	if(FileExists(fileName.c_str()))
	{
		cout << "File " << fileName << " has been created." << endl;
	}
	else
	{
		cout << "Failed to create file!" << endl;
		cout << "Test Case My-4 Failed!" << endl << endl;
		return -1;
	}
	// Open the file "test_6"
	FileHandle fileHandle;
	rc = rbfm->openFile(fileName.c_str(), fileHandle);
	assert(rc == success);
	RID rid;
	vector<Attribute> recordDescriptor;
	createRecordDescriptor(recordDescriptor);
	vector<string> attrNames;
	unsigned int i;
	for(i = 0; i < recordDescriptor.size(); i++)
	{
		cout << "Attribute Name: " << recordDescriptor[i].name << endl;
		attrNames.push_back(string(recordDescriptor[i].name));
		cout << "Attribute Type: " << (AttrType)recordDescriptor[i].type << endl;
		cout << "Attribute Length: " << recordDescriptor[i].length << endl << endl;
	}
	map<int, pair<RID, char*> > buf_of_records;
	int size = 0;
	void *record = malloc(100);
	int numRecords = 20;
	char* names[] = { "AaaaaA", "BbbbbbbbbB", "CcccccC", "DdddddD", "EeeeE" };
	// Insert records
	for(i = 0; i < numRecords; i++)
	{
		size = 0;
		char* str = names[i % 5];
		// Insert a record into a file
		prepareRecord(strlen(str), str, i % 10, i * 1.0f, 5000, record, &size);
		rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
		assert(rc == success);
	}
	//scan the file
	RBFM_ScanIterator it;
	RID itRid = {0, 0};
	void* data = malloc(PAGE_SIZE);
	memset(data, 0, PAGE_SIZE);
	char compArg1[] = "CcccccC";
	assert(rbfm->scan(fileHandle, recordDescriptor, "EmpName", EQ_OP, compArg1, attrNames, it) == success);
	while(it.getNextRecord(itRid, data) != RBFM_EOF)
	{
		rbfm->readRecord(fileHandle, recordDescriptor, itRid, data);
		cout << "found record: " << endl;
		rbfm->printRecord(recordDescriptor, data);
	}
	memset(data, 0, PAGE_SIZE);
	int compArg2 = 4;
	assert(rbfm->scan(fileHandle, recordDescriptor, "Age", GE_OP, &compArg2, attrNames, it) == success);
	while(it.getNextRecord(itRid, data) != RBFM_EOF)
	{
		rbfm->readRecord(fileHandle, recordDescriptor, itRid, data);
		cout << "found record: " << endl;
		rbfm->printRecord(recordDescriptor, data);
	}
	// Close the file "test_6"
	rc = rbfm->closeFile(fileHandle);
	assert(rc == success);
	free(record);
	cout << "Test Case My-4 Passed!" << endl << endl;
	return 0;
}

int main()
{
    // To test the functionality of the paged file manager
    //PagedFileManager *pfm = PagedFileManager::instance();


    // To test the functionality of the record-based file manager
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();


    // Remove files that might be created by previous test run
    remove("test");
    remove("test_1");
    remove("test_2");
    remove("test_3");
    remove("test_4");
    remove("test_my_1");
    remove("test_5");
    remove("test_6");

    /*RBFTest_1(pfm);
    RBFTest_2(pfm);
    RBFTest_3(pfm);
    RBFTest_4(pfm);
    RBFTest_5(pfm);
    RBFTest_6(pfm);
    RBFTest_7(pfm);
    RFBTest_boundary_cases(pfm);
    RBFTest_8(rbfm);
	*/
    vector<RID> rids;
    vector<int> sizes;
    //RBFTest_9(rbfm, rids, sizes);
    //RBFTest_10(rbfm, rids, sizes);

    /*rids.clear();
    sizes.clear();
    RBFTest_My_1(rbfm, rids, sizes);
    RBFTest_My_2(rbfm, rids, sizes);*/

    //RBFTest_My_3(rbfm, rids, sizes);
    RBFTest_My_4(rbfm);

    return 0;
}
