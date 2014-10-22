/*#include <iostream>
#include <fstream>
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
unsigned total = 0;

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

void createLargeRecordDescriptor2(vector<Attribute> &recordDescriptor)
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
        attr.length = (AttrLength)2000;
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

bool compareFileSizes(string fileName1, string fileName2) {
    streampos s1, s2;
    ifstream in1(fileName1.c_str(), ifstream::in |ifstream::binary);
    in1.seekg(0, ifstream::end);
    s1 = in1.tellg();

    ifstream in2(fileName2.c_str(), ifstream::in |ifstream::binary);
    in2.seekg(0, ifstream::end);
    s2 = in2.tellg();

    cout << "File 1 size: " << s1 << "\n";
    cout << "File 2 size: " << s2 << "\n";

    if (s1 != s2) {
        return false;
    }
    return true;
}
ifstream::pos_type filesize(const char* filename)
{
    std::ifstream in(filename, std::ifstream::in | std::ifstream::binary);
    in.seekg(0, std::ifstream::end);
    cout << filename << " - file size:" << in.tellg() << "\n";
    return in.tellg();
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
    if(rc != success) {
        return -1;
    }
    assert(rc == success);

    if(FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been created." << endl;
    }
    else
    {
        cout << "Failed to create file!" << endl;
        cout << "Test Case 1 Failed!" << endl;
        return -1;
    }

    // Create "test" again, should fail
    rc = pfm->createFile(fileName.c_str());
    if(rc == success) {
        return -1;
    }
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
    if(rc != success) {
        return -1;
    }
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
    if(rc != success) {
        return -1;
    }
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
    if(rc != success) {
        return -1;
    }
    if(rc != success) {
        return -1;
    }
    assert(rc == success);

    // Get the number of pages in the test file
    unsigned count = fileHandle.getNumberOfPages();
    if(count != (unsigned)0) {
        return -1;
    }
    assert(count == (unsigned)0);

    // Close the file "test_1"
    rc = pfm->closeFile(fileHandle);
    if(rc != success) {
        return -1;
    }
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
    if(rc != success) {
        return -1;
    }
    assert(rc == success);

    // Append the first page
    void *data = malloc(PAGE_SIZE);
    for(unsigned i = 0; i < PAGE_SIZE; i++)
    {
        *((char *)data+i) = i % 94 + 32;
    }
    rc = fileHandle.appendPage(data);
    if(rc != success) {
        return -1;
    }
    assert(rc == success);

    // Get the number of pages
    unsigned count = fileHandle.getNumberOfPages();
    if(count != (unsigned)1) {
        return -1;
    }
    assert(count == (unsigned)1);

    // Close the file "test_1"
    rc = pfm->closeFile(fileHandle);
    if(rc != success) {
        return -1;
    }
    assert(rc == success);

    free(data);

	int fsize = filesize(fileName.c_str());
	if (fsize == 0) {
		cout << "File Size should not be zero at this moment." << endl;
		return -1;
	}
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
    if(rc != success) {
        return -1;
    }
    assert(rc == success);

    // Read the first page
    void *buffer = malloc(PAGE_SIZE);
    rc = fileHandle.readPage(0, buffer);
    if(rc != success) {
        return -1;
    }
    assert(rc == success);

    // Check the integrity of the page
    void *data = malloc(PAGE_SIZE);
    for(unsigned i = 0; i < PAGE_SIZE; i++)
    {
        *((char *)data+i) = i % 94 + 32;
    }
    rc = memcmp(data, buffer, PAGE_SIZE);
    if(rc != success) {
        return -1;
    }
    assert(rc == success);

    // Close the file "test_1"
    rc = pfm->closeFile(fileHandle);
    if(rc != success) {
        return -1;
    }
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
    if(rc != success) {
        return -1;
    }
    assert(rc == success);

    // Update the first page
    void *data = malloc(PAGE_SIZE);
    for(unsigned i = 0; i < PAGE_SIZE; i++)
    {
        *((char *)data+i) = i % 10 + 32;
    }
    rc = fileHandle.writePage(0, data);
    if(rc != success) {
        return -1;
    }
    assert(rc == success);

    // Read the page
    void *buffer = malloc(PAGE_SIZE);
    rc = fileHandle.readPage(0, buffer);
    if(rc != success) {
        return -1;
    }
    assert(rc == success);

    // Check the integrity
    rc = memcmp(data, buffer, PAGE_SIZE);
    if(rc != success) {
        return -1;
    }
    assert(rc == success);

    // Close the file "test_1"
    rc = pfm->closeFile(fileHandle);
    if(rc != success) {
        return -1;
    }
    assert(rc == success);

    free(data);
    free(buffer);

	int fsize = filesize(fileName.c_str());
	if (fsize == 0) {
		cout << "File Size should not be zero at this moment." << endl;
		return -1;
	}
	
    // Destroy File
    rc = pfm->destroyFile(fileName.c_str());
    if(rc != success) {
        return -1;
    }
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
    if(rc != success) {
        return -1;
    }
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
    if(rc != success) {
        return -1;
    }
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
        if(rc != success) {
            return -1;
        }
        assert(rc == success);
    }
    cout << "50 Pages have been successfully appended!" << endl;

    // Get the number of pages
    unsigned count = fileHandle.getNumberOfPages();
    if(count != (unsigned)50) {
        return -1;
    }
    assert(count == (unsigned)50);

    // Read the 25th page and check integrity
    void *buffer = malloc(PAGE_SIZE);
    rc = fileHandle.readPage(24, buffer);
    if(rc != success) {
        return -1;
    }
    assert(rc == success);

    for(unsigned i = 0; i < PAGE_SIZE; i++)
    {
        *((char *)data + i) = i % 25 + 32;
    }
    rc = memcmp(buffer, data, PAGE_SIZE);
    if(rc != success) {
        return -1;
    }
    assert(rc == success);
    cout << "The data in 25th page is correct!" << endl;

    // Update the 25th page
    for(unsigned i = 0; i < PAGE_SIZE; i++)
    {
        *((char *)data+i) = i % 60 + 32;
    }
    rc = fileHandle.writePage(24, data);
    if(rc != success) {
        return -1;
    }
    assert(rc == success);

    // Read the 25th page and check integrity
    rc = fileHandle.readPage(24, buffer);
    if(rc != success) {
        return -1;
    }
    assert(rc == success);

    rc = memcmp(buffer, data, PAGE_SIZE);
    if(rc != success) {
        return -1;
    }
    assert(rc == success);

    // Close the file "test_2"
    rc = pfm->closeFile(fileHandle);
    if(rc != success) {
        return -1;
    }
    assert(rc == success);

	int fsize = filesize(fileName.c_str());
	if (fsize == 0) {
		cout << "File Size should not be zero at this moment." << endl;
		return -1;
	}
	
    // Destroy File
    rc = pfm->destroyFile(fileName.c_str());
    if(rc != success) {
        return -1;
    }
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
    if(rc != success) {
        return -1;
    }
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
    if(rc != success) {
        return -1;
    }
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
    if(rc != success) {
        return -1;
    }
    assert(rc == success);

    // Given the rid, read the record from file
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData);
    if(rc != success) {
        return -1;
    }
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
    if(rc != success) {
        return -1;
    }
    assert(rc == success);

	int fsize = filesize(fileName.c_str());
	if (fsize == 0) {
		cout << "File Size should not be zero at this moment." << endl;
		return -1;
	}
	
    // Destroy File
    rc = rbfm->destroyFile(fileName.c_str());
    if(rc != success) {
        return -1;
    }
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
    if(rc != success) {
        return -1;
    }
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
    if(rc != success) {
        return -1;
    }
    assert(rc == success);


    RID rid;
    void *record = malloc(1000);
    int numRecords = 2000;

    vector<Attribute> recordDescriptor;
    createLargeRecordDescriptor(recordDescriptor);

    for(unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        cout << "Attribute Name: " << recordDescriptor.at(i).name << " Type: " << (AttrType)recordDescriptor.at(i).type << " Length: " << recordDescriptor.at(i).length << endl;
    }

    // Insert 2000 records into file
    for(int i = 0; i < numRecords; i++)
    {
        // Test insert Record
        int size = 0;
        memset(record, 0, 1000);
        prepareLargeRecord(i, record, &size);

        rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
        if(rc != success) {
            return -1;
        }
        assert(rc == success);

        rids.push_back(rid);
        sizes.push_back(size);
    }
    // Close the file "test_4"
    rc = rbfm->closeFile(fileHandle);
    if(rc != success) {
        return -1;
    }
    assert(rc == success);

	int fsize = filesize(fileName.c_str());
	if (fsize == 0) {
		cout << "File Size should not be zero at this moment." << endl;
		return -1;
	}
	
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
    if(rc != success) {
        return -1;
    }
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
        if(rc != success) {
            return -1;
        }
        assert(rc == success);

		if (i % 500 == 0) {
	        cout << "Returned Data: " << endl;
   		    rbfm->printRecord(recordDescriptor, returnedData);
		}
		

        int size = 0;
        prepareLargeRecord(i, record, &size);
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
    if(rc != success) {
        return -1;
    }
    assert(rc == success);

	int fsize = filesize(fileName.c_str());
	if (fsize == 0) {
		cout << "File Size should not be zero at this moment." << endl;
		return -1;
	}
	
    rc = rbfm->destroyFile(fileName.c_str());
    if(rc != success) {
        return -1;
    }
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

int main()
{
    PagedFileManager *pfm = PagedFileManager::instance(); // To test the functionality of the paged file manager
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance(); // To test the functionality of the record-based file manager

    remove("test");
    remove("test_1");
    remove("test_2");
    remove("test_3");
    remove("test_4");
    remove("test_5");

    string failTest = "";

    int rc = RBFTest_1(pfm);
    if (rc == 0) {
        total += 3;
    } else {
    	failTest += "1 ";
    }
    rc = RBFTest_2(pfm);
    if (rc == 0) {
        total += 3;
    } else {
    	failTest += "2 ";
    }
    rc = RBFTest_3(pfm);
    if (rc == 0) {
        total += 4;
    } else {
    	failTest += "3 ";
    }
    rc = RBFTest_4(pfm);
    if (rc == 0) {
        total += 4;
    } else {
    	failTest += "4 ";
    }
    rc = RBFTest_5(pfm);
    if (rc == 0) {
        total += 4;
    } else {
    	failTest += "5 ";
    }
    rc = RBFTest_6(pfm);
    if (rc == 0) {
        total += 4;
    } else {
    	failTest += "6 ";
    }
    rc = RBFTest_7(pfm);
    if (rc == 0) {
        total += 4;
    } else {
    	failTest += "7 ";
    }
    rc = RBFTest_8(rbfm);
    if (rc == 0) {
        total += 4;
    } else {
    	failTest += "8 ";
    }

    vector<RID> rids;
    vector<int> sizes;
    rc = RBFTest_9(rbfm, rids, sizes);
    if (rc == 0) {
        total += 5;
    }  else {
    	failTest += "9 ";
    }
    rc = RBFTest_10(rbfm, rids, sizes);
    if (rc == 0) {
        total += 5;
    }  else {
    	failTest += "10 ";
    }

    if (failTest.length() > 1) {
    	cout << "Failed test case(s): " + failTest << endl;
    } else {
    	cout << "No test case(s) failed." << endl;
    }
    cout << "Score for test #1 ~ #10 is: " << total << " / 40" << endl;
    return 0;
}
*/
