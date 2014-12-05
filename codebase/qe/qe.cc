#include "qe.h"
#include <cfloat>

//prototype(s)
RC getOffsetToProperField(const void* record, const vector<Attribute> recordDesc, const Attribute properField, int& offset);

/** Filter Class:
 * This filter class is initialized by an input iterator and a selection condition.
 * It filters the tuples from the input iterator by applying the filter predicate condition on them.
 */
Filter::Filter(Iterator* input, const Condition &condition) {
	/**
	 * obs: I guess in this case condition.bRhsIsAttr is always FALSE, otherwise it is a Join query
	 */

	// set up the filter's parameters
	iterator = input;
	compOp = condition.op;
	rightHand.data = condition.rhsValue.data;
	rightHand.type = condition.rhsValue.type;

	iterator->getAttributes(attrs);

	//find the correct position in the vector of attributes comparing with the right hand
	for (unsigned i = 0; i < attrs.size(); i++) {
		if (attrs[i].name.compare(condition.lhsAttr) == 0) {
			position = i;
			leftHandValue = malloc(attrs[i].length + sizeof(int));
			break;
		}
	}
}

Filter::~Filter() {
	free(leftHandValue);
}

RC Filter::getNextTuple(void *data) {
	RC errCode = 0;

	do { // for each tuple from the Iterator (TableScan or IndexScan)

		if ((errCode = iterator->getNextTuple(data)) != 0) { //potencial tuple that matches
			return errCode;
		}

		int offset = 0;
		for (unsigned i = 0; i < position; i++) { //offset to the correct position of the field in tuple
			switch (attrs[i].type) {

			case TypeInt:
				//sum field size of int
				offset += sizeof(int);
				break;

			case TypeReal:
				//sum field size of float
				offset += sizeof(float);
				break;

			case TypeVarChar:
				//get integer that represent size of char-array and add it to the offset
				int stringLength = *(int *) ((char *) data + offset);
				offset += sizeof(int) + stringLength;
				break;
			}

		}

		int attrLength = 0;
		//get the length of the field
		switch (rightHand.type) {
		case TypeInt:
			//sum length of int
			attrLength = sizeof(int);
			break;

		case TypeReal:
			//sum length of float
			attrLength = sizeof(float);
			break;

		case TypeVarChar:
			//sum length of variable char
			attrLength = *(int *) ((char *) data + offset) + sizeof(int);
			break;
		}

		//get the property field
		memcpy(leftHandValue, (char *) data + offset, attrLength);

	} while (compareTwoField(leftHandValue, rightHand.data, rightHand.type,
			compOp) == false); // while don't find a tuple that matches

	return errCode;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;
}

bool compareTwoField(const void *valueToCompare, const void *condition,
		AttrType type, CompOp compOp) {
	if (condition == NULL)
		return true;

	bool result = true;

	switch (type) {
	case TypeInt: {
		int attr = *(int *) valueToCompare;
		int cond = *(int *) condition;

		switch (compOp) {
		case EQ_OP:
			result = attr == cond;
			break;
		case LT_OP:
			result = attr < cond;
			break;
		case GT_OP:
			result = attr > cond;
			break;
		case LE_OP:
			result = attr <= cond;
			break;
		case GE_OP:
			result = attr >= cond;
			break;
		case NE_OP:
			result = attr != cond;
			break;
		case NO_OP:
			break;
		}

		break;
	}

	case TypeReal: {
		float attr = *(float *) valueToCompare;
		float cond = *(float *) condition;

		switch (compOp) {
		case EQ_OP:
			result = attr == cond;
			break;
		case LT_OP:
			result = attr < cond;
			break;
		case GT_OP:
			result = attr > cond;
			break;
		case LE_OP:
			result = attr <= cond;
			break;
		case GE_OP:
			result = attr >= cond;
			break;
		case NE_OP:
			result = attr != cond;
			break;
		case NO_OP:
			break;
		}

		break;
	}

	case TypeVarChar: {
		// get both string according their lengths
		int attriLeng = *(int *) valueToCompare;
		string attr((char *) valueToCompare + sizeof(int), attriLeng);
		int condiLeng = *(int *) condition;
		string cond((char *) condition + sizeof(int), condiLeng);

		switch (compOp) {
		case EQ_OP:
			result = strcmp(attr.c_str(), cond.c_str()) == 0;
			break;
		case LT_OP:
			result = strcmp(attr.c_str(), cond.c_str()) < 0;
			break;
		case GT_OP:
			result = strcmp(attr.c_str(), cond.c_str()) > 0;
			break;
		case LE_OP:
			result = strcmp(attr.c_str(), cond.c_str()) <= 0;
			break;
		case GE_OP:
			result = strcmp(attr.c_str(), cond.c_str()) >= 0;
			break;
		case NE_OP:
			result = strcmp(attr.c_str(), cond.c_str()) != 0;
			break;
		case NO_OP:
			break;
		}

		break;
	}
	}
	return result;
}

/** Project Class:
 * This project class takes an iterator and a vector of attribute names as input.
 * It projects out the values of the attributes in the attrNames.
 */
Project::Project(Iterator *input, const vector<string> &attrNames) {
	// set up the project parameters
	iterator = input;

	// get the complete vector of attributes
	iterator->getAttributes(originalAttrs);

	// prepare the corresponding attributes to project from attrNames
	unsigned i = 0, j = 0;
	//cout << "originalAttrs.size = " << originalAttrs.size() << "; attrNames.size = " << attrNames.size() << endl;
	for (; i < originalAttrs.size() && j < attrNames.size(); i++) {
		/*
		 * in SQL statement of clitest04 => SELECT PROJECT (PROJECT tbl_employee GET [ * ]) GET [ EmpName ]
		 * outer projection that gets just single attribute EmpName happens first. It then calls inner projection and passes attrNames with just 1 attribute in it.
		 */
		//cout << "attrNames[" << j << "] = " << attrNames[j] << endl;
		if (originalAttrs[i].name.compare(attrNames[j]) == 0) {
			attrs.push_back(originalAttrs[i]);
			j++;
		}
	}

}

Project::~Project() {
	//nothing
}

RC Project::getNextTuple(void *data) {
	/*
	 * All tuples are candidates to project, then get all of them and one at a time and return its fields
	 */
	int errCode = 0;
	void *tuple = malloc(PAGE_SIZE);	//this function eventually calls RM.getNextEntry and RM expects that passed data buffer has
										//pre-allocated space (for safety reasons it is usually page size => so that it would not overflow)
	if ((errCode = iterator->getNextTuple(tuple)) != 0) { //get next tuple
		return errCode;
	}

	unsigned i = 0, j = 0;
	int offsetI = 0, offsetJ = 0;
	//look on the tuple for the corresponding attributes and add them to data
	while (i < originalAttrs.size() && j < attrs.size()) {
		/*
		 * 		source:							answer:
		 * 		original=|A|B|C|D|E|   			attrs=|A|D|
		 * 		tuple= |x|x|x|xxxx|x|			data= |x|xxxx|
		 */
		// get the length of field to add
		int length = 0;
		//switch (attrs[i].type) {
		switch(originalAttrs[i].type) {
		/*
		 * attrs is a subset of originalAttrs
		 * In statement SELECT PROJECT (PROJECT tbl_employee GET [ EmpName, Age ]) GET [ Age ]
		 * 	inner PROJECT would return <name:VarChar, age:Int>
		 * 	outer will scan thru the returned expression but keep in mind that for outer's attrs include only one element, i.e. Age
		 * 	During the outer scan the first field in the record will be EmpName, since outer does not need => skip
		 */

		case TypeInt:
			//sum field size of int
			length = sizeof(int);
			break;

		case TypeReal:
			//sum field size of float
			length = sizeof(float);
			break;

		case TypeVarChar:
			//get integer that represent size of char-array and add it to the offset
			length = sizeof(int) + *(int *) ((char *) tuple + offsetI);
			break;
		}


		// if it is the same field, add it and increase the offset
		if (originalAttrs[i].name.compare(attrs[j].name) == 0) {
			memcpy((char *) data + offsetJ, (char *) tuple + offsetI, length);
			j++;
			offsetJ += length;
		}

		// go to next field
		i++;
		offsetI += length;
	}

	free(tuple);
	return errCode;
}

void Project::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;
}


/**
 * Grace Hash Join  Class:
 **/
GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn,	const Condition &condition, const unsigned numPartitions ) {

/**
 * Algorithm:
 *
 * - Choose a hash bucket such that R is partitioned into partitions, each one of approximately equal size.
 *  Allocate output buffers, one each for each partition of R.
 *
 * - Scan R. Hash each tuple and place it in the appropriate output buffer. When an output buffer fills,
 *  it is written to disk. After R has been completely scanned, flush all output buffers to disk.
 *
 * - Scan S. Use same hash function which was used for R and repeat the process in previous step.
 *
 * - Read Ri into memory and build a hash table for it.
 *
 * - Hash each tuple of Si with same hash function used previously and probe for a match. If match exists, output tuple in result, move on to the next tuple.
**/
}
;

GHJoin::~GHJoin() {

}
;

RC GHJoin::getNextTuple(void *data) {
	return QE_EOF;
}
;

void GHJoin::getAttributes(vector<Attribute> &attrs) const {

}
;

/**
 * Block Nested-Loop Join Class:
 **/

inMemoryHashTable::inMemoryHashTable(const AttrType typeOfKeyInRecord, vector<Attribute> description)
: _table(NULL), _type(typeOfKeyInRecord), _desc(description)
{
	//construct in-memory hash table
	//since project did not specify to build own hash table (like it sort of did for grace hash join)
	//we will use just a standard library's table
	switch(_type)
	{
	case TypeInt:
		_table = new map<int, vector<void*> >();
		break;
	case TypeReal:
		_table = new map<float, vector<void*> >();
		break;
	case TypeVarChar:
		_table = new map<string, vector<void*> >();
		break;
	}
}

bool inMemoryHashTable::getRecord(const void* key, void* outRecordData, unsigned int& lengthOfRecord)
{
	bool foundEntry = false;

	//to avoid compilation errors about "cross intialization" keeping all variable declarations
	//outside of switch case statements
	int ikey = 0; float fkey = 0.0f; string skey = "";
	map<int, vector<void*> >* iTable; map<float, vector<void*> >* fTable; map<string, vector<void*> >* sTable;
	map<int, vector<void*> >::iterator iEntryIt; map<float, vector<void*> >::iterator fEntryIt; map<string, vector<void*> >::iterator sEntryIt;
	int lengthOfKey = 0; char* charArr = NULL;

	switch(_type)
	{
	case TypeInt:
		//determine integer key
		ikey = ((int*) ((const char*)key) )[0];

		//cast _table to map pointer for easier access afterwards
		iTable = ((map< int, vector<void*> >*)_table);

		//check if such key already been inserted
		if( (iEntryIt = iTable->find(ikey)) != iTable->end() )
		{
			foundEntry = true;

			//copy found record
			lengthOfRecord = sizeOfRecord(_desc, iEntryIt->second[0]);
			memcpy(outRecordData, iEntryIt->second[0], lengthOfRecord);

			//remove it from the list
			iEntryIt->second.erase( iEntryIt->second.begin() );

			//check if list became empty
			if( iEntryIt->second.size() == 0 )
			{
				//then remove entry
				iTable->erase(iEntryIt);
			}
		}
		break;
	case TypeReal:
		//determine real key
		fkey = ((float*) ((const char*)key) )[0];

		//cast _table to map pointer for easier access afterwards
		fTable = ((map< float, vector<void*> >*)_table);

		//check if such key already been inserted
		if( (fEntryIt = fTable->find(fkey)) != fTable->end() )
		{
			foundEntry = true;

			//copy found record
			lengthOfRecord = sizeOfRecord(_desc, fEntryIt->second[0]);
			memcpy(outRecordData, fEntryIt->second[0], lengthOfRecord);

			//remove it from the list
			fEntryIt->second.erase( fEntryIt->second.begin() );

			//check if list became empty
			if( fEntryIt->second.size() == 0 )
			{
				//then remove entry
				fTable->erase(fEntryIt);
			}
		}
		break;
	case TypeVarChar:
		//determine length of the key
		lengthOfKey = ((int*) ((const char*)key) )[0];

		//create string object to represent VarChar key
		charArr = (char*)malloc(lengthOfKey + 1);
		charArr[lengthOfKey] = '\0';	//null terminated character array
		memcpy( charArr, (char*)key + sizeof(int), lengthOfKey );
		string skey = string(charArr);
		free(charArr);

		//cast _table to map pointer for easier access afterwards
		sTable = ((map< string, vector<void*> >*)_table);

		//check if such key already been inserted
		if( (sEntryIt = sTable->find(skey)) != sTable->end() )
		{
			foundEntry = true;

			//copy found record
			lengthOfRecord = sizeOfRecord(_desc, sEntryIt->second[0]);
			memcpy(outRecordData, sEntryIt->second[0], lengthOfRecord);

			//remove it from the list
			sEntryIt->second.erase( sEntryIt->second.begin() );

			//check if list became empty
			if( sEntryIt->second.size() == 0 )
			{
				//then remove entry
				sTable->erase(sEntryIt);
			}
		}
		break;
	}

	return foundEntry;
}

void inMemoryHashTable::clearTable()
{
	int i = 0, max = -1;
	map<int, vector<void*> >::iterator iIt, iMax;
	map<float, vector<void*> >::iterator fIt, fMax;
	map<string, vector<void*> >::iterator sIt, sMax;
	switch(_type)
	{
	case TypeInt:
		iIt = ((map<int, vector<void*> >*)_table)->begin();
		iMax = ((map<int, vector<void*> >*)_table)->end();
		for( ; iIt != iMax; iIt++ )
		{
			max = (int)iIt->second.size();
			for( i = 0; i < max; i++ )
			{
				free( iIt->second[i] );
			}
		}
		((map<int, vector<void*> >*)_table)->clear();
		break;
	case TypeReal:
		fIt = ((map<float, vector<void*> >*)_table)->begin();
		fMax = ((map<float, vector<void*> >*)_table)->end();
		for( ; fIt != fMax; fIt++ )
		{
			max = (int)fIt->second.size();
			for( i = 0; i < max; i++ )
			{
				free( fIt->second[i] );
			}
		}
		((map<float, vector<void*> >*)_table)->clear();
		break;
	case TypeVarChar:
		sIt = ((map<string, vector<void*> >*)_table)->begin();
		sMax = ((map<string, vector<void*> >*)_table)->end();
		for( ; sIt != sMax; sIt++ )
		{
			max = (int)sIt->second.size();
			for( i = 0; i < max; i++ )
			{
				free( sIt->second[i] );
			}
		}
		((map<string, vector<void*> >*)_table)->clear();
		break;
	}
}

inMemoryHashTable::~inMemoryHashTable()
{
	//free contents of vectors in each map entry
	clearTable();

	//free instance of table
	switch(_type)
	{
	case TypeInt:
		delete (map<int, vector<void*> >*)_table;
		break;
	case TypeReal:
		delete (map<float, vector<void*> >*)_table;
		break;
	case TypeVarChar:
		delete (map<string, vector<void*> >*) _table;
		break;
	}
}

void inMemoryHashTable::insertRecord
	(const void* recordData, const unsigned int offsetToKeyField, const unsigned int recordLength)
{
	//copy the record into a separately maintained buffer
	void* buf = malloc(recordLength);
	memcpy(buf, recordData, recordLength);

	//my compiler does not like variables inside switch statement => gives "cross initializaton" errors
	//so pushing all such variables outside of switch statement
	int ikey = 0; float fkey = 0.0f; string skey = "";
	map<int, vector<void*> >* iTable; map<float, vector<void*> >* fTable; map<string, vector<void*> >* sTable;
	map<int, vector<void*> >::iterator iEntryIt; map<float, vector<void*> >::iterator fEntryIt; map<string, vector<void*> >::iterator sEntryIt;
	int lengthOfKey = 0;
	char* charArr = NULL;

	//depending on the type of the key, access table differently
	switch(_type)
	{
	case TypeInt:
		//determine integer key
		ikey = ((int*) ((const char*)recordData + offsetToKeyField) )[0];

		//cast _table to map pointer for easier access afterwards
		iTable = ((map< int, vector<void*> >*)_table);


		//check if such key already been inserted
		if( (iEntryIt = iTable->find(ikey)) == iTable->end() )
		{
			//no entry exists, insert new one
			iTable->insert
				( std::pair<int, vector<void*> >( ikey, vector<void*>() ) );

			//reset entry iterator
			iEntryIt = iTable->find(ikey);
		}

		//insert new entry
		iEntryIt->second.push_back(buf);
		break;
	case TypeReal:
		//determine real key
		fkey = ((float*) ((const char*)recordData + offsetToKeyField) )[0];

		//cast _table to map pointer for easier access afterwards
		fTable = ((map< float, vector<void*> >*)_table);


		//check if such key already been inserted
		if( (fEntryIt = fTable->find(fkey)) == fTable->end() )
		{
			//no entry exists, insert new one
			fTable->insert
				( std::pair<float, vector<void*> >( fkey, vector<void*>() ) );

			//reset entry iterator
			fEntryIt = fTable->find(fkey);
		}

		//insert new entry
		fEntryIt->second.push_back(buf);
		break;
	case TypeVarChar:
		//determine length of the key
		lengthOfKey = ((int*) ((const char*)recordData + offsetToKeyField) )[0];

		//create string object to represent VarChar key
		charArr = (char*)malloc(lengthOfKey + 1);
		charArr[lengthOfKey] = '\0';	//null terminated character array
		memcpy( charArr, (char*)recordData + offsetToKeyField + sizeof(int), lengthOfKey );
		string skey = string(charArr);
		free(charArr);

		//cast _table to map pointer for easier access afterwards
		sTable = ((map< string, vector<void*> >*)_table);

		//check if such key already been inserted
		if( (sEntryIt = sTable->find(skey)) == sTable->end() )
		{
			//no entry exists, insert new one
			sTable->insert
				( std::pair<string, vector<void*> >( skey, vector<void*>() ) );

			//reset entry iterator
			sEntryIt = sTable->find(skey);
		}

		//insert new entry
		sEntryIt->second.push_back(buf);
		break;
	}
}

void determineAttribute(const Iterator* it, const Condition cond, const bool isLeft, Attribute& outAttr)
{
	string attrName = cond.lhsAttr;
	if( isLeft == false )
	{
		if( cond.bRhsIsAttr == false )
		{
			//somehow need to integrate with rhsValue, not sure how?
			outAttr.type = cond.rhsValue.type;
			outAttr.name = cond.rhsAttr;
			outAttr.length = 0;
			return;
		}
		attrName = cond.rhsAttr;
	}
	vector<Attribute> attrs;

	it->getAttributes(attrs);

	vector<Attribute>::iterator i = attrs.begin(), max = attrs.end();
	for( ; i != max; i++ )
	{
		if( i->name == attrName )
		{
			outAttr.length = i->length;
			outAttr.name = i->name;
			outAttr.type = i->type;
			break;
		}
	}
}

BNLJoin::BNLJoin(Iterator *leftIn,	TableScan *rightIn, const Condition &condition, const unsigned numRecords)
: _hashTable(NULL)
{
	//determine attribute of the outer relation
	determineAttribute(leftIn, condition, true, _outer);

	//determine attribute of the inner relation
	determineAttribute(rightIn, condition, false, _inner);

	//set inner value
	_innerValue = condition.rhsValue.data;

	//set operator
	_operator = condition.op;

	//set block size
	_blockSize = numRecords;

	//ensure that both iterators are providing the same type of input values
	if( _outer.type != _inner.type )
	{
		exit(-62);	//joining on non-equivalent attribute types
	}

	leftIn->getAttributes(_outerDesc);
	rightIn->getAttributes(_innerDesc);

	//now, allocate hash table
	_hashTable = new inMemoryHashTable(_outer.type, _outerDesc);

	//set list of attributes
	_attrDesc.insert(_attrDesc.end(), _outerDesc.begin(), _outerDesc.end());
	_attrDesc.insert(_attrDesc.end(), _innerDesc.begin(), _innerDesc.end());

	//set iterator for inner relation
	_innerRelation = rightIn;

	//set iterator for outer relation
	_outerRelation = leftIn;

	//set that we are not yet finished
	_finishedProcessing = false;

	RC errCode = 0;

	//load the first block of outer relation
	loadNextBlock();
}
;

RC BNLJoin::reloadInnerRelation()
{
	//reload
	_innerRelation->setIterator();

	//success
	return 0;
}

RC BNLJoin::loadNextBlock()
{
	RC errCode = 0;

	//remove all records from the previous block
	_hashTable->clearTable();

	//keep number of records added to the block
	int curTupleNum = 0;

	//allocate buffer for iterated record
	void* data = malloc(PAGE_SIZE);
	memset(data, 0, PAGE_SIZE);

	//populate block until either maximum number of records per block is reached OR outer iterator is exhausted
	while( curTupleNum < _blockSize )
	{
		//get next record from the outer relation
		if( (errCode = _outerRelation->getNextTuple(data)) != 0 )
		{
			//quit, since reached end
			_finishedProcessing = true;
			break;
		}

		//determine position of the field on which to join
		int offset = 0;
		if( (errCode = getOffsetToProperField(data, _outerDesc, _outer, offset)) != 0 )
		{
			free(data);
			return errCode;
		}

		//determine size of the record
		int length = sizeOfRecord(_outerDesc, data);

		//insert record into hash table
		_hashTable->insertRecord(data, offset, length);

		//increment number of tuples in this block
		curTupleNum++;
	}

	free(data);

	//success
	return errCode;
}

BNLJoin::~BNLJoin() {
	//deallocate hash table
	delete _hashTable;
}
;

RC BNLJoin::getNextTuple(void *data) {
	//allocate buffer for iterated record
	void* recordBuf = malloc(PAGE_SIZE);
	memset(recordBuf, 0, PAGE_SIZE);

	RC errCode = 0;

	while(true)
	{
		//get tuple from the inner relation
		if( (errCode = _innerRelation->getNextTuple(recordBuf)) != 0 )
		{
			//next block
			if( (errCode = loadNextBlock()) != 0 )
			{
				free(recordBuf);
				return errCode;
			}

			//check if reached the end
			if(  _finishedProcessing )
			{
				//return end of joining procedure
				free(recordBuf);
				return QE_EOF;
			}

			//reload the inner relation
			reloadInnerRelation();
		}

		//determine position of the field on which to join
		int offset = 0;
		if( (errCode = getOffsetToProperField(recordBuf, _innerDesc, _inner, offset)) != 0 )
		{
			free(recordBuf);
			return errCode;
		}

		//determine pointer to the field
		void* ptr = (char*)recordBuf + offset;

		//records
		void* recordData = malloc(PAGE_SIZE);
		memset(recordData, 0, PAGE_SIZE);
		unsigned int lengthOuter = 0;

		//check if this record is in hash table
		if( _hashTable->getRecord(ptr, recordData, lengthOuter) )
		{
			//found match

			//determine size of the record
			int lengthInner = sizeOfRecord(_innerDesc, recordBuf);

			//copy outer and then inner records into out data buffer
			memcpy(data, recordData, lengthOuter);
			data = (char*)data + lengthOuter;
			memcpy(data, recordBuf, lengthInner);

			//quit and return to the caller
			free(recordData);
			break;
		}

		free(recordData);
	}

	free(recordBuf);

	//success
	return errCode;
}
;

void BNLJoin::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = _attrDesc;
}
;

/**
 * Index Nested-Loop Join Class:
 **/
INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn,
		const Condition &condition) {

}
;

INLJoin::~INLJoin() {

}
;

RC INLJoin::getNextTuple(void *data) {
	return QE_EOF;
}
;

void INLJoin::getAttributes(vector<Attribute> &attrs) const {

}
;

/**
 * Aggregate Class:
 **/
Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op) {
	//set shared data members
	_isGroupByAggregation = false;
	_inputStream = input;
	_aggAttr = aggAttr;
	_aggOp = op;
	AttrType type;
	int size;
	determineOutputType(type, size);
	_attributes.push_back
		( (Attribute){ generateOpName() + "()" /*"(" + _aggAttr.name + ")"*/, type, (unsigned int)size} );
	//determine type of records that this input stream will supply
	_inputStream->getAttributes(_inputStreamDesc);
	//set the group-by aggregation data members to default values (they will not be used)
	_groupByAttr = (Attribute){"", TypeInt, 0};
	_numOfPartitions = 0;
	_isFinishedProcessing = false;
}
;

void Aggregate::determineOutputType(AttrType& type, int& size)
{
	if( _aggOp == MIN || _aggOp == MAX || _aggOp == SUM )
	{
		type = _aggAttr.type;
		size = _aggAttr.length;
	}
	else if( _aggOp == COUNT )
	{
		type = TypeInt;
		size = sizeof(int);
	}
	else
	{
		type = TypeReal;
		size = sizeof(float);
	}
}

string Aggregate::generatePartitionName(unsigned int partitionNumber)
{
	return _aggAttr.name + "__" +  _groupByAttr.name + "__" + to_string(partitionNumber);
}

// Optional for everyone. 5 extra-credit points
// Group-based hash aggregation
Aggregate::Aggregate(Iterator *input, Attribute aggAttr, Attribute groupAttr,
		AggregateOp op, const unsigned numPartitions) {
	//set shared data members
	_isGroupByAggregation = true;
	_inputStream = input;
	_aggAttr = aggAttr;
	_aggOp = op;
	AttrType type;
	int size;
	determineOutputType(type, size);
	//set data members specifically used in group-by aggregation
	_groupByAttr = groupAttr;
	_numOfPartitions = numPartitions;
	//group-by attribute
	_groupByDesc.push_back( _groupByAttr );
	_attributes.push_back( _groupByAttr );
	_attributes.push_back
		( (Attribute){ generateOpName() + "(" + _groupByAttr.name + ")", type, (unsigned int)size } );
	//determine type of records that this input stream will supply
	_inputStream->getAttributes(_inputStreamDesc);
	_isFinishedProcessing = false;

	/*
	 * So the basic algorithm is outlined in lecture 11, slide # 14
	 * The idea is to scan (iterate) over the records and perform couple of things on the fly with them:
	 * 	1. hash the group field of the record to determine the bucket to which one it belongs
	 * 	2. load in the bucket and scan thru it to find the entry (if such entry in bucket exists)
	 * 	   that represents current operator status (MAX or MIN or SUM or COUNT or AVG).
	 * 	   The status includes (just like on the diagram in that lecture):
	 * 	   	< group-by attribute , operator's current values >
	 * 	   	Operator's current values:
	 * 	   		MAX, MIN, SUM => _aggAttr.type
	 * 	   		COUNT => integer
	 * 	   		AVG => < _aggAttr.type , integer >
	 *  3. If entry exists => update it
	 *    If such entry does not exist => insert it with the given _aggAttr values in place of operator's values
	 *  4. setup iterator to the 0th bucket to use it inside getNextEntry procedure
	 */

	//operator's current value (aggregate value)
	if( _aggOp == MAX || _aggOp == MIN || _aggOp == SUM || _aggOp == AVG )
	{
		_groupByDesc.push_back( _aggAttr );
	}
	//if necessary counter value
	if( _aggOp == AVG || _aggOp == COUNT )
	{
		_groupByDesc.push_back( (Attribute){"", TypeInt, sizeof(int)} );
	}

	//create a description of record but only with names
	for(unsigned int j = 0; j < _groupByDesc.size(); j++)
	{
		_groupByNames.push_back( _groupByDesc[j].name );
	}

	//loop thru input index
	void* buffer = malloc(PAGE_SIZE);
	memset(buffer, 0, PAGE_SIZE);

	RC errCode = 0;
	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();

	//create as many buckets as necessary (each bucket is a separate RBFM file)
	for( unsigned int j = 0; j < _numOfPartitions; j++ )
	{
		rbfm->createFile( generatePartitionName(j) );
	}

	//loop thru iterator and change in-memory values
	while( _inputStream->getNextTuple(buffer) == 0 )
	{
		//get offset to the aggregated record field
		int offset = 0;
		if( (errCode = getOffsetToProperField(buffer,
				_inputStreamDesc, _aggAttr, offset)) != 0 )
		{
			free(buffer);
			exit(errCode);
		}

		//calculate pointer to the aggregated field
		void* aggregateField = (char*)buffer + offset;

		offset = 0;
		//get offset to grouped record field
		if( (errCode = getOffsetToProperField(buffer,
				_inputStreamDesc, _groupByAttr, offset)) != 0 )
		{
			free(buffer);
			exit(errCode);
		}

		//calculate pointer to the grouped field
		void* groupByField = (char*)buffer + offset;

		//hash grouped field to determine bucket number
		unsigned int hashKey = IndexManager::instance()->hash(_groupByAttr, groupByField);
		unsigned int bktNumber = hashKey % _numOfPartitions;

		//now scan thru the partition to find out whether record with the group-by name already exists
		RBFM_ScanIterator it;
		FileHandle fileHandle;

		//open file that represents bucket
		if( (errCode = rbfm->openFile( generatePartitionName(bktNumber), fileHandle )) != 0 )
		{
			free(buffer);
			exit(errCode);
		}

		//open scan
		if( (errCode = rbfm->scan(fileHandle, _groupByDesc, "", NO_OP, NULL, _groupByNames, it)) != 0 )
		{
			free(buffer);
			exit(errCode);
		}

		//iterate over the records to find one with the same group-by attribute
		RID recRid;
		void* recBuf = malloc(PAGE_SIZE);
		memset(recBuf, 0, PAGE_SIZE);
		bool recordIsFound = false;
		while( it.getNextRecord(recRid, recBuf) == 0 )
		{
			//determine if this record is the one, i.e. it's first field (group-by attribute) matches
			//one that is coming from input stream record
			if( compareTwoField(recBuf, groupByField, _groupByAttr.type, EQ_OP) )
			{
				recordIsFound = true;
				//setup offset appropriately
				offset = 0;
				switch(_groupByAttr.type)
				{
				case TypeInt:
					offset = sizeof(int);
					break;
				case TypeReal:
					offset = sizeof(float);
					break;
				case TypeVarChar:
					offset = ((int*)groupByField)[0] + sizeof(int);
					break;
				}
				//advance offset yet another time by 4 in case of MAX, MIN, SUM, or AVG (but not for COUNT)
				if( _aggOp == MIN || _aggOp == MAX || _aggOp == SUM || _aggOp == AVG )
				{
					offset += 4;
				}
				break;
			}
		}

		if( recordIsFound == false )
		{
			//place group-by attribute value as a first item in the new record
			offset = 0;
			switch(_groupByAttr.type)
			{
			case TypeInt:
				((int*)recBuf)[0] = ((int*)groupByField)[0];
				offset = sizeof(int);
				break;
			case TypeReal:
				((float*)recBuf)[0] = ((float*)groupByField)[0];
				offset = sizeof(float);
				break;
			case TypeVarChar:
				offset = ((int*)groupByField)[0] + sizeof(int);
				memcpy(recBuf, groupByField, offset);
				break;
			}

			//if no record was found then one needs to be created
			//"allocate" value of aggregate type for MIN, MAX, SUM, and AVG
			if( _aggOp == MIN || _aggOp == MAX || _aggOp == SUM || _aggOp == AVG )
			{
				//depending on the type of aggregate value allocate either integer or float
				switch(_aggAttr.type)
				{
				case TypeInt:
					//in case this is MIN or MAX, set lower/upper bound
					if( _aggOp == MAX )
					{
						((int*) ((char*)recBuf + offset) )[0] = INT32_MIN;
					}
					else if( _aggOp == MIN )
					{
						((int*) ((char*)recBuf + offset) )[0] = INT32_MAX;
					}
					else
					{
						((int*) ((char*)recBuf + offset) )[0] = 0;
					}
					break;
				case TypeReal:
					//in case this is MIN or MAX, set lower/upper bound
					if( _aggOp == MAX )
					{
						((float*) ((char*)recBuf + offset) )[0] = FLT_MIN;
					}
					else if( _aggOp == MIN )
					{
						((float*) ((char*)recBuf + offset) )[0] = FLT_MAX;
					}
					else
					{
						((float*) ((char*)recBuf + offset) )[0] = 0;
					}
					break;
				case TypeVarChar:
					free(buffer);
					free(recBuf);
					exit(errCode);	//aggregated attribute cannot be VarChar
				}
				//point at the end of the added field
				//since both float and integer is 4 bytes long, so increment offset by 4
				offset += 4;
			}
			//"allocate" integer for AVG, and COUNT
			if( _aggOp == AVG || _aggOp == COUNT )
			{
				((int*) ((char*)recBuf + offset) )[0] = 0;	//set counter to 0
			}
		}

		//depending on the type perform comparisons and/or updates
		switch(_aggOp)
		{
		case MAX:
			//if new value > current maximum (inside bucket), then
			if( compareTwoField
					(aggregateField, (char*)recBuf + offset - 4, _aggAttr.type, GT_OP) )
			{
				//place the current maximum with new value
				memcpy((char*)recBuf + offset - 4, aggregateField, 4);
			}
			break;
		case MIN:
			//if new value < current minimum (inside bucket), then
			if( compareTwoField
					(aggregateField, (char*)recBuf + offset - 4, _aggAttr.type, LT_OP) )
			{
				//place the current minimum with new value
				memcpy((char*)recBuf + offset - 4, aggregateField, 4);
			}
			break;
		case SUM:
		case AVG:
			//add up new value to the one stored inside bucket
			if( _aggAttr.type == TypeInt )
			{
				((int*) ((char*)recBuf + offset - 4) )[0] += ((int*)aggregateField)[0];
			}
			else if( _aggAttr.type == TypeReal )
			{
				((float*) ((char*)recBuf + offset - 4) )[0] += ((float*)aggregateField)[0];
			}
			else
			{
				free(buffer);
				free(recBuf);
				exit(errCode);	//aggregated attribute cannot be VarChar
			}
			break;
		default:
			break;
		}

		//for AVG, we need to count number of elements processed to compute average value
		//and obviously for count same operation is required
		if( _aggOp == AVG || _aggOp == COUNT )
		{
			((int*) ((char*)recBuf + offset) )[0] += 1;
		}

		//now depending on whether record was found in the bucket or not, either update existing
		//record or insert a new one
		if( recordIsFound )
		{
			if( (errCode = rbfm->updateRecord(fileHandle, _groupByDesc, recBuf, recRid)) != 0 )
			{
				free(buffer);
				free(recBuf);
				exit(errCode);
			}
		}
		else
		{
			if( (errCode = rbfm->insertRecord(fileHandle, _groupByDesc, recBuf, recRid)) != 0 )
			{
				free(buffer);
				free(recBuf);
				exit(errCode);
			}
		}

		//deallocate buffer for record
		free(recBuf);

		//close iterator for the buckets
		it.close();
	}

	//deallocate buffer for input stream data
	free(buffer);

	//setup group iterator that will be used in GetNextEntry
	//now scan thru the partition to find out whether record with the group-by name already exists

	//start from the 0th bucket and advance thru all others
	_groupByCurrentBucketNumber = 0;

	//open file that represents bucket
	if( (errCode = rbfm->openFile( generatePartitionName(_groupByCurrentBucketNumber), _groupByHandle )) != 0 )
	{
		free(buffer);
		exit(errCode);
	}

	//open scan
	if( (errCode = rbfm->scan(_groupByHandle, _groupByDesc, "", NO_OP, NULL, _groupByNames, _groupByIterator)) != 0 )
	{
		free(buffer);
		exit(errCode);
	}
}
;

RC Aggregate::nextBucket()
{
	RC errCode = 0;

	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();

	if( (errCode = _groupByIterator.close()) != 0 )
	{
		return errCode;
	}

	_groupByCurrentBucketNumber++;
	if( _groupByCurrentBucketNumber >= (int)_numOfPartitions )
	{
		//clean up partition files
		for( int i = 0; i < (int)_numOfPartitions; i++ )
		{
			rbfm->destroyFile( generatePartitionName(i) );
		}
		return QE_EOF;	//no more buckets to search in
	}

	_groupByHandle = FileHandle();

	//open file that represents bucket
	if( (errCode = rbfm->openFile( generatePartitionName(_groupByCurrentBucketNumber), _groupByHandle )) != 0 )
	{
		return errCode;
	}

	//open scan
	if( (errCode = rbfm->scan(_groupByHandle, _groupByDesc, "", NO_OP, NULL, _groupByNames, _groupByIterator)) != 0 )
	{
		return errCode;
	}

	//success
	return errCode;
}

Aggregate::~Aggregate() {
//nothing to be done
}
;

string Aggregate::generateOpName()
{
	switch(_aggOp)
	{
	case MIN:
		return "MIN";
	case MAX:
		return "MAX";
	case SUM:
		return "SUM";
	case AVG:
		return "AVG";
	case COUNT:
		return "COUNT";
	}
	//should not reach, but compiler is complaining about not having the end return statement, so let it be...
	return "";
};

RC Aggregate::getNextTuple(void *data) {
	//since two types of aggregation are not very similar, they are split into
	//two separate functions
	if( _isGroupByAggregation == true )
		return next_groupBy(data);
	return next_basic(data);
}
;

RC Aggregate::next_basic(void* data)
{
	RC errCode = 0;

	if( _isFinishedProcessing )
		return QE_EOF;

	/*
	 * for aggregation purposes keep in memory important values:
	 * 	AGG_TYPE   NUM_OF_IN_MEM_VALUES   TYPE_OF_IN_MEM_VALUE1   TYPE_OF_IN_MEM_VALUE2
	 * 	   MIN	           1                 aggAttr.Type               NONE
	 * 	   MAX             1                 aggAttr.Type               NONE
	 * 	   SUM             1                 aggAttr.Type               NONE
	 * 	  COUNT            1                   integer                  NONE
	 * 	   AVG             2                 aggAttr.Type              integer
	 *
	 * From project description: The aggregated attribute can be INT or REAL.
	 */
	vector<void*> inMemValues;

	void* elem = NULL;
	//allocate value of aggregate type for MIN, MAX, SUM, and AVG
	if( _aggOp == MIN || _aggOp == MAX || _aggOp == SUM || _aggOp == AVG )
	{
		//depending on the type of aggregate value allocate either integer or float
		switch(_aggAttr.type)
		{
		case TypeInt:
			elem = new int(0);
			//in case this is MIN or MAX, set lower/upper bound
			if( _aggOp == MAX )
			{
				((int*)elem)[0] = INT32_MIN;
			}
			else if( _aggOp == MIN )
			{
				((int*)elem)[0] = INT32_MAX;
			}
			break;
		case TypeReal:
			elem = new float(0);
			//in case this is MIN or MAX, set lower/upper bound
			if( _aggOp == MAX )
			{
				((float*)elem)[0] = FLT_MIN;
			}
			else if( _aggOp == MIN )
			{
				((float*)elem)[0] = FLT_MAX;
			}
			break;
		case TypeVarChar:
			return -61;	//aggregated attribute cannot be VarChar
		}
		inMemValues.push_back(elem);
	}
	//allocate integer for AVG, and COUNT
	if( _aggOp == AVG || _aggOp == COUNT )
	{
		elem = new int(0);
		inMemValues.push_back(elem);
	}

	void* buffer = malloc(PAGE_SIZE);
	memset(buffer, 0, PAGE_SIZE);

	//loop thru iterator and change in-memory values
	while( _inputStream->getNextTuple(buffer) == 0 )
	{
		//get offset to the proper record field
		int offset = 0;
		if( (errCode = getOffsetToProperField(buffer,
				_inputStreamDesc, _aggAttr, offset)) != 0 )
		{
			free(buffer);
			return errCode;
		}

		//calculate pointer to the field
		void* buf = (char*)buffer + offset;

		//depending on the type perform comparisons and/or updates
		switch(_aggOp)
		{
		case MAX:
			//if new value (stored in buf) > current maximum (stored in inMemValues[0]), then
			if( compareTwoField(buf, inMemValues[0], _aggAttr.type, GT_OP) )
			{
				//place the current maximum with new value
				memcpy(inMemValues[0], buf, 4);
			}
			break;
		case MIN:
			//if new value (stored in buf) < current minimum (stored in inMemValues[0]), then
			if( compareTwoField(buf, inMemValues[0], _aggAttr.type, LT_OP) )
			{
				//place the current minimum with new value
				memcpy(inMemValues[0], buf, 4);
			}
			break;
		case SUM:
		case AVG:
			//add up new value to the one stored inside in-memory
			if( _aggAttr.type == TypeInt )
			{
				((int*)inMemValues[0])[0] += ((int*)buf)[0];
			}
			else if( _aggAttr.type == TypeReal )
			{
				((float*)inMemValues[0])[0] += ((float*)buf)[0];
			}
			else
			{
				return -61;	//aggregated attribute cannot be VarChar
			}
			break;
		default:
			break;
		}

		//for AVG, we need to count number of elements processed to compute average value
		//and obviously for count same operation is required
		if( _aggOp == AVG || _aggOp == COUNT )
		{
			//just add 1
			((int*)inMemValues[ _aggOp == COUNT ? 0 : 1 ])[0] += 1;
		}
	}

	free(buffer);

	//in case operation is AVG then divide SUM (stored in 0th element) by COUNT stored in 1st element
	//and store the result inside the 0th element
	if( _aggOp == AVG )
	{
		float currentSum = 0.0f;
		if( _aggAttr.type == TypeInt )
			currentSum = (float) ( (int*)inMemValues[0] )[0];

		((float*)inMemValues[0])[0] = currentSum / (float)((int*)inMemValues[1])[0];
	}

	//copy the result into output buffer
	memcpy( data, inMemValues[0], 4 );

	//deallocate in memory important values
	for( int i = 0; i < (int)inMemValues.size(); i++ )
	{
		free( inMemValues[i] );
	}

	//set that we are finished processing, so that next iteration will immediately return EOF
	_isFinishedProcessing = true;

	//success
	return errCode;
};

RC Aggregate::next_groupBy(void* data)
{
	RC errCode = 0;

	//check if there is more data to be scanned
	if( _isFinishedProcessing )
	{
		//if not then quit
		return QE_EOF;
	}

	//iterate over the records to find one with the same group-by attribute
	RID recRid;
	void* recBuf = malloc(PAGE_SIZE);
	memset(recBuf, 0, PAGE_SIZE);
	bool recordIsFound = false;

	//keep looping until either get a record or find an end
	while( recordIsFound == false )
	{
		if( _groupByIterator.getNextRecord(recRid, recBuf) == 0 )
		{
			recordIsFound = true;
		}
		else
		{
			//go to next bucket (i.e. open new file)
			if( nextBucket() != 0 )
			{
				//finished scanning
				_isFinishedProcessing = true;
				//if all buckets have been scanned, then return QE_EOF
				return QE_EOF;
			}

			//clean up record buffer
			memset(recBuf, 0 , PAGE_SIZE);
		}
	}

	//copy group-by attribute value
	int size = ((int*)recBuf)[0] + sizeof(int);
	memcpy( data, recBuf, size );
	data = (char*)data + size;

	//update pointer to point to the start of control information for operators
	recBuf = (char*)recBuf + size;

	//in case operation is AVG then divide SUM (stored in 0th element) by COUNT stored in 1st element
	//and store the result inside the 0th element
	if( _aggOp == AVG )
	{
		float currentSum = 0.0f;
		if( _aggAttr.type == TypeInt )
			currentSum = (float) ( (int*)recBuf )[0];
		else if( _aggAttr.type == TypeReal )
			currentSum = (float) ( (int*)recBuf )[0];

		((float*)recBuf)[0] = currentSum / (float)((int*)recBuf + 1)[0];
	}

	//copy the result into output buffer
	memcpy( data, recBuf, 4 );

	//success
	return errCode;
};

// Please name the output attribute as aggregateOp(aggAttr)
// E.g. Relation=rel, attribute=attr, aggregateOp=MAX
// output attrname = "MAX(rel.attr)"
void Aggregate::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = _attributes;
}
;

RC getOffsetToProperField(const void* record, const vector<Attribute> recordDesc, const Attribute properField, int& offset)
{
	RC errCode = 0;

	//set initially offset to 0
	offset = 0;

	//now loop thru record description until the proper field is found
	vector<Attribute>::const_iterator i = recordDesc.begin(), imax = recordDesc.end();
	for (; i != imax; i++)
	{
		//if the proper field is found, then
		if( i->name == properField.name )
		{
			//quit loop and return
			break;
		}

		//update offset
		switch (i->type)
		{
		case TypeInt:
			offset += sizeof(int);
			break;

		case TypeReal:
			offset += sizeof(float);
			break;

		case TypeVarChar:
			offset += sizeof(int) + ( (unsigned int*)((char*)record + offset) )[0];
			break;
		}
	}

	//success
	return errCode;
}
