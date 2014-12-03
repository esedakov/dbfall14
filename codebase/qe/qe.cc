#include "qe.h"
#include <cfloat>

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
BNLJoin::BNLJoin(Iterator *leftIn,	TableScan *rightIn, const Condition &condition, const unsigned numRecords) {

}
;

BNLJoin::~BNLJoin() {

}
;

RC BNLJoin::getNextTuple(void *data) {
	return QE_EOF;
}
;

void BNLJoin::getAttributes(vector<Attribute> &attrs) const {

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
	_attributes.push_back( (Attribute){ generateOpName() + "()" /*"(" + _aggAttr.name + ")"*/, type, size} );
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
	_attributes.push_back
		( (Attribute){ generateOpName() + "(" + _groupByAttr.name + ")", type, size } );
	//set data members specifically used in group-by aggregation
	_groupByAttr = groupAttr;
	_numOfPartitions = numPartitions;
	//set default value for is finished processing, since it will not be used here
	_isFinishedProcessing = false;
}
;

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

RC Aggregate::getOffsetToProperField(const void* record, const vector<Attribute> recordDesc, const Attribute properField, int& offset)
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

	//determine type of records that this input stream will supply
	vector<Attribute> recordDesc;
	_inputStream->getAttributes(recordDesc);

	//loop thru iterator has and change in-memory values
	while( _inputStream->getNextTuple(buffer) == 0 )
	{
		//get offset to the proper record field
		int offset = 0;
		if( (errCode = getOffsetToProperField(buffer,
				recordDesc, _aggAttr, offset)) != 0 )
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
			//if new value (stored in buf) > current maximum (storeed in inMemValues[0]), then
			if( compareTwoField(buf, inMemValues[0], _aggAttr.type, GT_OP) )
			{
				//place the current maximum with new value
				memcpy(inMemValues[0], buf, 4);
			}
			break;
		case MIN:
			//if new value (stored in buf) < current minimum (storeed in inMemValues[0]), then
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

	//

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

