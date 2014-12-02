#include "qe.h"

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
	for (; i < originalAttrs.size(); i++) {
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
	void *tuple;
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
		switch (attrs[i].type) {

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

}
;

// Optional for everyone. 5 extra-credit points
// Group-based hash aggregation
Aggregate::Aggregate(Iterator *input, Attribute aggAttr, Attribute groupAttr,
		AggregateOp op, const unsigned numPartitions) {

}
;

Aggregate::~Aggregate() {

}
;

RC Aggregate::getNextTuple(void *data) {
	return QE_EOF;
}
;

// Please name the output attribute as aggregateOp(aggAttr)
// E.g. Relation=rel, attribute=attr, aggregateOp=MAX
// output attrname = "MAX(rel.attr)"
void Aggregate::getAttributes(vector<Attribute> &attrs) const {

}
;

