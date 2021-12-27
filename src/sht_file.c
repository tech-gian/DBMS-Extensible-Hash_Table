#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "sht_file.h"
#include "common.h"
#define MAX_OPEN_FILES 20

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HT_ERROR;        \
  }                         \
}

// #ifndef HASH_FILE_H
// #define HASH_FILE_H


typedef enum HT_ErrorCode {
  HT_OK,
  HT_ERROR
} HT_ErrorCode;

typedef struct Record {
	int id;
	char name[15];
	char surname[20];
	char city[20];
} Record;

typedef struct{
	char index_key[20];
	int tupleId;  /*Ακέραιος που προσδιορίζει το block και τη θέση μέσα στο block στην οποία     έγινε η εισαγωγή της εγγραφής στο πρωτεύον ευρετήριο.*/ 
}SecondaryRecord;



HT_ErrorCode SHT_Init() {
	//insert code here
	return HT_OK;
}

HT_ErrorCode SHT_CreateSecondaryIndex(const char *sfileName, char *attrName, int attrLength, int depth,char *fileName ) {
	//insert code here
	return HT_OK;
}

HT_ErrorCode SHT_OpenSecondaryIndex(const char *sfileName, int *indexDesc  ) {
	//insert code here
	return HT_OK;
}

HT_ErrorCode SHT_CloseSecondaryIndex(int indexDesc) {
	//insert code here
	return HT_OK;
}
 
HT_ErrorCode SHT_SecondaryInsertEntry (int indexDesc,SecondaryRecord record  ) {
	//insert code here
	// //! den einai record id, alla to pedio pou kanoume hash. Tha to paro apo gianni
	unsigned int key = hash_string(record.id);	//hash record id

	int gdepth = get_global_depth(openFiles[indexDesc]->fd);

	//Pick the proper number of bits. For example if gd=2 and key is 32 bits, keep the first 2 
	key = key >> (sizeof(int)*8-gdepth);

	//Just in case gdepth is 0
	if(gdepth == 0){
		key = 0;
	}

	//find in which block, new record will be inserted
	int block_index = find_hash_table_block(openFiles[indexDesc]->fd ,key);

	BF_Block* block;
	BF_Block_Init(&block);

	if(block_index == -1){
		return HT_ERROR;
	}

	CALL_BF(BF_GetBlock(openFiles[indexDesc]->fd,block_index,block));
	
	int ldepth = get_local_depth(block);
	
	if(ldepth == -1){
		CALL_BF(BF_UnpinBlock(block));	
		BF_Block_Destroy(&block);
		return HT_ERROR;
	}

	CALL_BF(BF_UnpinBlock(block));	
	BF_Block_Destroy(&block);
	
		//if there is free space in the block, insert the new record
	if (sht_insert_record(&record, indexDesc , block_index) == HT_OK) {
		return HT_OK;
	}
	else {	// if no free space is available
		
		if (ldepth < gdepth){	//1st case
			int buddies_number= power_of_two(gdepth)/power_of_two(ldepth);
			sht_arrange_buckets(indexDesc,buddies_number,&record,key);		//split the bucket
																		// increase bucket's depth
																		// re-insert record
		}
		else{	//aextend hash table and try to re-insert the record
			sht_extend_hash_table(openFiles[indexDesc]->fd);
			return SHT_InsertEntry(indexDesc,record);
		}
	}
	
	return HT_OK;

}

HT_ErrorCode SHT_SecondaryUpdateEntry (int indexDesc, UpdateRecordArray *updateArray ) {
  
	// store global depth information, usefull for hashing
	int globalDepth = get_global_depth(indexDesc);

	BF_Block* block;
	BF_Block_Init(&block);

	int maxNumberOfRecordsInPrimary = ((BF_BLOCK_SIZE - sizeof(char) - 2*sizeof(int)) * 8) / (sizeof(Record)*8 + 1);
	int maxNUmberOfRecordsInSecondary = ((BF_BLOCK_SIZE - sizeof(char) - 2*sizeof(int)) * 8) / (sizeof(SecondaryRecord)*8 + 1);
	
	int indexesBytesInSec=maxNUmberOfRecordsInSecondary % (sizeof(char)*8) == 0 ? 
		maxNUmberOfRecordsInSecondary / (sizeof(char)*8) : 
		maxNUmberOfRecordsInSecondary / (sizeof(char)*8) + 1;
	
	
	SecondaryRecord* secRecord = malloc(sizeof(SecondaryRecord));			//! kane to free
	
	//* an kapoia thesi edo einai true simainei oti exei allaxthei.Os timi mpainei to neo tuppleId
	bool* confictsArray = malloc(sizeof(bool)* maxNumberOfRecordsInPrimary);	//! kane ton free

	// iterate through records in the secondrary indexes and update these records which changed position due to split
	// do..while since the same must me done for the overflow buckets as well	
	for(int i=0;i< maxNumberOfRecordsInPrimary;i++){
		
		int overflowBlock = 0;
		// if this particular record has not benn changed
		if(updateArray[i].newTupleId == updateArray[i].oldTupleId){
			continue;
		}
		
		// create key for search
		unsigned int key = hash_string(updateArray[i].city);
		key = key >> (sizeof(int)*8-globalDepth);

		// find which bucket corresponds to this key
		int bucketIndex = find_hash_table_block(indexDesc,key);

		do{
			BF_GetBlock(indexDesc,bucketIndex,block);
			char blockData = BF_Block_GetData(block);
			memcpy(&overflowBlock,blockData+1,sizeof(int));
			bucketIndex = overflowBlock;

			for(int j=0;j<maxNUmberOfRecordsInSecondary;j++){

				//! check flags
				char flagValue;
				memcpy(&flagValue, blockData+1+2*sizeof(int) + j/8, sizeof(char));
				
				flagValue = flagValue << (j%8);
				flagValue = flagValue >> 7;
				if (flagValue == 0){
					continue;
				}

				memcpy(secRecord,blockData+1+2*sizeof(int)+indexesBytesInSec+i*sizeof(SecondaryRecord),sizeof(SecondaryRecord));
				if ((secRecord->tupleId == updateArray[i].oldTupleId) && confictsArray[j] == false ){

					// update secRecord-> tupleId with newTupleId
					secRecord->tupleId = updateArray[i].newTupleId;
					memcpy(blockData+1+2*sizeof(int)+indexesBytesInSec+i*sizeof(SecondaryRecord),secRecord,sizeof(SecondaryRecord));

					// set block dirty since its data is changed
					BF_Block_SetDirty(block);

					// there is no need to continue the iteration
					overflowBlock = 0;
					confictsArray[j] = true; 
					break;
				}

			}
			BF_UnpinBlock(block);
		}while(overflowBlock != 0);
		
	}

	BF_Block_Destroy(&block);
	
	return HT_OK;
}

HT_ErrorCode SHT_PrintAllEntries(int sindexDesc, char *index_key ) {
	//insert code here
	return HT_OK;
}

HT_ErrorCode SHT_HashStatistics(char *filename ) {
	//insert code here
	return HT_OK;
}

HT_ErrorCode SHT_InnerJoin(int sindexDesc1, int sindexDesc2,  char *index_key ) {
	//insert code here
	return HT_OK;
}


// #endif // HASH_FILE_H