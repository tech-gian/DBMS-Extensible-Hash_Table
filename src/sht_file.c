#include "bf.h"
#include "common.h"
#include "sht_blockFunctions.h"
// #include "usefull.h"

#define MAX_OPEN_FILES 20

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {      \
    BF_PrintError(code);    \
    return HT_ERROR;        \
  }                         \
}

// #ifndef HASH_FILE_H
#define HASH_FILE_H


// typedef enum HT_ErrorCode {
//   HT_OK,
//   HT_ERROR
// } HT_ErrorCode;


// typedef struct Record {
// 	int id;
// 	char name[15];
// 	char surname[20];
// 	char city[20];
// } Record;

// typedef struct{
// 	char index_key[20];
// 	int tupleId;  /*Ακέραιος που προσδιορίζει το block και τη θέση μέσα στο block στην οποία     έγινε η εισαγωγή της εγγραφής στο πρωτεύον ευρετήριο.*/ 
// }SecondaryRecord;



HT_ErrorCode SHT_Init() {
    openSHTFilesCount = 0;
    for(int i = 0; i < MAX_OPEN_FILES; ++i){
        openSHTFiles[i] = NULL;
    }
    return HT_OK;
}

HT_ErrorCode SHT_CreateSecondaryIndex(const char *sfileName, char *attrName, int attrLength, int depth,char *fileName ) {

    // 0 for surname, 1 for city
    int attrCategory;

    if(strncmp(attrName, "surname", attrLength) == 0)
        attrCategory = 0;
    else if(strncmp(attrName, "city", attrLength) == 0)
        attrCategory = 1;
    else
        return HT_ERROR;
    
    // Open primary hash table

    // Check if file is already open
    int primaryFD = 0, closePrimaryHT = 1;
    int destIndexPrimary = -1;
	for (int i = 0; i < openFilesCount; ++i) {
		if (!strcmp(openFiles[i]->name, fileName)) {
            destIndexPrimary = i;
			primaryFD = openFiles[i]->fd;
            closePrimaryHT = 0;
            break;
		}
	}

    if(closePrimaryHT && HT_OpenIndex(fileName, &primaryFD) != HT_OK){
		return HT_ERROR;
	}

    // Create a new file

    // Check if no other SHT file can be opened
	if (openSHTFilesCount >= MAX_OPEN_FILES) {
		return HT_ERROR;
	}

	// Check if SHT file is already open
	for (int i=0 ; i<openSHTFilesCount ; ++i) {
		if (!strcmp(openSHTFiles[i]->name, sfileName)) {
			return HT_ERROR;
		}
	}

    int fileDescriptor;
    CALL_BF(BF_CreateFile(sfileName));
    CALL_BF(BF_OpenFile(sfileName, &fileDescriptor));

    BF_Block* block1;
    BF_Block_Init(&block1);

    // Create block 1 (s block)
    CALL_BF(BF_AllocateBlock(fileDescriptor, block1));
    if(SHT_BlockHeaderInit(block1, 's', attrName, fileName) != HT_OK){
		CALL_BF(BF_UnpinBlock(block1));
		BF_Block_Destroy(&block1);
		return HT_ERROR;
	}

    CALL_BF(BF_UnpinBlock(block1));
	BF_Block_Destroy(&block1);

    BF_Block* currentHashBlock;
    BF_Block_Init(&currentHashBlock);

    // Create block 2 (Hash Table)
	CALL_BF(BF_AllocateBlock(fileDescriptor, currentHashBlock));
	if (SHT_BlockHeaderInit(currentHashBlock, 'H', attrName, fileName) != HT_OK) {
		CALL_BF(BF_UnpinBlock(currentHashBlock));
		BF_Block_Destroy(&currentHashBlock);
		return HT_ERROR;
	}

    // Save to it the global depth
    char* hashBlockData = BF_Block_GetData(currentHashBlock);
	memcpy(hashBlockData + 1 + sizeof(int), &depth, sizeof(int));

    int totalBuckets = power_of_two(depth);
	int maxNumberOfBucketsPerHashBlock = (BF_BLOCK_SIZE - 1 - 3*sizeof(int)) / sizeof(int);

	CALL_BF(BF_UnpinBlock(currentHashBlock));

	int nextHB = 1;

    // Loop until all hash blocks have been created
	do {
		BF_Block* newBucket;
		BF_Block_Init(&newBucket);

		CALL_BF(BF_GetBlock(fileDescriptor, nextHB, currentHashBlock));
		hashBlockData = BF_Block_GetData(currentHashBlock);

		int bucketCounter = 0;
		int blockCounter = 0;
		for (bucketCounter = 0 ; bucketCounter < maxNumberOfBucketsPerHashBlock; ++bucketCounter) {
			// Loop until all buckets have been created (total_buckets reaches 0)
			// Or until the current hash block gets filled with
			// the maximum number of buckets that it can save (bucket_counter reaches maxNoOfBucketsPerHashBlock)

			if (!totalBuckets) {
				break;
			}
			
			// Allocate and initialize the new bucket block
			CALL_BF(BF_AllocateBlock(fileDescriptor, newBucket));
			if (SHT_BlockHeaderInit(newBucket, 'D', attrName, fileName) != HT_OK) {	
				return HT_ERROR;
			}
			
			// Write to it the local depth
			char* bucketData = BF_Block_GetData(newBucket);
			memcpy(bucketData + 1 + sizeof(int), &depth, sizeof(int));

			// Save the number of the new bucket block to the current hash block
			CALL_BF(BF_GetBlockCounter(fileDescriptor, &blockCounter));
			--blockCounter;
			memcpy(hashBlockData + 1 + sizeof(int) * (bucketCounter + 3), &blockCounter, sizeof(int));
	
			--totalBuckets;
			BF_Block_SetDirty(newBucket);
			CALL_BF(BF_UnpinBlock(newBucket));

			CALL_HT(BucketStatsInit(fileDescriptor, blockCounter));
		}

		BF_Block_Destroy(&newBucket);

		// Save the total number of buckets of the current hash block to the current hash block
		memcpy(hashBlockData + 1 + 2 * sizeof(int), &bucketCounter, sizeof(int));

		// Check if all buckets were created (total_buckets is 0)
		// If not (total_buckets is greater than 0),
		// we will need to create new hash blocks
		// to accommodate for the "pointers" of the remaining buckets
		
		if (totalBuckets != 0) {

			// Allocate and initialize the new hash block
			BF_Block* newHashBlock;
			BF_Block_Init(&newHashBlock);
			CALL_BF(BF_AllocateBlock(fileDescriptor, newHashBlock));
			if (SHT_BlockHeaderInit(newHashBlock, 'H', attrName, fileName) != HT_OK) {
				CALL_BF(BF_UnpinBlock(newHashBlock));
				BF_Block_Destroy(newHashBlock);
				return HT_ERROR;
			}
			
			// Save the number of the new hash block to the previous hash block
			CALL_BF(BF_GetBlockCounter(fileDescriptor, &blockCounter));
			--blockCounter;
			memcpy(hashBlockData + 1, &blockCounter, sizeof(int));

			// Set the previous hash block to dirty and unpin it
			BF_Block_SetDirty(currentHashBlock);
			CALL_BF(BF_UnpinBlock(currentHashBlock));
			
			// Update the hash block "pointer" to point to the new HB
            nextHB = blockCounter;

			// Save the global depth to the new hash block
			char* newHashBlockData = BF_Block_GetData(newHashBlock);
			memcpy(newHashBlockData + 1 + sizeof(int), &depth, sizeof(int));

			CALL_BF(BF_UnpinBlock(newHashBlock));
			BF_Block_Destroy(&newHashBlock);
		}
		else {
			// Set the current hash block to dirty and unpin it
			BF_Block_SetDirty(currentHashBlock);
			CALL_BF(BF_UnpinBlock(currentHashBlock));
		}
	} while(totalBuckets > 0);

    // Open this file that we have not yet created, so that SHT_SecondaryInsertEntry can insert records to it
    
    // Create SHT structure and update the openSHTFiles array
    struct openSHTFile *currentFile = sizeof(struct openSHTFile);

	currentFile->name = malloc(strlen(sfileName) + 1);
	if (currentFile->name == NULL) {
		CALL_BF(BF_CloseFile(fileDescriptor));
        return HT_ERROR;
	}

    int destIndexSecondary = -1;
    for (int i = 0; i < MAX_OPEN_FILES; ++i) {
		if (openSHTFiles[i] == NULL) {
			destIndexSecondary = i;
            break;
		}
	}
    if(destIndexSecondary == -1)
    {
        CALL_BF(BF_CloseFile(fileDescriptor));
        return HT_ERROR;
    }

	openSHTFiles[destIndexSecondary] = currentFile;

	strcpy(openSHTFiles[destIndexSecondary]->name, sfileName);
	openSHTFiles[destIndexSecondary]->fd = fileDescriptor;
    currentFile->primaryName = fileName;
	++openSHTFilesCount;


    int primaryTotalBlocks = 0;
    CALL_BF(BF_GetBlockCounter(primaryFD, &primaryTotalBlocks));

    // Just to see how many bytes of ευρετήριο we have
	int numberOfFlags = ((BF_BLOCK_SIZE - sizeof(char) - 2*sizeof(int)) * 8) / (sizeof(struct record)*8 + 1);
	int numberOfBytesFlags = numberOfFlags % (sizeof(char)*8) == 0 ? numberOfFlags / (sizeof(char)*8) : numberOfFlags / (sizeof(char)*8) + 1;

    for (int i = 0; i < primaryTotalBlocks ; ++i) {
        BF_Block* block;
        BF_Block_Init(&block);
        CALL_BF(BF_GetBlock(destIndexPrimary, i, block));
        char* data = BF_Block_GetData(block);
        char type;
        memcpy(&type, data, sizeof(char));

        // We don't care about 'H', 'M' or 'm' blocks
        if (type == 'H' || type == 'M' || type == 'm') {
            CALL_BF(BF_UnpinBlock(block));
            BF_Block_Destroy(&block);
            continue;
        }

        // For each flagbit == 1, we get the corresponding Record
        for (int j=0 ; j<numberOfFlags ; ++j) {
            Record* record = malloc(sizeof(Record));
            if (record == NULL) {
                return HT_ERROR;
            }
            char flagValue;

            memcpy(&flagValue, data+1+2*sizeof(int) + j/8, sizeof(char));

            flagValue = flagValue << (j%8);
            flagValue = flagValue >> 7;
            if (flagValue == 0) {
                free(record);
                continue;
            }

            memcpy(record, data+1+2*sizeof(int)+numberOfBytesFlags + j*sizeof(Record), sizeof(Record));

            // tupleId= (blockId+1) *num_of_rec_in_block)+index_of_rec_in_block
            int tupleId = (i + 1) * numberOfFlags + j;
            SecondaryRecord *sr = malloc(sizeof(*sr));
            sr->tupleId = tupleId;

            if(!attrCategory)
                strncpy(sr->index_key, record->surname, 20);
            else
                strncpy(sr->index_key, record->city, 20);

            SHT_SecondaryInsertEntry(destIndexSecondary, *sr);
            free(sr);
            free(record);
        }

        CALL_BF(BF_UnpinBlock(block));
        BF_Block_Destroy(&block);
    }
    

    // Close the new file
    CALL_BF(BF_CloseFile(fileDescriptor));
	free(openSHTFiles[destIndexSecondary]->name);
	free(openSHTFiles[destIndexSecondary]);
    openSHTFiles[destIndexSecondary] = NULL;
	--openSHTFilesCount;

	BF_Block_Destroy(&currentHashBlock);
    if(closePrimaryHT)
        CALL_BF(BF_CloseFile(primaryFD));
    return HT_OK;
}

HT_ErrorCode SHT_OpenSecondaryIndex(const char *sfileName, int *indexDesc) {

    // Check if no other SHT file can be opened
	if (openSHTFilesCount >= MAX_OPEN_FILES) {
		return HT_ERROR;
	}

	// Check if SHT file is already open
	for (int i=0 ; i<openSHTFilesCount ; ++i) {
		if (!strcmp(openSHTFiles[i]->name, sfileName)) {
			return HT_ERROR;
		}
	}

	int sFileDescriptor;
	CALL_BF(BF_OpenFile(sfileName, &sFileDescriptor));
    
    // Get the name of the primary HT file
    BF_Block* block1;
    BF_Block_Init(&block1);
    CALL_BF(BF_GetBlock(sFileDescriptor, 0, block1));
    char *data = BF_Block_GetData(block1);
    char primaryFileName = malloc((strlen(data+2+3*sizeof(int)) + 1) * sizeof(char));
    if(primaryFileName == NULL)
        return HT_ERROR;
    strncpy(primaryFileName, data+2+3*sizeof(int), 20);

    // Check if primary HT is already open
    int pmAlreadyOpen = 0, pmIndex = 0;
    for (int i=0 ; i<openFilesCount ; ++i) {
		if (!strcmp(openFiles[i]->name, primaryFileName)) {
			pmAlreadyOpen = 1;
            pmIndex = i;
            break;
		}
	}
    if(!pmAlreadyOpen){
        fprintf(stderr, "error: cannot open SHT file \"%s\" as the primary HT file \"%s\" is not currently open\n", sfileName, primaryFileName);
        free(primaryFileName);
        CALL_BF(BF_CloseFile(sFileDescriptor));
        return HT_ERROR;
    }

    // Create SHT structure and update the openSHTFiles array
    struct openSHTFile *currentFile = sizeof(struct openSHTFile);

	currentFile->name = malloc(strlen(sfileName) + 1);
	if (currentFile->name == NULL) {
		free(primaryFileName);
        CALL_BF(BF_CloseFile(sFileDescriptor));
        return HT_ERROR;
	}

    int destIndex = -1;
    for (int i = 0; i < MAX_OPEN_FILES; ++i) {
		if (openSHTFiles[i] == NULL) {
			destIndex = i;
            break;
		}
	}
    if(destIndex == -1)
    {
        free(primaryFileName);
        CALL_BF(BF_CloseFile(sFileDescriptor));
        return HT_ERROR;
    }

	openSHTFiles[destIndex] = currentFile;

	strcpy(openSHTFiles[destIndex]->name, sfileName);
	openSHTFiles[destIndex]->fd = sFileDescriptor;
    currentFile->primaryName = primaryFileName;
	++openSHTFilesCount;
	*indexDesc = sFileDescriptor;
	
    return HT_OK;
}

HT_ErrorCode SHT_CloseSecondaryIndex(int indexDesc) {
	if (indexDesc >= MAX_OPEN_FILES) {
		return HT_ERROR;
	}

	CALL_BF(BF_CloseFile(openSHTFiles[indexDesc]->fd));

	if (openSHTFiles[indexDesc]->name != NULL) {
		free(openSHTFiles[indexDesc]->name);
	}
	free(openSHTFiles[indexDesc]);
    openSHTFiles[indexDesc] = NULL;
	--openSHTFilesCount;

	return HT_OK;
}
 
HT_ErrorCode SHT_SecondaryInsertEntry (int indexDesc,SecondaryRecord record  ) {
	//insert code here
	// //! den einai record id, alla to pedio pou kanoume hash. Tha to paro apo gianni
	unsigned int key = hash_string(record.index_key);	//hash record id

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
			return SHT_SecondaryInsertEntry(indexDesc,record);
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
			char* blockData = BF_Block_GetData(block);
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
  	if (sindexDesc >= SHT_openFilesCount) {
		return HT_ERROR;
	}

	int numberBlocks;
	CALL_BF(BF_GetBlockCounter(sindexDesc, &numberBlocks));

	// Just to see how many bytes of ευρετήριο we have
	int numberOfFlags = ((BF_BLOCK_SIZE - sizeof(char) - 2*sizeof(int)) * 8) / (sizeof(SecondaryRecord)*8 + 1);
	int numberOfBytesFlags = numberOfFlags % (sizeof(char)*8) == 0 ? numberOfFlags / (sizeof(char)*8) : numberOfFlags / (sizeof(char)*8) + 1;
	
	// If id is NULL, then we print all the Records of the file
	if (index_key == NULL) {
		for (int i=0 ; i<numberBlocks ; ++i) {
			BF_Block* block;
			BF_Block_Init(&block);
			CALL_BF(BF_GetBlock(sindexDesc, i, block));
			char* data = BF_Block_GetData(block);
			char type;
			memcpy(&type, data, sizeof(char));

			// We don't care about 'H', 'M' or 'm' blocks
			if (type == 'H' || type == 'M' || type == 's') {
				CALL_BF(BF_UnpinBlock(block));
				BF_Block_Destroy(&block);
				continue;
			}

			// For each flagbit == 1, we print the corresponding Record
			for (int j=0 ; j<numberOfFlags ; ++j) {
				SecondaryRecord* record = malloc(sizeof(SecondaryRecord));
				if (record == NULL) {
					return HT_ERROR;
				}
				char flagValue;

				memcpy(&flagValue, data+1+2*sizeof(int) + j/8, sizeof(char));

				flagValue = flagValue << (j%8);
				flagValue = flagValue >> 7;
				if (flagValue == 0) {
					free(record);
					continue;
				}

				memcpy(record, data+1+2*sizeof(int)+numberOfBytesFlags + j*sizeof(SecondaryRecord), sizeof(SecondaryRecord));
				printf("SecondaryRecord with tupleId: %d and index_key: %s, in bucket: %d\n", record->tupleId, record->index_key, i);
				free(record);
			}

			CALL_BF(BF_UnpinBlock(block));
			BF_Block_Destroy(&block);
		}

		return HT_OK;
	}

	// Else we print the Records that have the same id with the given one

	int globalDepth = get_global_depth(sindexDesc);
	unsigned int key = hash_string(index_key);
	key = key >> (sizeof(int)*8 - globalDepth);
	if (globalDepth == 0) {
		key = 0;
	}
	int dataBlockPointer = find_hash_table_block(sindexDesc, key);

	BF_Block* block;
	BF_Block_Init(&block);

	int overflowBlock = 0;
	SecondaryRecord* record = malloc(sizeof(SecondaryRecord));

	// Checking about overflowBlocks (in README we explain why they exist)
	do {
		CALL_BF(BF_GetBlock(sindexDesc, dataBlockPointer, block));
		char* data = BF_Block_GetData(block);
		memcpy(&overflowBlock, data+1, sizeof(int));

		// For each flagbit == 1, we print the corresponding Record
		for (int i=0 ; i<numberOfFlags ; ++i) {
			unsigned char flagValue;
			memcpy(&flagValue,data+1+2*sizeof(int)+i/8,sizeof(char));

			flagValue = flagValue << i;
			flagValue = flagValue >>(sizeof(char)*8-1);

			if (flagValue == 1) {
				memcpy(record, data+1+2*sizeof(int)+numberOfBytesFlags + i*sizeof(SecondaryRecord), sizeof(SecondaryRecord));

				if (strcmp(record->index_key, index_key) == 0) {
					printf("SecondaryRecord with tupleId: %d and index_key: %s, in bucket: %d\n", record->tupleId, record->index_key, dataBlockPointer);
				}
			}
			
		}

		CALL_BF(BF_UnpinBlock(block));
		dataBlockPointer = overflowBlock;

	} while (overflowBlock != 0);

	free(record);
	BF_Block_Destroy(&block);

	return HT_OK;
}



HT_ErrorCode SHT_HashStatistics(char *filename ) {
	int sindexDesc;
	// We found the indexDesc of the file
	for (int i=0 ; i<SHT_openFilesCount ; ++i) {
		if (strcmp(SHT_openFiles[i]->name, filename) == 0) {
			sindexDesc = SHT_openFiles[i]->fd;
			break;
		}
	}

	// Printing the number of total blocks
	int numberBlocks;
	CALL_BF(BF_GetBlockCounter(sindexDesc, &numberBlocks));
	printf("The file: %s has %d blocks\n", filename, numberBlocks);	

	int numberOfStructs;
	BF_Block* mblock;
    BF_Block_Init(&mblock);
    char* data;

	// Iterate through all MBlocks
	int nextMBlock = 0;

	// For each MBlock of the file
	while (true) {
		CALL_BF(BF_GetBlock(sindexDesc, nextMBlock, mblock));
        data = BF_Block_GetData(mblock);

		int numberOfInts = nextMBlock == 0 ? 3 : 2;
		int numberOfChars = nextMBlock == 0 ? 22 : 1;
		numberOfStructs = (BF_BLOCK_SIZE - numberOfChars*sizeof(char) - numberOfInts*sizeof(int)) / sizeof(Statistics);

		int currentNumberOfStructs;
		memcpy(&currentNumberOfStructs, data+1+1*sizeof(int), sizeof(int));

		// For each Statistics of the MBlock
		for (int j=0 ; j<numberOfStructs && currentNumberOfStructs>0; ++j) {
			Statistics* stats = malloc(sizeof(Statistics));
			if (stats == NULL) {
				return HT_ERROR;
			}

			memcpy(stats, data+numberOfChars+numberOfInts*sizeof(int) + j*sizeof(Statistics), sizeof(Statistics));
			// We check that it's not empty and that it hasn't just been initialized
			if (stats->bucketID != 0 && stats->max != INT_MIN) {
				--currentNumberOfStructs;
				printf("Bucket id: %d has max: %d, min: %d, average: %f\n", stats->bucketID, stats->max, stats->min, stats->average);
			}
			free(stats);
		}

		memcpy(&nextMBlock, data+1, sizeof(int));
		CALL_BF(BF_UnpinBlock(mblock));

		// Break only if we get in the last mblock
		if (nextMBlock == 0) {
			break;
		}
	}
	BF_Block_Destroy(&mblock);

	return HT_OK;
}

HT_ErrorCode SHT_InnerJoin(int sindexDesc1, int sindexDesc2, char *index_key) {
	if (sindexDesc1 >= SHT_openFilesCount || sindexDesc2 >= SHT_openFilesCount) {
		return HT_ERROR;
	}

	// Just to see how many bytes of ευρετήριο we have
	int numberOfFlags = ((BF_BLOCK_SIZE - sizeof(char) - 2*sizeof(int)) * 8) / (sizeof(SecondaryRecord)*8 + 1);
	int numberOfBytesFlags = numberOfFlags % (sizeof(char)*8) == 0 ? numberOfFlags / (sizeof(char)*8) : numberOfFlags / (sizeof(char)*8) + 1;





	// TODO: Check the logic again
	if (index_key == NULL) {
		// Iterate through all different index_keys of one file (choose the one with less blocks)
		// For each index_key search the hash_key
		// 		If it exists, go to the block and print the corresponding things
		// 		If it doesn't exist, move on to the next index_key (it doesn't exist when find_hash_table_block() returns -1)

		int numberOfBlocks;
		CALL_BF(BF_GetBlockCounter(sindexDesc1, &numberOfBlocks));
		BF_Block* block;
		BF_Block_Init(&block);
		SecondaryRecord* record = malloc(sizeof(SecondaryRecord));
		if (record == NULL) {
			return HT_ERROR;
		}
		char prev_index_key[20] = '0';

		for (int i=0 ; i<numberOfBlocks ; ++i) {
			CALL_BF(BF_GetBlock(sindexDesc1, i, block));
			char* data = BF_Block_GetData(block);

			char type;
			memcpy(&type, data, sizeof(char));

			// We don't care about 'H', 'M' or 'm' blocks
			if (type == 'H' || type == 'M' || type == 's') {
				CALL_BF(BF_UnpinBlock(block));
				BF_Block_Destroy(&block);
				continue;
			}

			for (int j=0 ; j<numberOfFlags ; ++j) {
				unsigned char flagValue;
				memcpy(&flagValue, data+1+2*sizeof(int)+i/8, sizeof(char));
				bool flag = false;

				flagValue = flagValue << i;
				flagValue = flagValue >> (sizeof(char)*8 - 1);

				if (flagValue == 1) {
					memcpy(record, data+1+2*sizeof(int)+numberOfBytesFlags + i*sizeof(SecondaryRecord), sizeof(SecondaryRecord));

					if (strcmp(record->index_key, prev_index_key) != 0) {
						strcpy(prev_index_key, record->index_key);

						int globalDepth = get_global_depth(sindexDesc2);
						unsigned int key = hash_string(prev_index_key);
						key = key >> (sizeof(int)*8 - globalDepth);
						if (globalDepth == 0) {
							key = 0;
						}
						int dataBlockPointer = find_hash_table_block(sindexDesc1, key);

						if (dataBlockPointer == -1) {
							flag = false;
						}
						else {
							BF_Block* block2;
							BF_Block_Init(&block2);

							BF_GetBlock(sindexDesc2, dataBlockPointer, block2);
							char* data2 = BF_Block_GetData(block2);
							SecondaryRecord* record2 = malloc(sizeof(SecondaryRecord));
							if (record == NULL) {
								return HT_ERROR;
							}

							for (int k=0 ; k<numberOfFlags ; ++k) {
								unsigned char flagValue2;
								memcpy(&flagValue2, data+1+2*sizeof(int)+i/8, sizeof(char));
								flagValue2 = flagValue2 << i;
								flagValue2 = flagValue2 >> (sizeof(char)*8 - 1);

								if (flagValue2 == 1) {
									memcpy(record2, data+1+2*sizeof(int)+numberOfBytesFlags + i*sizeof(SecondaryRecord), sizeof(SecondaryRecord));

									if (strcmp(record2->index_key, prev_index_key) == 0) {
										flag = true;
										printf("tupleId: %d ", record->tupleId);
									}
								}
							}
							CALL_BF(BF_UnpinBlock(block2));
							BF_Block_Destroy(&block2);

						}
					}
					if (strcmp(record->index_key, prev_index_key) == 0 && flag) {
						printf("tupleId: %d ", record->tupleId);
					}
				}
			}

			CALL_BF(BF_UnpinBlock(block));
		}

		BF_Block_Destroy(&block);
		return HT_OK;
	}




	// Else we print for specific index_key

	int globalDepth1 = get_global_depth(sindexDesc1);
	unsigned int key1 = hash_string(index_key);
	key1 = key1 >> (sizeof(int)*8 - globalDepth1);
	if (globalDepth1 == 0) {
		key1 = 0;
	}
	int dataBlockPointer1 = find_hash_table_block(sindexDesc1, key1);

	int globalDepth2 = get_global_depth(sindexDesc2);
	unsigned int key2 = hash_string(index_key);
	key2 = key2 >> (sizeof(int)*8 - globalDepth2);
	if (globalDepth2 == 0) {
		key2 = 0;
	}
	int dataBlockPointer2 = find_hash_table_block(sindexDesc2, key2);

	BF_Block* block;
	BF_Block_Init(&block);

	SecondaryRecord* record = malloc(sizeof(SecondaryRecord));
	if (record == NULL) {
		return HT_ERROR;
	}

	printf("Inner_Join with index_key: %s\n", index_key);

	CALL_BF(BF_GetBlock(sindexDesc1, dataBlockPointer1, block));
	char* data = BF_Block_GetData(block);
	printf("File1: %s\n", SHT_openFiles[sindexDesc1]->name);
	for (int i=0 ; i<numberOfFlags ; ++i) {
		unsigned char flagValue;
		memcpy(&flagValue, data+1+2*sizeof(int)+i/8, sizeof(char));

		flagValue = flagValue << i;
		flagValue = flagValue >> (sizeof(char)*8 - 1);

		if (flagValue == 1) {
			memcpy(record, data+1+2*sizeof(int)+numberOfBytesFlags + i*sizeof(SecondaryRecord), sizeof(SecondaryRecord));

			if (strcmp(record->index_key, index_key) == 0) {
				printf("tupleId: %d ", record->tupleId);
			}
		}
	}
	printf("\n");
	CALL_BF(BF_UnpinBlock(block));

	CALL_BF(BF_GetBlock(sindexDesc2, dataBlockPointer2, block));
	char* data = BF_Block_GetData(block);
	printf("File2: %s\n", SHT_openFiles[sindexDesc2]->name);
	for (int i=0 ; i<numberOfFlags ; ++i) {
		unsigned char flagValue;
		memcpy(&flagValue, data+1+2*sizeof(int)+i/8, sizeof(char));

		flagValue = flagValue << i;
		flagValue = flagValue >> (sizeof(char)*8 - 1);

		if (flagValue == 1) {
			memcpy(record, data+1+2*sizeof(int)+numberOfBytesFlags + i*sizeof(SecondaryRecord), sizeof(SecondaryRecord));

			if (strcmp(record->index_key, index_key) == 0) {
				printf("tupleId: %d ", record->tupleId);
			}
		}
	}
	printf("\n");
	CALL_BF(BF_UnpinBlock(block));
	BF_Block_Destroy(block);

	return HT_OK;
}


// #endif // HASH_FILE_H
