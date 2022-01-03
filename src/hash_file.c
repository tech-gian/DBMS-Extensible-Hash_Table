#include "bf.h"
#include "common.h"
#include "blockFunctions.h"





///////////////////////////////////
// Functions of the original project

HT_ErrorCode HT_Init() {
	openFilesCount = 0;
    for(int i = 0; i < MAX_OPEN_FILES; ++i){
        openFiles[i] = NULL;
    }
	return HT_OK;
}


HT_ErrorCode HT_CreateIndex(const char *filename, int depth) {
	BF_Block* currentBlock;
	BF_Block* currentHashBlock;
	BF_Block_Init(&currentBlock);
	BF_Block_Init(&currentHashBlock);
		
	int fileDescriptor;
	CALL_BF(BF_CreateFile(filename));
	CALL_BF(BF_OpenFile(filename, &fileDescriptor));


	// Create block 1(M block)
	CALL_BF(BF_AllocateBlock(fileDescriptor, currentBlock));
	if (BlockHeaderInit(currentBlock, 'm') != HT_OK) {
		CALL_BF(BF_UnpinBlock(currentBlock));
		BF_Block_Destroy(&currentBlock);
		return HT_ERROR;
	}
	CALL_BF(BF_UnpinBlock(currentBlock));
	BF_Block_Destroy(&currentBlock);

	// Create block 2 (Hash Table)
	CALL_BF(BF_AllocateBlock(fileDescriptor, currentHashBlock));
	if (BlockHeaderInit(currentHashBlock, 'H') != HT_OK) {
		CALL_BF(BF_UnpinBlock(currentHashBlock));
		BF_Block_Destroy(&currentHashBlock);
		return HT_ERROR;
	}

	char* hashBlockData = BF_Block_GetData(currentHashBlock);
	// Change globalDepth
	memcpy(hashBlockData + 1 + sizeof(int), &depth, sizeof(int));


	int totalBuckets = power_of_two(depth);
	int maxNumberOfBucketsPerHashBlock = (BF_BLOCK_SIZE - 1 - 3*sizeof(int)) / sizeof(int);

	CALL_BF(BF_UnpinBlock(currentHashBlock));

	int nextHB=1;

	// Loop until all hash blocks have been created
	do {
		BF_Block* newBucket;
		BF_Block_Init(&newBucket);

		//!
		CALL_BF(BF_GetBlock(fileDescriptor, nextHB,currentHashBlock));
		hashBlockData = BF_Block_GetData(currentHashBlock);
		//!

		int bucketCounter = 0;
		int blockCounter = 0;
		for (bucketCounter=0 ; bucketCounter<maxNumberOfBucketsPerHashBlock ; ++bucketCounter) {
			// Loop until all buckets have been created (total_buckets reaches 0)
			// Or until the current hash block gets filled with
			// the maximum number of buckets that it can save (bucket_counter reaches maxNoOfBucketsPerHashBlock)
			if (!totalBuckets) {
				break;
			}
			
			// Allocate and initialize the new bucket block
			CALL_BF(BF_AllocateBlock(fileDescriptor, newBucket));
			if (BlockHeaderInit(newBucket, 'D') != HT_OK) {	
				return HT_ERROR;
			}
			
			// Write to it the local depth
			char* bucketData = BF_Block_GetData(newBucket);
			memcpy(bucketData + 1+sizeof(int), &depth, sizeof(int));

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
			if (BlockHeaderInit(newHashBlock, 'H') != HT_OK) {
				CALL_BF(BF_UnpinBlock(newHashBlock));
				BF_Block_Destroy(&newHashBlock);
				return HT_ERROR;
			}
			
			// Save the number of the new hash block to the previous hash block
			CALL_BF(BF_GetBlockCounter(fileDescriptor, &blockCounter));
			--blockCounter;

			memcpy(hashBlockData + 1, &blockCounter, sizeof(int));

			// Set the previous hash block to dirty and unpin it
			BF_Block_SetDirty(currentHashBlock);
			CALL_BF(BF_UnpinBlock(currentHashBlock));
			
			//!
			nextHB = blockCounter;
			//!

			// Change globalDepth to newHashBlock
			char* newHashBlockData = BF_Block_GetData(newHashBlock);
			memcpy(newHashBlockData + 1 + sizeof(int), &depth, sizeof(int));


			//!
			CALL_BF(BF_UnpinBlock(newHashBlock));
			BF_Block_Destroy(&newHashBlock);
			//!

		}
		else {
			// Set the current hash block to dirty and unpin it
			BF_Block_SetDirty(currentHashBlock);
			CALL_BF(BF_UnpinBlock(currentHashBlock));
		}
	} while(totalBuckets > 0);

	BF_Block_Destroy(&currentHashBlock);
	CALL_BF(BF_CloseFile(fileDescriptor));

	return HT_OK;
}


HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc) {
	
	// Check if no other file can be opened
	if (openFilesCount >= MAX_OPEN_FILES) {
		return HT_ERROR;
	}

	// Check if file is already open
	for (int i = 0; i < MAX_OPEN_FILES; ++i) {
		if (openFiles[i] != NULL && !strcmp(openFiles[i]->name, fileName)) {
			return HT_ERROR;
		}
	}

    int destIndex = -1;
    for (int i = 0; i < MAX_OPEN_FILES; ++i) {
		if (openFiles[i] == NULL) {
			destIndex = i;
            break;
		}
	}
    if(destIndex == -1)
        return HT_ERROR;

	int fileDescriptor;
	CALL_BF(BF_OpenFile(fileName, &fileDescriptor));

	struct openFile* currentFile = malloc(sizeof(struct openFile));
	if (currentFile == NULL) {
		return HT_ERROR;
	}

	currentFile->name = malloc(strlen(fileName) + 1);
	if (currentFile->name == NULL) {
		return HT_ERROR;
	}

	openFiles[destIndex] = currentFile;

	strcpy(openFiles[destIndex]->name, fileName);
	openFiles[destIndex]->fd = fileDescriptor;
	++openFilesCount;
	*indexDesc = fileDescriptor;
	
	return HT_OK;
}


HT_ErrorCode HT_CloseFile(int indexDesc) {
	if (indexDesc >= MAX_OPEN_FILES) {
		return HT_ERROR;
	}
	CALL_BF(BF_CloseFile(openFiles[indexDesc]->fd));

	if (openFiles[indexDesc]->name != NULL) {
		free(openFiles[indexDesc]->name);
	}
	free(openFiles[indexDesc]);
    openFiles[indexDesc] = NULL;
	--openFilesCount;

	return HT_OK;
}


HT_ErrorCode HT_InsertEntry(int indexDesc, Record record,int* tupleId,UpdateRecordArray* updateArray) {
	unsigned int key = hash_id(record.id);	//hash record id

	int gdepth = get_global_depth(openFiles[indexDesc]->fd);

	//Pick the proper number of bits. For example if gd=2 and key is 32 bits, keep the first 2 
	key = key >> (sizeof(int)*8-gdepth);

	//Just in case gdepth is 0
	if(gdepth == 0){
		key = 0;
	}

	//find in which block, new record will be inserted
	int block_index = find_hash_table_block(indexDesc ,key);

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
	if (insert_record(&record, indexDesc , block_index, tupleId) == HT_OK) {
		return HT_OK;
	}
	else {	// if no free space is available
		
		if (ldepth < gdepth){	//1st case
			int buddies_number= power_of_two(gdepth)/power_of_two(ldepth);
			arrange_buckets(indexDesc,buddies_number,&record,key,updateArray,tupleId);		//split the bucket
																		// increase bucket's depth
																		// re-insert record
		}
		else{	//aextend hash table and try to re-insert the record
			extend_hash_table(indexDesc);
			return HT_InsertEntry(indexDesc,record, tupleId,updateArray);
		}
	}
	
  return HT_OK;
}


HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
	if (indexDesc >= openFilesCount) {
		return HT_ERROR;
	}

	int numberBlocks;
	CALL_BF(BF_GetBlockCounter(openFiles[indexDesc]->fd, &numberBlocks));

	// Just to see how many bytes of ευρετήριο we have
	int numberOfFlags = ((BF_BLOCK_SIZE - sizeof(char) - 2*sizeof(int)) * 8) / (sizeof(Record)*8 + 1);
	int numberOfBytesFlags = numberOfFlags % (sizeof(char)*8) == 0 ? numberOfFlags / (sizeof(char)*8) : numberOfFlags / (sizeof(char)*8) + 1;
	
	// If id is NULL, then we print all the Records of the file
	if (id == NULL) {
		for (int i=0 ; i<numberBlocks ; ++i) {
			BF_Block* block;
			BF_Block_Init(&block);
			CALL_BF(BF_GetBlock(openFiles[indexDesc]->fd, i, block));
			char* data = BF_Block_GetData(block);
			char type;
			memcpy(&type, data, sizeof(char));

			// We don't care about 'H', 'M' or 'm' blocks
			if (type == 'H' || type == 'M' || type == 'm') {
				CALL_BF(BF_UnpinBlock(block));
				BF_Block_Destroy(&block);
				continue;
			}

			// For each flagbit == 1, we print the corresponding Record
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
				int tupleId = ((i+1)*numberOfFlags + j);
				memcpy(record, data+1+2*sizeof(int)+numberOfBytesFlags + j*sizeof(Record), sizeof(Record));
				printf("Record with id: %d, name: %s, surname: %s and city: %s in bucket: %d with tupleId %d\n", record->id, record->name, record->surname, record->city, i,tupleId);
				free(record);
			}

			CALL_BF(BF_UnpinBlock(block));
			BF_Block_Destroy(&block);
		}

		return HT_OK;
	}

	// Else we print the Records that have the same id with the given one

	int globalDepth = get_global_depth(openFiles[indexDesc]->fd);
	unsigned int key = hash_id(*id);
	key = key >> (sizeof(int)*8 - globalDepth);
	if (globalDepth == 0) {
		key = 0;
	}
	int dataBlockPointer = find_hash_table_block(indexDesc, key);

	BF_Block* block;
	BF_Block_Init(&block);

	int overflowBlock = 0;
	Record* record = malloc(sizeof(Record));

	// Checking about overflowBlocks (in README we explain why they exist)
	do {
		CALL_BF(BF_GetBlock(openFiles[indexDesc]->fd, dataBlockPointer, block));
		char* data = BF_Block_GetData(block);
		memcpy(&overflowBlock, data+1, sizeof(int));

		// For each flagbit == 1, we print the corresponding Record
		for (int i=0 ; i<numberOfFlags ; ++i) {
			unsigned char flagValue;
			memcpy(&flagValue,data+1+2*sizeof(int)+i/8,sizeof(char));

			flagValue = flagValue << i;
			flagValue = flagValue >>(sizeof(char)*8-1);

			if (flagValue == 1) {
				memcpy(record, data+1+2*sizeof(int)+numberOfBytesFlags + i*sizeof(Record), sizeof(Record));

				if (record->id == *id) {
					printf("Record with id: %d, name: %s, surname: %s and city: %s in bucket: %d\n", record->id, record->name, record->surname, record->city, dataBlockPointer);
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


HT_ErrorCode HashStatistics(char* filename) {
	int indexDesc;
	// We found the indexDesc of the file
	for (int i=0 ; i<openFilesCount ; ++i) {
		if (strcmp(openFiles[i]->name, filename) == 0) {
			indexDesc = i;
			break;
		}
	}

	// Printing the number of total blocks
	int numberBlocks;
	CALL_BF(BF_GetBlockCounter(openFiles[indexDesc]->fd, &numberBlocks));
	printf("The file: %s has %d blocks\n", filename, numberBlocks);	

	int numberOfStructs;
	BF_Block* mblock;
    BF_Block_Init(&mblock);
    char* data;

	// Iterate through all MBlocks
	int nextMBlock = 0;

	// For each MBlock of the file
	while (true) {
		CALL_BF(BF_GetBlock(openFiles[indexDesc]->fd, nextMBlock, mblock));
        data = BF_Block_GetData(mblock);

		int numberOfInts = nextMBlock == 0 ? 3 : 2;
		numberOfStructs = (BF_BLOCK_SIZE - sizeof(char) - numberOfInts*sizeof(int)) / sizeof(Statistics);

		int currentNumberOfStructs;
		memcpy(&currentNumberOfStructs, data+1+1*sizeof(int), sizeof(int));

		// For each Statistics of the MBlock
		for (int j=0 ; j<numberOfStructs && currentNumberOfStructs>0; ++j) {
			Statistics* stats = malloc(sizeof(Statistics));
			if (stats == NULL) {
				return HT_ERROR;
			}

			memcpy(stats, data+1+numberOfInts*sizeof(int) + j*sizeof(Statistics), sizeof(Statistics));
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
