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
    return HP_ERROR;        \
  }                         \
}


#ifndef HASH_FILE_H
#define HASH_FILE_H

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
    openSHTFilesCount = 0;
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
	for (int i = 0; i < openFilesCount; ++i) {
		if (!strcmp(openFiles[i]->name, fileName)) {
			primaryFD = openFiles[i]->fd;
            closePrimaryHT = 0;
            break;
		}
	}

    if(closePrimaryHT && HT_OpenIndex(fileName, &primaryFD) != HT_OK){
		return HT_ERROR;
	}

    // Create a new file
    int fileDescriptor;
    CALL_BF(BF_CreateFile(sfileName));
    CALL_BF(BF_OpenFile(sfileName, &fileDescriptor));

    BF_Block* block1;
    BF_Block_Init(&block1);

    // Create block 1 (s block)
    //TODO Save attrName to s block
	CALL_BF(BF_AllocateBlock(fileDescriptor, block1));
	if (BlockHeaderInit(block1, 's') != HT_OK) {
		return HT_ERROR;
	}
    //TODO Save attrName to s block

	CALL_BF(BF_UnpinBlock(block1));
    CALL_BF(BF_UnpinBlock(block1));
	BF_Block_Destroy(&block1);

    BF_Block* currentHashBlock;
    BF_Block_Init(&currentHashBlock);

    // Create block 2 (Hash Table)
	CALL_BF(BF_AllocateBlock(fileDescriptor, currentHashBlock));
	if (BlockHeaderInit(currentHashBlock, 'H') != HT_OK) {
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
			if (BlockHeaderInit(newBucket, 'D') != HT_OK) {	
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
			if (BlockHeaderInit(newHashBlock, 'H') != HT_OK) {
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

    //TODO: Add data from primary HT
    //TODO: Open this file that we have not yet created, so that SHT_SecondaryInsertEntry can insert records to it


    int primaryTotalBlocks = 0;
    CALLBF(BF_GetBlockCounter(primaryFD, &primaryTotalBlocks));

    // Just to see how many bytes of ευρετήριο we have
	int numberOfFlags = ((BF_BLOCK_SIZE - sizeof(char) - 2*sizeof(int)) * 8) / (sizeof(struct record)*8 + 1);
	int numberOfBytesFlags = numberOfFlags % (sizeof(char)*8) == 0 ? numberOfFlags / (sizeof(char)*8) : numberOfFlags / (sizeof(char)*8) + 1;

    for (int i = 0; i < primaryTotalBlocks ; ++i) {
        BF_Block* block;
        BF_Block_Init(&block);
        CALL_BF(BF_GetBlock(indexDesc, i, block));
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

            //TODO Open this file and get its index
            SHT_SecondaryInsertEntry(/*TODO index*/, *sr);
            free(sr);
            free(record);
        }

        CALL_BF(BF_UnpinBlock(block));
        BF_Block_Destroy(&block);
    }
    



	BF_Block_Destroy(&currentHashBlock);
	CALL_BF(BF_CloseFile(fileDescriptor));
    if(closePrimaryHT)
        CALL_BF(BF_CloseFile(primaryFD));
    return HT_OK;
}

HT_ErrorCode SHT_OpenSecondaryIndex(const char *sfileName, int *indexDesc  ) {
    //TODO open primary file

    // Check if no other file can be opened
	if (openSHTFilesCount >= MAX_OPEN_FILES) {
		return HT_ERROR;
	}

	// Check if file is already open
	for (int i=0 ; i<openSHTFilesCount ; ++i) {
		if (!strcmp(openSHTFiles[i]->name, sfileName)) {
			return HT_ERROR;
		}
	}

	int sFileDescriptor;
	CALL_BF(BF_OpenFile(filesfileNameName, &sFileDescriptor));

	struct openFile* currentFile = malloc(sizeof(struct openFile));
	if (currentFile == NULL) {
		return HT_ERROR;
	}

	currentFile->name = malloc(strlen(sfileName) + 1);
	if (currentFile->name == NULL) {
		return HT_ERROR;
	}

	openSHTFiles[openSHTFilesCount] = currentFile;

	strcpy(openSHTFiles[openSHTFilesCount]->name, sfileName);
	openSHTFiles[openSHTFilesCount]->fd = sFileDescriptor;
	++openSHTFilesCount;
	*indexDesc = sFileDescriptor;
	
	return HT_OK;
}

HT_ErrorCode SHT_CloseSecondaryIndex(int indexDesc) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode SHT_SecondaryInsertEntry (int indexDesc,SecondaryRecord record  ) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode SHT_SecondaryUpdateEntry (int indexDesc, UpdateRecordArray *updateArray ) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode SHT_PrintAllEntries(int sindexDesc, char *index-key ) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode SHT_HashStatistics(char *filename ) {
  //insert code here
  return HT_OK;
}

HT_ErrorCode SHT_InnerJoin(int sindexDesc1, int sindexDesc2,  char *index-key ) {
  //insert code here
  return HT_OK;
}


#endif // HASH_FILE_H
