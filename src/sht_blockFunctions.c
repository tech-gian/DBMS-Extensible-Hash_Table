#include "sht_blockFunctions.h"


// TODO: CHECK all files. Where you use indexDesc (position in the array) instead of the FileDesc returned from BF_CreateFile(), CHANGE IT



HT_ErrorCode SHT_BlockHeaderInit(BF_Block* block, char type, char* attrName, char* primaryFilename) {
	char* data = BF_Block_GetData(block);

	// Depending on the type of the Block,
	// we initialize some values for each one
	switch (type) {
	case 'D':
		memcpy(data, &type, sizeof(char));
		int ld = 0;
		int next = 0;
		int temp = 0;
		memcpy(data+1, &temp, sizeof(int));
		memcpy(data+1 + 1*sizeof(int), &ld, sizeof(int));
		int numberOfFlags = ((BF_BLOCK_SIZE - sizeof(char) - 2*sizeof(int)) * 8) / (sizeof(SecondaryRecord)*8 + 1);
		int numberOfBytesFlags = numberOfFlags % (sizeof(char)*8) == 0 ? numberOfFlags / (sizeof(char)*8) : numberOfFlags / (sizeof(char)*8) + 1;
		for (int i=0 ; i<numberOfBytesFlags ; ++i) {
			int temp = 0;
			memcpy(data+1+2*sizeof(int)+i, &temp, sizeof(char));
		}
		break;
	case 'H':
		memcpy(data, &type, sizeof(char));
		for (int i=0 ; i<3 ; ++i) {
			int temp = 0;
			memcpy(data+1 + i*sizeof(int), &temp, sizeof(int));
		}
		break;
	case 'M':
		memcpy(data, &type, sizeof(char));
		for (int i=0 ; i<2 ; ++i) {
			int temp = 0;
			memcpy(data+1 + i*sizeof(int), &temp, sizeof(int));
		}
		break;
	case 's':
		memcpy(data, &type, sizeof(char));
		for (int i=0 ; i<3 ; ++i) {
			int temp = 0;
			memcpy(data+1 + i*sizeof(int), &temp, sizeof(int));
		}
        // Add, 1 byte for hashKey
        if (strcmp(attrName, 'surname') == 0) {
            unsigned char temp = Surname;
            memcpy(data+1+3*sizeof(int), &temp, sizeof(char));
        }
        else if (strcmp(attrName, 'city') == 0) {
            unsigned char temp = City;
            memcpy(data+1+3*sizeof(int), &temp, sizeof(char));
        }
        else {
            return HT_ERROR;
        }

		if (strlen(primaryFilename) < 20) {
			memcpy(data+2+3*sizeof(int), primaryFilename, 20*sizeof(char));
		}
		else {
			memcpy(data+2+3*sizeof(int), primaryFilename, 19*sizeof(char));
			char lastChar = '\0';
			memcpy(data+2+3*sizeof(int)+19*sizeof(char), &lastChar, sizeof(char));
		}
        // TODO: Add this to documentation
		break;
	}
	BF_Block_SetDirty(block);

	return HT_OK;
}



HT_ErrorCode SHT_BlockHeaderUpdate(BF_Block* block, int flagPosition, char value) {
	// Changes only blocks of type 'D'
	char* data = BF_Block_GetData(block);
	int nByte = flagPosition / 8;

	// Changing the corresponding bits in flagBytes, based on the Records
	// (inserted or deleted)
	char flags;
	memcpy(&flags, data+1+2*sizeof(int)+nByte, sizeof(char));

	int nBit = flagPosition;
	if (value == 0) {
		value = ((char)1) << (7 - nBit);
		flags = flags & (~value);
	}
	else {
		value = value << (7 - nBit);
		flags = flags | value;
	}
	
	memcpy(data+1+2*sizeof(int)+nByte,&flags, sizeof(char));
	BF_Block_SetDirty(block);
	
	return HT_OK;
}



HT_ErrorCode BucketStatsInit(int indexDesc, int id) {
    int numberOfStructs;
	BF_Block* mblock;
    BF_Block_Init(&mblock);
	BF_Block* firstBlock;
	BF_Block_Init(&firstBlock);

	// Getting the last MBlock of the file
	CALL_BF(BF_GetBlock(indexDesc, 0, firstBlock));
	char* firstData = BF_Block_GetData(firstBlock);

	char* data;
	int lastMBlock;
	memcpy(&lastMBlock, firstData+1+2*sizeof(int), sizeof(int));

	if (lastMBlock != 0) {
		CALL_BF(BF_GetBlock(indexDesc, lastMBlock, mblock));
		data = BF_Block_GetData(mblock);
	}
	else {
		data = firstData;
	}

	// If new Statistics doesn't fit in current MBlock
	int oldSizeOfMBlock;
	memcpy(&oldSizeOfMBlock, data+1+1*sizeof(int), sizeof(int));
	BF_Block* newBlock;
	BF_Block_Init(&newBlock);

	numberOfStructs = lastMBlock == 0 ? (BF_BLOCK_SIZE - 22*sizeof(char) - 3*sizeof(int)) / sizeof(Statistics) : (BF_BLOCK_SIZE - sizeof(char) - 2*sizeof(int)) / sizeof(Statistics);

    if (oldSizeOfMBlock == numberOfStructs) {
        CALL_BF(BF_AllocateBlock(indexDesc, newBlock));
		
        int newBlockID;
        CALL_BF(BF_GetBlockCounter(indexDesc, &newBlockID));
		newBlockID--;
		memcpy(data+1, &newBlockID, sizeof(int));

		// Changing lastMblockPointer of the first 's' Block
		memcpy(firstData+1+2*sizeof(int), &newBlockID, sizeof(int));

        CALL_HT(BlockHeaderInit(newBlock, 'M'));
        data = BF_Block_GetData(newBlock);
    }

	int numberOfInts = data == firstData ? 3 : 2;
    int numberOfChars = data == firstData ? 22 : 1;
	numberOfStructs = (BF_BLOCK_SIZE - numberOfChars*sizeof(char) - numberOfInts*sizeof(int)) / sizeof(Statistics);
	

	// Initializing values of current Statistics
    Statistics* stats = malloc(sizeof(Statistics));
    if (stats == NULL) {
        return HT_ERROR;
    }

    stats->bucketID = id;
    stats->average = 0.0;
    stats->max = INT_MIN;
    stats->min = INT_MAX;
    stats->counter = 0;
    stats->sum = 0;
	
	int sizeOfMBlock;
	memcpy(&sizeOfMBlock, data+1+1*sizeof(int), sizeof(int));
	++sizeOfMBlock;
	memcpy(data+1+1*sizeof(int), &sizeOfMBlock, sizeof(int));
    memcpy(data+numberOfChars+numberOfInts*sizeof(int) + (sizeOfMBlock-1)*sizeof(Statistics), stats, sizeof(Statistics));


	if (oldSizeOfMBlock == numberOfStructs) {
		BF_Block_SetDirty(newBlock);
		CALL_BF(BF_UnpinBlock(newBlock));
	}
	if (lastMBlock != 0) {
		BF_Block_SetDirty(mblock);
		CALL_BF(BF_UnpinBlock(mblock));
	}
	BF_Block_SetDirty(firstBlock);
	CALL_BF(BF_UnpinBlock(firstBlock));
	BF_Block_Destroy(&firstBlock);
	BF_Block_Destroy(&newBlock);
	BF_Block_Destroy(&mblock);
	free(stats);

	return HT_OK;
}




HT_ErrorCode SHT_BucketStatsUpdate(int indexDesc, int id) {
	int numberOfStructs;

	BF_Block* block;
	BF_Block_Init(&block);
	int mblock = 0;
	char* data;
	int positionDblock = -1;
	Statistics* stats;

	// Searching for the MBlock with this id
	while (true) {
		CALL_BF(BF_GetBlock(indexDesc, mblock, block));
		data = BF_Block_GetData(block);
		stats = malloc(sizeof(Statistics));
		if (stats == NULL) {
			return HT_ERROR;
		}

		int numberOfInts = mblock == 0 ? 3 : 2;
        int numberOfChars = mblock == 0 ? 22 : 1;
		numberOfStructs = (BF_BLOCK_SIZE - numberOfChars*sizeof(char) - numberOfInts*sizeof(int)) / sizeof(Statistics);

		// Searching each Statistics of each MBlock to find given id
		for (int i=0 ; i<numberOfStructs ; ++i) {
			memcpy(stats, data+numberOfChars+numberOfInts*sizeof(int) + i*sizeof(Statistics), sizeof(Statistics));

			if (stats->bucketID == id) {
				positionDblock = i;
				break;
			}
		}

		if (positionDblock != -1) {
			break;
		}

		free(stats);
		memcpy(&mblock, data+1, sizeof(int));
		CALL_BF(BF_UnpinBlock(block));

		if (mblock == 0) {
			return HT_ERROR;
		}

	}

	// Getting DBlock's data (containing given id)
	BF_Block* dblock;
	BF_Block_Init(&dblock);
	CALL_BF(BF_GetBlock(indexDesc, id, dblock));
	int numberOfRecords = count_flags(dblock, true);
	CALL_BF(BF_UnpinBlock(dblock));
	BF_Block_Destroy(&dblock);


	// Changing the HashStatistics values of this struct
	stats->counter++;
	stats->sum += numberOfRecords;
	stats->average = (float)stats->sum / (float)stats->counter;
	stats->bucketID = id;
	if (numberOfRecords > stats->max) {
		stats->max = numberOfRecords;
	}
	if (numberOfRecords < stats->min) {
		stats->min = numberOfRecords;
	}

	int numberOfInts = mblock == 0 ? 3 : 2;
    int numberOfChars = mblock == 0 ? 22 : 1;

	memcpy(data+ numberOfChars + numberOfInts*sizeof(int) + positionDblock*sizeof(Statistics), stats, sizeof(Statistics));
	BF_Block_SetDirty(block);
	CALL_BF(BF_UnpinBlock(block));
	BF_Block_Destroy(&block);
	free(stats);

	return HT_OK;
}


