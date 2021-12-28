#include "common.h"
// #include "blockFunctions.h"
#include "sht_file.h"
#include "sht_blockFunctions.h"
// #include "hash_file.h"

// from chatziko
uint hash_string(char* value) {
	// djb2 hash function, απλή, γρήγορη, και σε γενικές γραμμές αποδοτική
    uint hash = 5381;
    for (char* s = value; *s != '\0'; s++)
		hash = (hash << 5) + hash + *s;			// hash = (hash * 33) + *s. Το foo << 5 είναι γρηγορότερη εκδοχή του foo * 32.
    return hash;
}


HT_ErrorCode sht_insert_record(SecondaryRecord* record, int indexDesc, int blockIndex){
	//How many records each D-block can store
	int total_number_of_records=((BF_BLOCK_SIZE - sizeof(char) - 2*sizeof(int)) * 8) / (sizeof(SecondaryRecord)*8 + 1);

	//How many bytes are used to store flags
	int indexes_bytes=total_number_of_records % (sizeof(char)*8) == 0 ? total_number_of_records / (sizeof(char)*8) : total_number_of_records / (sizeof(char)*8) + 1;

	int overflowBlock = 0;
	// int currentId = -1;
	char* currentIndexKey = malloc(sizeof(char)*20+1); //* +1 for '\0'
	strcpy(currentIndexKey,"NONE");

	bool overflowFlag = true;
	bool first = false;

	BF_Block* nextBlock;
	BF_Block_Init(&nextBlock);

	SecondaryRecord*  rec = malloc( sizeof(SecondaryRecord) );
	int i;
	
	//do..while in case overflow block exists, as i explain in README
	do{		
		i=0;
		BF_GetBlock(openSHTFiles[indexDesc]->fd,blockIndex,nextBlock);
		char* c = BF_Block_GetData(nextBlock);

		for (i=0;i<total_number_of_records;i++) {
			unsigned char flagValue;

			// i/8 because with different data 'i' might not be 8,so if i is >8 we  access 2nd byte etc...
			memcpy(&flagValue,c+1+2*sizeof(int)+i/8,sizeof(char));

			flagValue = flagValue << (i - (i/8)*8);
			flagValue = flagValue >>(sizeof(char)*8-1);

			//if flase flag found, insert record there 
			if (flagValue == 0) {
				memcpy(c+1+ 2*sizeof(int)+ indexes_bytes+ i*sizeof(SecondaryRecord),record,sizeof(SecondaryRecord));

				CALL_HT(SHT_BlockHeaderUpdate(nextBlock,i,1));

				// CALL_HT(SHT_BucketStatsUpdate(indexDesc, blockIndex));

				CALL_BF(BF_UnpinBlock(nextBlock));
				BF_Block_Destroy(&nextBlock);


				free(rec);
				return HT_OK;

			}else{
				//if no space to insert the record is available, we must examine if all the current records have the same id
				//if the answer is yes, we must create an overflow block
				memcpy(rec,c+1+2*sizeof(int)+indexes_bytes+i*sizeof(SecondaryRecord),sizeof(SecondaryRecord));
				if((strcmp(rec->index_key,currentIndexKey) != 0 )&& (strcmp(currentIndexKey,"NONE") != 0) ){
					overflowFlag = false;
					// currentId = rec->index_key;
					strcpy(currentIndexKey,rec->index_key);
				}
				else if(first == false){
					// currentId = rec->index_key;
					strcpy(currentIndexKey,rec->index_key);
					first = true;
				}
			}
		}	

		memcpy(&overflowBlock,c+1,sizeof(int));
		if(overflowBlock != 0 ){
			blockIndex = overflowBlock;}
		CALL_BF(BF_UnpinBlock(nextBlock));

	}while(overflowBlock != 0);
	free(rec);
	
	overflowBlock =0;

	// In case overflow block is needed
	if((i == total_number_of_records) && (overflowFlag == true)){
		
		
		BF_Block* newBlock;
		BF_Block_Init(&newBlock);

		CALL_BF(BF_GetBlock(openSHTFiles[indexDesc]->fd,blockIndex,nextBlock));
		char* c = BF_Block_GetData(nextBlock);
		int local_depth;
		memcpy(&local_depth,c+1+sizeof(int),sizeof(int));

		//Allocate the new D-block
		CALL_BF(BF_AllocateBlock(openSHTFiles[indexDesc]->fd,newBlock));
		CALL_HT(BlockHeaderInit(newBlock,'D'));
		int blockCounter;
		CALL_BF(BF_GetBlockCounter(openSHTFiles[indexDesc]->fd,&blockCounter));
		blockCounter--;

		//connect the new D-block with the previous one
		memcpy(c+1,&blockCounter,sizeof(int));

		//call SHT_BucketStatsInit because new D-block created
		// CALL_HT(SHT_BucketStatsInit(openSHTFiles[indexDesc]->fd, blockCounter));
		
		BF_Block_SetDirty(nextBlock);
		CALL_BF(BF_UnpinBlock(nextBlock));

		char* newData = BF_Block_GetData(newBlock);
		memcpy(newData+1+2*sizeof(int)+indexes_bytes,record,sizeof(SecondaryRecord));
		memcpy(newData+1+sizeof(int),&local_depth,sizeof(int));

		//New record inserted, so update the position flag
		CALL_HT(SHT_BlockHeaderUpdate(newBlock,0,1));
		// CALL_HT(SHT_BucketStatsUpdate(indexDesc, blockCounter));

		CALL_BF(BF_UnpinBlock(newBlock));
		BF_Block_Destroy(&newBlock);
		CALL_BF(BF_UnpinBlock(nextBlock));
		BF_Block_Destroy(&nextBlock);

		return HT_OK;
	}
	BF_Block_Destroy(&nextBlock);
	return HT_ERROR;
}

HT_ErrorCode sht_arrange_buckets(const int indexDesc,int buddies_number,SecondaryRecord* record,unsigned int key){


	int tupleId;

	BF_Block* newBucket;
	BF_Block_Init(&newBucket);
	
	//create new bucket
	
	CALL_BF(BF_AllocateBlock(openSHTFiles[indexDesc]->fd,newBucket));

	//initilize it to type D
	CALL_HT(BlockHeaderInit(newBucket,'D'));	

	//store new bucket id
	int newBucketPosition;	
	
	CALL_BF(BF_GetBlockCounter(openSHTFiles[indexDesc]->fd,&newBucketPosition));
	newBucketPosition--;
	// CALL_HT(SHT_BucketStatsInit(indexDesc,newBucketPosition));	//prosthese to bucket se block typou M gia ta stats


	//Unpin block since its data is changed
	//setDirty is in BlockHeaderInit function
	CALL_BF(BF_UnpinBlock(newBucket));


	BF_Block* block; // eyretirio
	BF_Block_Init(&block);

	//  store which bucket, buddies points to
	int index_to_bucket= sht_find_hash_table_block(indexDesc,key);

	//Now we must find in which position of the block table, buddies start

	int counter=0;	//apo poy xekinane ta buddies
	int next_HT=1;

	
	int i;	
	
	// search in which position of the hash table 'buddies' start, 
	// so we can make half of buddies points to the new bucket
	//The use of do..while is essential since more than one hash table blocks can exist.
	do{
		CALL_BF(BF_GetBlock(openSHTFiles[indexDesc]->fd,next_HT,block));
		char* c = BF_Block_GetData(block);
		int currentNumberOfBuckets;
		memcpy(&currentNumberOfBuckets,c+1+2*sizeof(int),sizeof(int));
		
		bool flag = false;
		for(i=0;i<currentNumberOfBuckets;i++){
			int bucket_id;
			memcpy(&bucket_id,c+1+3*sizeof(int)+i*sizeof(int),sizeof(int));

			//if we find where buddies start
			if (bucket_id == index_to_bucket){
				flag = true;
				CALL_BF(BF_UnpinBlock(block));
				break;
			}
			counter++;
		}
		if(flag == true)
			break;
		
		memcpy(&next_HT,c+1,sizeof(int));
		CALL_BF(BF_UnpinBlock(block));
	}while(next_HT != 0);

	

	int half_buddies = buddies_number/2;
	next_HT=1;
	int j=0;

	//make half buddies points to the new bucket we created
	do{
		CALL_BF(BF_GetBlock(openSHTFiles[indexDesc]->fd,next_HT,block));
		char* c = BF_Block_GetData(block);
		int currentNumberOfBuckets;
		memcpy(&currentNumberOfBuckets,c+1+2*sizeof(int),sizeof(int));
		
		bool flag = false;

		for(int i=0;i<currentNumberOfBuckets;i++){
			if(j >= counter){
				int preBucket;
				memcpy(&preBucket,c+1+3*sizeof(int)+i*sizeof(int),sizeof(int));
				memcpy(c+1+3*sizeof(int)+i*sizeof(int),&newBucketPosition,sizeof(int));
				
				if(--half_buddies <= 0){
					flag = true;
					break;
				}
			}
			j++	;			
		}
		
		BF_Block_SetDirty(block);
		CALL_BF(BF_UnpinBlock(block));
		if(flag == true)
			break;
		memcpy(&next_HT,c+1,sizeof(int));
	}while(next_HT != 0);

	BF_Block_Destroy(&block);

	//Time to re-insert records from old bucket and the new one

	BF_Block* old_bucket;
	BF_Block_Init(&old_bucket);
	CALL_BF(BF_GetBlock(openSHTFiles[indexDesc]->fd,index_to_bucket,old_bucket));

	char* c = BF_Block_GetData(old_bucket);
	
	//after split, we increase local depth of the old bucket
	int local_deph = get_local_depth(old_bucket);
	local_deph++;
	memcpy(c+1+sizeof(int),&local_deph,sizeof(int));
	
	//since it's data is altered
	BF_Block_SetDirty(old_bucket);
	CALL_BF(BF_UnpinBlock(old_bucket));
	BF_Block_Destroy(&old_bucket);

	CALL_BF(BF_GetBlock(openSHTFiles[indexDesc]->fd,newBucketPosition,newBucket));

	// update local depth of the new bucket too
	char* newBucketData = BF_Block_GetData(newBucket);
	memcpy(newBucketData+1+sizeof(int), &local_deph,sizeof(int));
	
	BF_Block_SetDirty(newBucket);
	CALL_BF(BF_UnpinBlock(newBucket));
	BF_Block_Destroy(&newBucket);


	// How many records can type D block store
	int number_of_records=((BF_BLOCK_SIZE - sizeof(char) - 2*sizeof(int)) * 8) / (sizeof(SecondaryRecord)*8 + 1);

	//How many bytes of a D-block are used to store flags
	int index_bytes=number_of_records % (sizeof(char)*8) == 0 ? number_of_records / (sizeof(char)*8) : number_of_records / (sizeof(char)*8) + 1;
	
	int overflowBlock = 0;

	int startingBlock=index_to_bucket;
	
	int updateCounter = 0;

	//In the below part is the re-insertion of each record + the new one
	do{ 
		SecondaryRecord*  old_records = malloc(sizeof(SecondaryRecord));
	
		for (int i=0;i<number_of_records;i++) {
			
			BF_Block_Init(&old_bucket);
			CALL_BF(BF_GetBlock(openSHTFiles[indexDesc]->fd,startingBlock,old_bucket));
			c = BF_Block_GetData(old_bucket);
			
			unsigned char flagValue;
			
			memcpy(&flagValue,c+1+2*sizeof(int)+i/8,sizeof(char));

			// Check if the flag is true or false
			flagValue = flagValue << (i%8);
			flagValue = flagValue >>7;

			// if flag is false there is no record in position 'i'
			if (flagValue == 0) {
				CALL_BF(BF_UnpinBlock(old_bucket));	
				BF_Block_Destroy(&old_bucket);	
				continue;
			}
			
			// remove record, set its flag to false and re-insert it
			memcpy(old_records,c+1+2*sizeof(int) + index_bytes+sizeof(SecondaryRecord)*i,sizeof(SecondaryRecord));
			CALL_HT(SHT_BlockHeaderUpdate(old_bucket,i,0));
			CALL_BF(BF_UnpinBlock(old_bucket));
			BF_Block_Destroy(&old_bucket);
			

			// CALL_HT(SHT_BucketStatsUpdate(indexDesc,index_to_bucket));	//gia na enimerothoun ta stats, afoy 'bgike' mia eggrafi
			CALL_HT(SHT_SecondaryInsertEntry(indexDesc,*old_records));

		}

		// Check if the block has an overflow block
		BF_Block_Init(&old_bucket);
		CALL_BF(BF_GetBlock(openSHTFiles[indexDesc]->fd,startingBlock,old_bucket));
		c = BF_Block_GetData(old_bucket);	

		memcpy(&overflowBlock,c+1,sizeof(int));

		CALL_BF(BF_UnpinBlock(old_bucket));
		BF_Block_Destroy(&old_bucket);

		free(old_records);

		startingBlock = overflowBlock;
	}while(overflowBlock != 0);

	if (SHT_SecondaryInsertEntry(indexDesc,*record) == HT_OK ){
		return HT_OK;
	}

	return HT_ERROR;
}


HT_ErrorCode sht_extend_hash_table(int indexDesc){
	int maxNumberOfBuckets = (BF_BLOCK_SIZE - 1 - 3*sizeof(int))/sizeof(int); // poios einai o megistos arithmos apo buckets pou xoraei kathe hash table
	int currentNumberOfBuckets;	// poios einai o arithmos apo buckets pou exei to hash table
	int newHT=1;	// poio block typou Hash table einai meta. An einai 0 simainei oti den yparxei allo

	BF_Block* block;
	BF_Block_Init(&block);
	char* c;
	int global_depth;
	int newIndexes;
	
	CALL_BF(BF_GetBlock(openSHTFiles[indexDesc]->fd,1,block));
	c= BF_Block_GetData(block);


	global_depth = get_global_depth(openSHTFiles[indexDesc]->fd);
	if(global_depth == -1){
		return HT_ERROR;
	}
	
	newIndexes = power_of_two(global_depth);
	
	// ston pinaka auton tha apothikeyso prosorina ta ids pou exei apothikeysei to hash table mexri tora
	int* indexArray = malloc(sizeof(int)*2*newIndexes);
	
	//antigrafo tous palious deiktes stin katallili thesi tou pinaka
	int arrayCounter = 0;
	do{	

		CALL_BF(BF_GetBlock(openSHTFiles[indexDesc]->fd,newHT,block));
		c = BF_Block_GetData(block);
		memcpy(&currentNumberOfBuckets, c+1+2*sizeof(int),sizeof(int));
		
		
		for(int i=0; i<currentNumberOfBuckets; i++){
			int blockIndex;
			memcpy(&blockIndex,c+1+3*sizeof(int)+ i*sizeof(int),sizeof(int));
			indexArray[2*(arrayCounter)] = blockIndex;
			indexArray[2*(arrayCounter)+1] = blockIndex;
			arrayCounter++; 
		}
		// pairno ton "pointer" stin epomeni domi typou hash table, an yparxei kai xanakano to idio
		memcpy(&newHT,c+1,sizeof(int));	
		CALL_BF(BF_UnpinBlock(block));

	}while(newHT != 0 );


	arrayCounter*=2;
	

	int j=0;
	newHT = 1;
	global_depth++;

	//antigrafo apo ton pinaka piso sto hash table, apla epeidi egine extend 2 diadoxikes theseis tha deixnoyn ston idio bucket 
	do{
		
		CALL_BF(BF_GetBlock(openSHTFiles[indexDesc]->fd,newHT,block));
		c = BF_Block_GetData(block);
		memcpy(c+1+sizeof(int),&global_depth,sizeof(int));
		
		
		int i;
		for( i=0; i<maxNumberOfBuckets;i++){
			if( j == arrayCounter){
				break;
			}

			memcpy(c+1+3*sizeof(int)+sizeof(int)*i,&indexArray[j],sizeof(int));
			j++;
		}
		memcpy(c+1+sizeof(int)*2,&i,sizeof(int));
		memcpy(&newHT , c+1,sizeof(int));
		
		BF_Block_SetDirty(block);
		CALL_BF(BF_UnpinBlock(block));
		
		//an exoume akoma eggrafes na prosthesoume alla oxi diathesimo block gia to hash table, ftiaxnoume ena
		if((newHT==0) && (j< arrayCounter)){
			int blockCounter;
			BF_Block* newHashBlock;
			
			BF_Block_Init(&newHashBlock);
			CALL_BF(BF_AllocateBlock(openSHTFiles[indexDesc]->fd,newHashBlock));
			CALL_BF(BF_GetBlockCounter(openSHTFiles[indexDesc]->fd,&blockCounter));
			
			blockCounter--;
			CALL_HT(BlockHeaderInit(newHashBlock,'H'));



			memcpy(c+1,&(blockCounter),sizeof(int));	//bazo to palio hash table na deixnei to kainourgio
			// BF_Block_SetDirty(newHashBlock);
			CALL_BF(BF_UnpinBlock(newHashBlock));

			newHT = blockCounter;
			BF_Block_Destroy(&newHashBlock);
		}
	}while( j < arrayCounter );
	free(indexArray);
	BF_Block_Destroy(&block);

}