#include "common.h"
#include "blockFunctions.h"
#include "sht_file.h"


int power_of_two(int power){
    int result=2;
	if(power == 0)
		return 1;
	while(--power){
		result*=2;
	}
	return result;
}

// online source
int hash_id(unsigned int id){
	id = ((id >> 16) ^ id) * 0x45d9f3b;
    id = ((id >> 16) ^ id) * 0x45d9f3b;
    id = (id >> 16) ^ id;
    return id;
}


int count_flags(BF_Block* block, bool sht) {
	int numberOfFlags;
	if (sht == true) {
		numberOfFlags = ((BF_BLOCK_SIZE - sizeof(char) - 2*sizeof(int)) * 8) / (sizeof(SecondaryRecord)*8 + 1);
	}
	else {
		numberOfFlags = ((BF_BLOCK_SIZE - sizeof(char) - 2*sizeof(int)) * 8) / (sizeof(Record)*8 + 1);
	}
	int numberOfBytesFlags = numberOfFlags % (sizeof(char)*8) == 0 ? numberOfFlags / (sizeof(char)*8) : numberOfFlags / (sizeof(char)*8) + 1;
	int counter = 0;

	// Iteration of all the flagBytes and checking how many
	// flags (1 bits) exist
	char* data = BF_Block_GetData(block);
	for (int i=0 ; i<numberOfBytesFlags ; ++i) {
		unsigned char flagByte;

		for (int j=0 ; j<8 ; ++j) {
			memcpy(&flagByte, data+1+2*sizeof(int) + i, sizeof(char));
			flagByte = flagByte << j;
			flagByte = flagByte >> 7;
			if (flagByte == 1) {
				++counter;
			}
		}
	}
	CALL_BF(BF_UnpinBlock(block));

	return counter;
}




// Functions for InsertEntry

//pairnei to kleidi apo tin hash function kai epistrefei to id tou bucket pou tha mpei i eggrafi
// px an to key = 0100, tha epistrafei to 5o bucket

int find_hash_table_block(int indexDesc,unsigned int key){

	int currentNumberOfBuckets;	// deixnei poios einai o arithmos twn buckets sto yparxon hash table block
	
	BF_Block* block;
	BF_Block_Init(&block);
	
	// BF_GetBlock(open_files[indexDesc],1,block);	//to proto block typou H einai to block me id=1

	int nextHtBlock=1;	//to proto HT block id
	int positionIndex=0;
	
	//an exoume pano apo ena HT epeidi den xorane ola ta buckets se ena mono HT
	do {
		CALL_BF (BF_GetBlock(openFiles[indexDesc]->fd,nextHtBlock,block));	// pare to block me id = nextHtBlock
		
		char* c = BF_Block_GetData(block);
		memcpy(&currentNumberOfBuckets,c+1+sizeof(int)*2,sizeof(int));	//grapse stin metabliti current number of buckets, posous koybades exei to paron HT
		int index_to_bucket;
		memcpy(&nextHtBlock,c+1,sizeof(int));	//grapse stin metabliti current block, poio einai to epomeno block typou H

		
		for(int i=0;i<currentNumberOfBuckets;i++){

			if( positionIndex == key){	
				memcpy(&index_to_bucket,c+1+sizeof(int)*3+i*sizeof(int),sizeof(int));
				CALL_BF(BF_UnpinBlock(block));
				BF_Block_Destroy(&block);
				return index_to_bucket;
			}
			positionIndex++;
			
		}

		CALL_BF(BF_UnpinBlock(block));
	} while(nextHtBlock != 0);
	BF_Block_Destroy(&block);
	return -1;	//se periptosi lathous pou den yparxei tetoio key
}

int sht_find_hash_table_block(int indexDesc,unsigned int key){

	int currentNumberOfBuckets;	// deixnei poios einai o arithmos twn buckets sto yparxon hash table block
	
	BF_Block* block;
	BF_Block_Init(&block);
	
	// BF_GetBlock(open_files[indexDesc],1,block);	//to proto block typou H einai to block me id=1

	int nextHtBlock=1;	//to proto HT block id
	int positionIndex=0;
	
	//an exoume pano apo ena HT epeidi den xorane ola ta buckets se ena mono HT
	do {
		CALL_BF (BF_GetBlock(openSHTFiles[indexDesc]->fd,nextHtBlock,block));	// pare to block me id = nextHtBlock
		
		char* c = BF_Block_GetData(block);
		memcpy(&currentNumberOfBuckets,c+1+sizeof(int)*2,sizeof(int));	//grapse stin metabliti current number of buckets, posous koybades exei to paron HT
		int index_to_bucket;
		memcpy(&nextHtBlock,c+1,sizeof(int));	//grapse stin metabliti current block, poio einai to epomeno block typou H

		
		for(int i=0;i<currentNumberOfBuckets;i++){

			if( positionIndex == key){	
				memcpy(&index_to_bucket,c+1+sizeof(int)*3+i*sizeof(int),sizeof(int));
				CALL_BF(BF_UnpinBlock(block));
				BF_Block_Destroy(&block);
				return index_to_bucket;
			}
			positionIndex++;
			
		}

		CALL_BF(BF_UnpinBlock(block));
	} while(nextHtBlock != 0);
	BF_Block_Destroy(&block);
	return -1;	//se periptosi lathous pou den yparxei tetoio key
}

int get_global_depth(int fileDesc){
	BF_Block* block;
	BF_Block_Init(&block);
	
	//to global depth to pairno apo to block me id=1
	CALL_BF(BF_GetBlock(fileDesc, 1, block));
	char* c= BF_Block_GetData(block);
	
	char type;
	memcpy(&type,c,sizeof(char));
	
	//Se periptosi pou to block den einai typou 'H'
	if(type != 'H'){
		return -1;
	}


	
	
	int gd;
	memcpy(&gd,c+1+sizeof(int),sizeof(int));
	
	CALL_BF(BF_UnpinBlock(block));	//to kano unpin, den to xreiazomai allo
	BF_Block_Destroy(&block);
	
	return gd;
}


//epistrefei to local depth tou block block
int get_local_depth(BF_Block* block){

	char* c= BF_Block_GetData(block);
	char type;
	memcpy(&type,c,sizeof(char));
	
	//Se periptosi pou to block den einai typou 'D'
	if(type != 'D'){
		return -1;
	}
	
	int ld;
	memcpy(&ld,c+1+sizeof(int),sizeof(int));
	return ld;
}



HT_ErrorCode insert_record(Record* record, int indexDesc, int blockIndex, int* tupleId){
	//How many records each D-block can store
	int total_number_of_records=((BF_BLOCK_SIZE - sizeof(char) - 2*sizeof(int)) * 8) / (sizeof(Record)*8 + 1);

	//How many bytes are used to store flags
	int indexes_bytes=total_number_of_records % (sizeof(char)*8) == 0 ? total_number_of_records / (sizeof(char)*8) : total_number_of_records / (sizeof(char)*8) + 1;

	int overflowBlock = 0;
	int currentId = -1;

	bool overflowFlag = true;
	bool first = false;

	BF_Block* nextBlock;
	BF_Block_Init(&nextBlock);

	Record* rec = malloc( sizeof(Record) );
	int i;

	//do..while in case overflow block exists, as i explain in README
	do{		
		i=0;
		BF_GetBlock(openFiles[indexDesc]->fd,blockIndex,nextBlock);
		char* c = BF_Block_GetData(nextBlock);

		for (i=0;i<total_number_of_records;i++) {
			unsigned char flagValue;

			// i/8 because with different data 'i' might not be 8,so if i is >8 we  access 2nd byte etc...
			memcpy(&flagValue,c+1+2*sizeof(int)+i/8,sizeof(char));

			flagValue = flagValue << (i - (i/8)*8);
			flagValue = flagValue >>(sizeof(char)*8-1);

			//if flase flag found, insert record there 
			if (flagValue == 0) {
				memcpy(c+1+ 2*sizeof(int)+ indexes_bytes+ i*sizeof(Record),record,sizeof(Record));
				CALL_HT(BlockHeaderUpdate(nextBlock,i,1));

				CALL_HT(BucketStatsUpdate(indexDesc, blockIndex));

				CALL_BF(BF_UnpinBlock(nextBlock));
				BF_Block_Destroy(&nextBlock);

				free(rec);
				*tupleId = ((blockIndex+1)*total_number_of_records + i);
				return HT_OK;

			}else{
				//if no space to insert the record is available, we must examine if all the current records have the same id
				//if the answer is yes, we must create an overflow block
				memcpy(rec,c+1+2*sizeof(int)+indexes_bytes+i*sizeof(Record),sizeof(Record));
				if((rec->id != currentId) && (currentId != -1)){
					overflowFlag = false;
					currentId = rec->id;
				}
				else if(first == false){
					currentId = rec->id;
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

		CALL_BF(BF_GetBlock(openFiles[indexDesc]->fd,blockIndex,nextBlock));
		char* c = BF_Block_GetData(nextBlock);
		int local_depth;
		memcpy(&local_depth,c+1+sizeof(int),sizeof(int));

		//Allocate the new D-block
		CALL_BF(BF_AllocateBlock(openFiles[indexDesc]->fd,newBlock));
		CALL_HT(BlockHeaderInit(newBlock,'D'));
		int blockCounter;
		CALL_BF(BF_GetBlockCounter(openFiles[indexDesc]->fd,&blockCounter));
		blockCounter--;

		//connect the new D-block with the previous one
		memcpy(c+1,&blockCounter,sizeof(int));

		//call bucketStatsInit because new D-block created
		CALL_HT(BucketStatsInit(indexDesc, blockCounter));
		
		BF_Block_SetDirty(nextBlock);
		CALL_BF(BF_UnpinBlock(nextBlock));

		char* newData = BF_Block_GetData(newBlock);
		memcpy(newData+1+2*sizeof(int)+indexes_bytes,record,sizeof(Record));
		memcpy(newData+1+sizeof(int),&local_depth,sizeof(int));

		//New record inserted, so update the position flag
		CALL_HT(BlockHeaderUpdate(newBlock,0,1));
		CALL_HT(BucketStatsUpdate(indexDesc, blockCounter));

		CALL_BF(BF_UnpinBlock(newBlock));
		BF_Block_Destroy(&newBlock);
		CALL_BF(BF_UnpinBlock(nextBlock));
		BF_Block_Destroy(&nextBlock);
		//*
		*tupleId = (blockIndex+1)*total_number_of_records;
		//*
		return HT_OK;
	}
	BF_Block_Destroy(&nextBlock);
	return HT_ERROR;
}


// diaxorismos twn deiktwn se dyo isa yposynola me kathena na deixnei ena apo tous 2 kadous
HT_ErrorCode arrange_buckets(const int indexDesc,int buddies_number,Record* record,unsigned int key,UpdateRecordArray* updateArray, int* tupleId){

	// int tupleId;

	BF_Block* newBucket;
	BF_Block_Init(&newBucket);
	
	//create new bucket
	CALL_BF(BF_AllocateBlock(openFiles[indexDesc]->fd,newBucket));
	
	//initilize it to type D
	CALL_HT(BlockHeaderInit(newBucket,'D'));	

	//store new bucket id
	int newBucketPosition;	
	
	CALL_BF(BF_GetBlockCounter(openFiles[indexDesc]->fd,&newBucketPosition));
	newBucketPosition--;
	CALL_HT(BucketStatsInit(indexDesc,newBucketPosition));	//prosthese to bucket se block typou M gia ta stats


	//Unpin block since its data is changed
	//setDirty is in BlockHeaderInit function
	CALL_BF(BF_UnpinBlock(newBucket));


	BF_Block* block; // eyretirio
	BF_Block_Init(&block);

	//  store which bucket, buddies points to
	int index_to_bucket= find_hash_table_block(indexDesc,key);

	//Now we must find in which position of the block table, buddies start

	int counter=0;	//apo poy xekinane ta buddies
	int next_HT=1;

	
	int i;	
	
	// search in which position of the hash table 'buddies' start, 
	// so we can make half of buddies points to the new bucket
	//The use of do..while is essential since more than one hash table blocks can exist.
	do{
		CALL_BF(BF_GetBlock(openFiles[indexDesc]->fd,next_HT,block));
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
		CALL_BF(BF_GetBlock(openFiles[indexDesc]->fd,next_HT,block));
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
	CALL_BF(BF_GetBlock(openFiles[indexDesc]->fd,index_to_bucket,old_bucket));

	char* c = BF_Block_GetData(old_bucket);
	
	//after split, we increase local depth of the old bucket
	int local_deph = get_local_depth(old_bucket);
	local_deph++;
	memcpy(c+1+sizeof(int),&local_deph,sizeof(int));
	
	//since it's data is altered
	BF_Block_SetDirty(old_bucket);
	CALL_BF(BF_UnpinBlock(old_bucket));
	BF_Block_Destroy(&old_bucket);

	CALL_BF(BF_GetBlock(openFiles[indexDesc]->fd,newBucketPosition,newBucket));

	// update local depth of the new bucket too
	char* newBucketData = BF_Block_GetData(newBucket);
	memcpy(newBucketData+1+sizeof(int), &local_deph,sizeof(int));
	
	BF_Block_SetDirty(newBucket);
	CALL_BF(BF_UnpinBlock(newBucket));
	BF_Block_Destroy(&newBucket);


	// How many records can type D block store
	int number_of_records=((BF_BLOCK_SIZE - sizeof(char) - 2*sizeof(int)) * 8) / (sizeof(Record)*8 + 1);

	//How many bytes of a D-block are used to store flags
	int index_bytes=number_of_records % (sizeof(char)*8) == 0 ? number_of_records / (sizeof(char)*8) : number_of_records / (sizeof(char)*8) + 1;
	
	int overflowBlock = 0;

	int startingBlock=index_to_bucket;
	
	int updateCounter = 0;

	//In the below part is the re-insertion of each record + the new one
	do{ 
		Record* old_records = malloc(sizeof(Record));
	
		for (int i=0;i<number_of_records;i++) {
			
			BF_Block_Init(&old_bucket);
			CALL_BF(BF_GetBlock(openFiles[indexDesc]->fd,startingBlock,old_bucket));
			c = BF_Block_GetData(old_bucket);
			
			unsigned char flagValue;
			
			memcpy(&flagValue,c+1+2*sizeof(int)+i/8,sizeof(char));

			// Check if the flag is true or false
			flagValue = flagValue << i;
			flagValue = flagValue >>7;

			// if flag is false there is no record in position 'i'
			if (flagValue == 0) {
				CALL_BF(BF_UnpinBlock(old_bucket));	
				BF_Block_Destroy(&old_bucket);	
				continue;
			}
			
			// remove record, set its flag to false and re-insert it
			memcpy(old_records,c+1+2*sizeof(int) + index_bytes+sizeof(Record)*i,sizeof(Record));
			CALL_HT(BlockHeaderUpdate(old_bucket,i,0));
			CALL_BF(BF_UnpinBlock(old_bucket));
			BF_Block_Destroy(&old_bucket);
			

			CALL_HT(BucketStatsUpdate(indexDesc,index_to_bucket));	//gia na enimerothoun ta stats, afoy 'bgike' mia eggrafi
			CALL_HT(HT_InsertEntry(indexDesc,*old_records,tupleId, updateArray));
			
			
			memcpy(updateArray[updateCounter].city,old_records->city,20);
			memcpy(updateArray[updateCounter].surname,old_records->surname,20);
			updateArray[updateCounter].oldTupleId = ((startingBlock+1)*number_of_records + i);
			updateArray[updateCounter].newTupleId = *tupleId;
			updateCounter++;
		}

		// Check if the block has an overflow block
		BF_Block_Init(&old_bucket);
		CALL_BF(BF_GetBlock(openFiles[indexDesc]->fd,startingBlock,old_bucket));
		c = BF_Block_GetData(old_bucket);	

		memcpy(&overflowBlock,c+1,sizeof(int));

		CALL_BF(BF_UnpinBlock(old_bucket));
		BF_Block_Destroy(&old_bucket);

		free(old_records);

		startingBlock = overflowBlock;
	}while(overflowBlock != 0);
	
	if (HT_InsertEntry(indexDesc,*record,tupleId,updateArray) == HT_OK ){
		return HT_OK;
	}

	return HT_ERROR;
}



HT_ErrorCode extend_hash_table(int indexDesc){

	int maxNumberOfBuckets = (BF_BLOCK_SIZE - 1 - 3*sizeof(int))/sizeof(int); // poios einai o megistos arithmos apo buckets pou xoraei kathe hash table
	int currentNumberOfBuckets;	// poios einai o arithmos apo buckets pou exei to hash table
	int newHT=1;	// poio block typou Hash table einai meta. An einai 0 simainei oti den yparxei allo

	BF_Block* block;
	BF_Block_Init(&block);
	char* c;
	int global_depth;
	int newIndexes;

	CALL_BF(BF_GetBlock(openFiles[indexDesc]->fd,1,block));
	c= BF_Block_GetData(block);


	global_depth = get_global_depth(openFiles[indexDesc]->fd);
	if(global_depth == -1){
		return HT_ERROR;
	}
	
	newIndexes = power_of_two(global_depth);
	
	// ston pinaka auton tha apothikeyso prosorina ta ids pou exei apothikeysei to hash table mexri tora
	int* indexArray = malloc(sizeof(int)*2*newIndexes);
	
	//antigrafo tous palious deiktes stin katallili thesi tou pinaka
	int arrayCounter = 0;
	do{	

		CALL_BF(BF_GetBlock(openFiles[indexDesc]->fd,newHT,block));
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
		
		CALL_BF(BF_GetBlock(openFiles[indexDesc]->fd,newHT,block));
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
			CALL_BF(BF_AllocateBlock(openFiles[indexDesc]->fd,newHashBlock));
			CALL_BF(BF_GetBlockCounter(openFiles[indexDesc]->fd,&blockCounter));
			
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

// ----------------------------------------
// ----------------------------------------