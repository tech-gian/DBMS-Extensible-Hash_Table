#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "hash_file.h"
#include "sht_file.h"

//! exei elegxthei kai douleuei gia 1000 eggrafes me global depth 1
#define RECORDS_NUM 10 // you can change it if you want 
#define GLOBAL_DEPT 1 // you can change it if you want
#define PRIMARY_FILE_NAME "data.db"
#define SECONDARY_FILE_NAME "Sec_data.db"

const char* names[] = {
  "Yannis",
  "Christofos",
  "Sofia",
  "Marianna",
  "Vagelis",
  "Maria",
  "Iosif",
  "Dionisis",
  "Konstantina",
  "Theofilos",
  "Giorgos",
  "Dimitris"
};

const char* surnames[] = {
  "Ioannidis",
  "Svingos",
  "Karvounari",
  "Rezkalla",
  "Nikolopoulos",
  "Berreta",
  "Koronis",
  "Gaitanis",
  "Oikonomou",
  "Mailis",
  "Michas",
  "Halatsis"
};

const char* cities[] = {
  "Athens",
  "San_Francisco",
  "Los_Angeles",
  "Amsterdam",
  "London",
  "New_York",
  "Tokyo",
  "Hong_Kong",
  "Munich",
  "Miami",
  
};

#define CALL_OR_DIE(call)     \
  {                           \
    HT_ErrorCode code = call; \
    if (code != HT_OK) {      \
      printf("Error\n");      \
      exit(code);             \
    }                         \
  }

int main() {
	BF_Init(LRU);
  
	CALL_OR_DIE(HT_Init());

	int indexDesc;
	CALL_OR_DIE(HT_CreateIndex(PRIMARY_FILE_NAME, GLOBAL_DEPT));
	CALL_OR_DIE(HT_OpenIndex(PRIMARY_FILE_NAME, &indexDesc)); 
  
  int secIndexDesc;
  CALL_OR_DIE(SHT_CreateSecondaryIndex(SECONDARY_FILE_NAME,"city",20,GLOBAL_DEPT,PRIMARY_FILE_NAME));
  CALL_OR_DIE(SHT_OpenSecondaryIndex(SECONDARY_FILE_NAME, &secIndexDesc)); 


	Record record;
  SecondaryRecord secRecord;
	srand(12569874);
	int r;

	int tuppleId;
	
	UpdateRecordArray* updateArray;
	updateArray = malloc(sizeof(UpdateRecordArray)*8);
	for (int i=0;i<8;i++){
		updateArray[i].oldTupleId = -1;
		updateArray[i].newTupleId = -1;
	}

	printf("Insert Entries\n");
	for (int id = 0; id < RECORDS_NUM; ++id) {
    for (int i=0;i<8;i++){
		updateArray[i].oldTupleId = -1;
		updateArray[i].newTupleId = -1;
	}
		// create a record
		record.id = id;
		r = rand() % 12;
		memcpy(record.name, names[r], strlen(names[r]) + 1);
		r = rand() % 12;
		memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
		r = rand() % 10;
		memcpy(record.city, cities[r], strlen(cities[r]) + 1);
		CALL_OR_DIE(HT_InsertEntry(indexDesc, record,&tuppleId,updateArray));
    strcpy(secRecord.index_key,record.city);
    secRecord.tupleId = tuppleId;
    
    if(updateArray[0].newTupleId != -1){
      SHT_SecondaryUpdateEntry(secIndexDesc,updateArray);
    }
    CALL_OR_DIE(SHT_SecondaryInsertEntry(secIndexDesc,secRecord));
	}

	printf("RUN PrintAllEntries\n");
	int id = rand() % RECORDS_NUM;
	CALL_OR_DIE(HT_PrintAllEntries(indexDesc, NULL));

  printf("\n\nRUN PrintSecondary\n");
  CALL_OR_DIE(SHT_PrintAllEntries(secIndexDesc, NULL));



}
