#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "hash_file.h"
#include "sht_file.h"

#define RECORDS_NUM 10 // you can change it if you want 
#define GLOBAL_DEPT 1 // you can change it if you want
#define PRIMARY_FILE_NAME "data.db"
#define SECONDARY_FILE_NAME "Sec_data.db"
#define SECONDARY_FILE_NAME1 "Sec_data1.db"

#define PRIMARY_FILE_NAME1 "data1.db"
#define SECONDARY_FILE_NAME1 "Sec_data1.db"

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
    CALL_OR_DIE(SHT_Init());

    int a;
	CALL_OR_DIE(HT_CreateIndex("a", GLOBAL_DEPT));
	CALL_OR_DIE(HT_OpenIndex("a", &a)); 
  
    int A;
    CALL_OR_DIE(SHT_CreateSecondaryIndex("A2","surname",20,GLOBAL_DEPT,"a"));
    CALL_OR_DIE(SHT_OpenSecondaryIndex("A2", &A));

    int b;
	CALL_OR_DIE(HT_CreateIndex("b", GLOBAL_DEPT));
	CALL_OR_DIE(HT_OpenIndex("b", &b)); 
  
    int B;
    CALL_OR_DIE(SHT_CreateSecondaryIndex("B2","city",123,4,"b"));
    CALL_OR_DIE(SHT_OpenSecondaryIndex("B2", &B));

    int c;
	CALL_OR_DIE(HT_CreateIndex("c", GLOBAL_DEPT));
	CALL_OR_DIE(HT_OpenIndex("c", &c)); 
  
    int C;
    CALL_OR_DIE(SHT_CreateSecondaryIndex("C2","surname",20,GLOBAL_DEPT,"c"));
    CALL_OR_DIE(SHT_OpenSecondaryIndex("C2", &C));

    CALL_OR_DIE(SHT_CloseSecondaryIndex(B));
    CALL_OR_DIE(HT_CloseFile(b));

    int d;
	CALL_OR_DIE(HT_CreateIndex("d", GLOBAL_DEPT));
	CALL_OR_DIE(HT_OpenIndex("d", &d));

    CALL_OR_DIE(SHT_CloseSecondaryIndex(A));
    CALL_OR_DIE(HT_CloseFile(a));
  
    int D;
    CALL_OR_DIE(SHT_CreateSecondaryIndex("D2","city",20,GLOBAL_DEPT,"d"));
    CALL_OR_DIE(SHT_OpenSecondaryIndex("D2", &D));

    CALL_OR_DIE(SHT_CloseSecondaryIndex(D));
    CALL_OR_DIE(HT_CloseFile(d));
    CALL_OR_DIE(SHT_CloseSecondaryIndex(C));
    CALL_OR_DIE(HT_CloseFile(c));

  // dimiourgia arxeiou epektatou katakermatismou
	int indexDesc;
	CALL_OR_DIE(HT_CreateIndex(PRIMARY_FILE_NAME, GLOBAL_DEPT));
	CALL_OR_DIE(HT_OpenIndex(PRIMARY_FILE_NAME, &indexDesc)); 
  
  //dimiourgia deutereuontos eurethriou sto pedio surname
  int secIndexDesc;
  CALL_OR_DIE(SHT_CreateSecondaryIndex(SECONDARY_FILE_NAME, "surname", 20, GLOBAL_DEPT, PRIMARY_FILE_NAME));
  CALL_OR_DIE(SHT_OpenSecondaryIndex(SECONDARY_FILE_NAME, &secIndexDesc)); 


  // eisagogi  endeiktikon eggrafon  sto HT kai sto SHT 
	Record record;
  SecondaryRecord secRecord;
	srand(12569874);
	int r;

	int tuppleId;
  
  int numberOfFlags = ((BF_BLOCK_SIZE - sizeof(char) - 2*sizeof(int)) * 8) / (sizeof(Record)*8 + 1);
	
	UpdateRecordArray* updateArray;
	updateArray = malloc(sizeof(UpdateRecordArray)*numberOfFlags);
	for (int i=0;i<numberOfFlags;i++){
		updateArray[i].oldTupleId = -1;
		updateArray[i].newTupleId = -1;
	}

  char secondaryKey[20];

	printf("Insert Entries\n");
  int id;
	for (id = 0; id < RECORDS_NUM; ++id) {
    
    for (int i=0;i<numberOfFlags;i++){
		updateArray[i].oldTupleId = -1;
		updateArray[i].newTupleId = -1;
	}
		// create a record
		record.id = id;
		r = rand() % 12;
		memcpy(record.name, names[r], strlen(names[r]) + 1);
		r = rand() % 12;
		memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
    strcpy(secondaryKey, surnames[r]);
		r = rand() % 10;
		memcpy(record.city, cities[r], strlen(cities[r]) + 1);


		CALL_OR_DIE(HT_InsertEntry(indexDesc, record,&tuppleId,updateArray));
    strcpy(secRecord.index_key,record.surname);
    secRecord.tupleId = tuppleId;
    
    if(updateArray[0].newTupleId != -1){
      SHT_SecondaryUpdateEntry(secIndexDesc,updateArray);
    }
    CALL_OR_DIE(SHT_SecondaryInsertEntry(secIndexDesc,secRecord));

	}

  //bima 4
	int primaryId = rand() % RECORDS_NUM;
	printf("RUN PrintAllEntries with id = %d\n",primaryId);
  CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &primaryId));

  //bima 5
  printf("\n\nRUN PrintSecondary with key: %s\n",secondaryKey);
  CALL_OR_DIE(SHT_PrintAllEntries(secIndexDesc, secondaryKey));

  //* eisagogi neas eggrafis sto primary kai to secondary hash table
  
  // arxikopoiisi tou updateArray
  for (int i=0;i<numberOfFlags;i++){
		updateArray[i].oldTupleId = -1;
		updateArray[i].newTupleId = -1;
	}
  
  // dimiourgia eggrafis
  record.id = id;
  r = rand() % 12;
  memcpy(record.name, names[r], strlen(names[r]) + 1);
  r = rand() % 12;
  memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
  r = rand() % 10;
  memcpy(record.city, cities[r], strlen(cities[r]) + 1);
  CALL_OR_DIE(HT_InsertEntry(indexDesc, record,&tuppleId,updateArray));
  strcpy(secRecord.index_key,record.surname);
  secRecord.tupleId = tuppleId;
  
  if(updateArray[0].newTupleId != -1){
    SHT_SecondaryUpdateEntry(secIndexDesc,updateArray);
  }
  CALL_OR_DIE(SHT_SecondaryInsertEntry(secIndexDesc,secRecord));

  //bima 4
	printf("RUN PrintAllEntries with id = %d\n",id);
  CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &id));

  

  //bima 5
  printf("\n\nRUN PrintSecondary with key: %s\n",secondaryKey);
  CALL_OR_DIE(SHT_PrintAllEntries(secIndexDesc, secondaryKey));




  printf("\n\nRUN SHT_InnerJoin\n");

  // dimiourgia neou secondary hash table gia tin inner join
  int secIndexDesc1;
  CALL_OR_DIE(SHT_CreateSecondaryIndex(SECONDARY_FILE_NAME1, "city", 20, GLOBAL_DEPT, PRIMARY_FILE_NAME));
  CALL_OR_DIE(SHT_OpenSecondaryIndex(SECONDARY_FILE_NAME1, &secIndexDesc1));

  CALL_OR_DIE(SHT_InnerJoin(secIndexDesc, secIndexDesc1, "Miami"));

  CALL_OR_DIE(SHT_InnerJoin(secIndexDesc, secIndexDesc1, NULL));

  // ektyposi statistikon
  printf("RUN Hashstatistics\n");
  CALL_OR_DIE(HashStatistics(PRIMARY_FILE_NAME));
  
  printf("\n\nRUN SHT_hash statistics\n");
  CALL_OR_DIE(SHT_HashStatistics(SECONDARY_FILE_NAME));



  // Closing open files
  CALL_OR_DIE(HT_CloseFile(indexDesc));
  CALL_OR_DIE(SHT_CloseSecondaryIndex(secIndexDesc));
  CALL_OR_DIE(SHT_CloseSecondaryIndex(secIndexDesc1));

  // Free memory
  BF_Close();
  free(updateArray);
}
