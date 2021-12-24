#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "hash_file.h"

#define RECORDS_NUM 4 // you can change it if you want
#define GLOBAL_DEPT 10 // you can change it if you want
#define FILE_NAME "data.db"

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
  "San Francisco",
  "Los Angeles",
  "Amsterdam",
  "London",
  "New York",
  "Tokyo",
  "Hong Kong",
  "Munich",
  "Miami"
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
  CALL_OR_DIE(HT_CreateIndex(FILE_NAME, GLOBAL_DEPT));
  CALL_OR_DIE(HT_OpenIndex(FILE_NAME, &indexDesc)); 

  Record record;
  srand(12569874);
  int r;
  printf("Insert Entries\n");

  //! prota bazo kapoies eggrafes me diaforetika ids
    for (int id = 0; id < RECORDS_NUM; ++id) {
    // create a record
    record.id = id;
    r = rand() % 12;
    memcpy(record.name, names[r], strlen(names[r]) + 1);
    r = rand() % 12;
    memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
    r = rand() % 10;
    memcpy(record.city, cities[r], strlen(cities[r]) + 1);

    CALL_OR_DIE(HT_InsertEntry(indexDesc, record));
  }

    printf("RUN PrintAllEntries\n");
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc, NULL));
    CALL_OR_DIE(HashStatistics(FILE_NAME));
    printf("--------------------------\n");

    //! tora bazo kapoies eggrafes me to idio id alla toses oste na xorane se ena block
    //! diladi sto paradeigma mas 8. Ara i for tha prosthesei alles 7 eggrafes me idio id 
    for (int id = 0; id < 7; ++id) {
    // create a record
    record.id = 1;
    r = rand() % 12;
    memcpy(record.name, names[r], strlen(names[r]) + 1);
    r = rand() % 12;
    memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
    r = rand() % 10;
    memcpy(record.city, cities[r], strlen(cities[r]) + 1);

    CALL_OR_DIE(HT_InsertEntry(indexDesc, record));
    }   
     printf("RUN PrintAllEntries\n");
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc, NULL));
    CALL_OR_DIE(HashStatistics(FILE_NAME));
    printf("--------------------------\n");
    
    //! tora bazo liges akoma me to idio id  oste na dimiourgithei block yperxeilisis
    for (int id = 0; id < 3; ++id) {
    // create a record
    record.id = 1;
    r = rand() % 12;
    memcpy(record.name, names[r], strlen(names[r]) + 1);
    r = rand() % 12;
    memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
    r = rand() % 10;
    memcpy(record.city, cities[r], strlen(cities[r]) + 1);

    CALL_OR_DIE(HT_InsertEntry(indexDesc, record));
    }   
     printf("RUN PrintAllEntries\n");
    CALL_OR_DIE(HT_PrintAllEntries(indexDesc, NULL));
    CALL_OR_DIE(HashStatistics(FILE_NAME));
    printf("--------------------------\n");


    //! kalo tin printAllEntries alla me id=1 gia na mou emfanisei oles tis eggrafes me id = 1 poy ebala
    //! kai autes tou block yperxeilisis
  int id = 1;
  CALL_OR_DIE(HT_PrintAllEntries(indexDesc, &id));


  CALL_OR_DIE(HashStatistics(FILE_NAME));


  CALL_OR_DIE(HT_CloseFile(indexDesc));
  BF_Close();
}
