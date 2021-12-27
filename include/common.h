#include "hash_file.h"
#include <stdbool.h>
#include <limits.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "sht_file.h"
#pragma once

#define META_BLOCK 0
#define FIRST_M_BLOCK 0
#define FIRST_HASH_BLOCK 1

struct openFile {
	int fd;
	char *name;
};
int openFilesCount;
struct openFile* openFiles[MAX_OPEN_FILES];

struct record {
	int id;
	char name[15];
	char surname[20];
	char city[20];
};


#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {      \
    BF_PrintError(code);    \
    return HT_ERROR;        \
  }                         \
}

#define CALL_HT(call)         \
{                             \
	HT_ErrorCode code = call; \
	if (code != HT_OK) {      \
		printf("Error\n");    \
		exit(code);           \
	}                         \
}


int power_of_two(int power);

int hash_id(unsigned int id);
uint hash_string(char* value);


// This function calculates how many Records exist in block
int count_flags(BF_Block* block);


/////////////////////
// Extra-Helping Functions to initialize blocks, based on our structure

// Η συνάρτηση αυτή παίρνει έναν pointer σε block και τον τύπο του block
// και κάνει τις σωστές αρχικοποιήσεις στο header και στο ευρετήριο του block.
HT_ErrorCode BlockHeaderInit(BF_Block* block, char type);

// Η συνάρτηση αυτή παίρνει έναν pointer σε block και αλλάζει την τιμή
// του flag στο ευρετήριο σε value, για το δεδομένο που προστέθηκε ή αφαιρέθηκε στη θέση flagPosition.
HT_ErrorCode BlockHeaderUpdate(BF_Block* block, int flagPosition, char value);

/////////////////////


////////////////////////////
// Functions for primary hash table InsertEntry

int find_hash_table_block(int indexDesc, unsigned int key);

int get_global_depth(int fileDesc);

int get_local_depth(BF_Block* block);

HT_ErrorCode insert_record(Record* record, int indexDesc, int blockIndex, int* tupleId);

HT_ErrorCode arrange_buckets(const int indexDesc,int buddies_number,Record* record,unsigned int key,UpdateRecordArray* updateArray);

HT_ErrorCode extend_hash_table(int indexDesc);

////////////////////////////
// Functions for secondary hash table InsertEntry

HT_ErrorCode sht_insert_record(Record* record, int indexDesc, int blockIndex);

HT_ErrorCode sht_arrange_buckets(const int indexDesc,int buddies_number,Record* record,unsigned int key);

HT_ErrorCode sht_extend_hash_table(int indexDesc);