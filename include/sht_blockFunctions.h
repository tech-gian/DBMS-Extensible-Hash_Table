#include "common.h"


typedef struct statistics {
    int bucketID;
    int max;
    int min;
    float average;
    int sum;
    int counter;
} Statistics;



/////////////////////
// Extra-Helping Functions to initialize bucketStats of MBlock, based on our structure

// Η συνάρτηση αυτή αρχικοποιεί ένα νέο Statistics σε υπάρχον
// ή νέο MBlock. Καλείται κάθε φορά που δημιουργείται ένα νέο DBlock
HT_ErrorCode SHT_BucketStatsInit(int fileDesc, int id);

// Η συνάρτηση αυτή ενημερώνη τις τιμές ενός υπάρχοντος Statistics
// με βάση τις εγγραφές κάποιου DBlock που υπάρχει ήδη
HT_ErrorCode SHT_BucketStatsUpdate(int indexDesc, int id);

/////////////////////


