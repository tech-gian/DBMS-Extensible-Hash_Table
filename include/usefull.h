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

typedef struct {  //μπορειτε να αλλαξετε τη δομη συμφωνα  με τις ανάγκες σας
	char surname[20];
	char city[20];
	int oldTupleId; // η παλια θέση της εγγραφής πριν την εισαγωγή της νέας
	int newTupleId; // η νέα θέση της εγγραφής που μετακινήθηκε μετα την εισαγωγή της νέας εγγραφής 
	
} UpdateRecordArray;

typedef struct{
	char index_key[20];
	int tupleId;  /*Ακέραιος που προσδιορίζει το block και τη θέση μέσα στο block στην οποία     έγινε η εισαγωγή της εγγραφής στο πρωτεύον ευρετήριο.*/ 
}SecondaryRecord;
