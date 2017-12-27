
#ifdef __cplusplus
extern "C" {
#endif
#include <stdlib.h> 
#include "rocksdb/c.h"


void iter_key_value(
	const rocksdb_iterator_t* iter, 
	const char** key, size_t* key_len, 
	const char** value, size_t* value_len);
unsigned char iter_valid_key_value(
	const rocksdb_iterator_t* iter, 
	const char** key, size_t* key_len, 
	const char** value, size_t* value_len);
unsigned char iter_next_valid_key_value(
	rocksdb_iterator_t* iter, 
	const char** key, size_t* key_len, 
	const char** value, size_t* value_len);
unsigned char iter_prev_valid_key_value(
	rocksdb_iterator_t* iter, 
	const char** key, size_t* key_len, 
	const char** value, size_t* value_len);







#ifdef __cplusplus
}  /* end extern "C" */
#endif