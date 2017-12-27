#ifdef __cplusplus
extern "C" {
#endif
#include <stdlib.h> 
#include "rocksdb/c.h"

typedef void (*iter_mover_fn)(rocksdb_iterator_t*);


// buffer is filled with key|value|key|value and plengths must be at least of size max_cnt *2 which is filled with key_len|value_len|key_len|value_len...
// if pneeded != 0 means that the iterator is valid but the first element(key+value) tried to insert to buffer is too large. 
// pneeded is then key_len + value_len.
void iter_valid_next_to_buffer(
	rocksdb_iterator_t* iter, 
	const int64_t direction, 
	char* buffer, size_t buffer_size, 
	size_t* plengths, size_t max_cnt, size_t* psize, size_t* pcnt,
	size_t* pneeded, size_t* pvalid, char** errptr);



#ifdef __cplusplus
}  /* end extern "C" */
#endif