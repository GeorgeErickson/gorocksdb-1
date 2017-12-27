
#ifdef __cplusplus
extern "C" {
#endif
#include <stdlib.h>
#include "rocksdb/c.h"


typedef struct multiiterator_t multiiterator_t;

typedef rocksdb_iterator_t* (*iter_get_fn)(const multiiterator_t*);

typedef int (*key_cmp_fn)(const char* key1, const char* key2, size_t key_len);

typedef void (*iters_cmp_fn)(
	multiiterator_t* multi_iter,
	rocksdb_iterator_t* current_itr,
	const size_t iters_cmp_fix_size,
	key_cmp_fn key_cmp,
	int *next_idx);



multiiterator_t* create_multiiterator_by_rocksdb_iterators(
	rocksdb_iterator_t** iters, const size_t cnt_itrs,
	size_t iters_cmp_fix_offset);


unsigned char multiiterator_seek_to_first(multiiterator_t* multi_iter, char** errptr);

unsigned char multiiterator_seek_to_last(multiiterator_t* multi_iter, char** errptr);

unsigned char multiiterator_seek(
		multiiterator_t* multi_iter,
	    const char* const* keys_list,
    	const size_t* keys_list_sizes,
    	char** errptr);

unsigned char multiiterator_seek_for_prev(
		multiiterator_t* multi_iter,
	    const char* const* keys_list,
    	const size_t* keys_list_sizes,
    	char** errptr);

unsigned char multiiterator_valid(multiiterator_t* multi_iter);

unsigned char multiiterator_key_value(
	const multiiterator_t* multi_iter,
	const char** pkey, size_t* klen,
	const char** pvalue, size_t* vlen);

void multiiterator_next_and_set_valid(multiiterator_t* multi_iter);

void multiiterator_valid_next_to_buffer(
	multiiterator_t* multi_iter,
	const int64_t direction, 
	char* buffer, size_t buffer_size, 
	size_t* plengths, uint32_t* pindexes, size_t max_cnt, size_t* psize, size_t* pcnt, 
	size_t* pneeded, size_t* pvalid, char** errptr);


void multiiterator_close(multiiterator_t* multi_iter);


void multiiterator_cmp_all_fixed_suffix(
	multiiterator_t* multi_iter,
	rocksdb_iterator_t* current_iter,
	const size_t iters_cmp_fix_offset,
	key_cmp_fn key_cmp,
	int *next_idx);

void multiiterator_cmp_all_fixed_prefix(
	multiiterator_t* multi_iter,
	rocksdb_iterator_t* current_iter,
	const size_t iters_cmp_fix_offset,
	key_cmp_fn key_cmp,
	int *next_idx);


// cmp setter

void multiiterator_set_cmp_fn(multiiterator_t* multi_iter, iters_cmp_fn iters_cmp);

void multiiterator_set_cmp_fn_cmp_all_fixed_suffix(multiiterator_t* multi_iter);

void multiiterator_set_cmp_fn_cmp_all_fixed_prefix(multiiterator_t* multi_iter);



// Functions for key_cmp_fn

int key_memcmp(const char* key1, const char* key2, size_t key_len);

// interprets the last 8 bytes as uint64_t
// key_len must be sizeof(uint64)
int key_uint64cmp(const char* key1, const char* key2, size_t key_len);


// key cmp setter

void multiiterator_set_keycmp_fn(multiiterator_t* multi_iter, key_cmp_fn key_cmp);

void multiiterator_set_keycmp_fn_key_memcmp(multiiterator_t* multi_iter);


#ifdef __cplusplus
}  /* end extern "C" */
#endif