#include "multiiterator.h"
#include "goiterator.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <vector>
#include "rocksdb/c.h"

extern "C" {

struct multiiterator_t {
	std::vector<rocksdb_iterator_t*> iters;
	rocksdb_iterator_t *current_iter;
	int current_iter_idx;
	unsigned char valid;

	iters_cmp_fn iters_cmp;
	size_t iters_cmp_fix_offset;

	key_cmp_fn key_cmp;
};


multiiterator_t* create_multiiterator_by_rocksdb_iterators(
	rocksdb_iterator_t** iters, const size_t cnt_itrs,
	size_t iters_cmp_fix_offset) {
	multiiterator_t *multi_iter = new multiiterator_t;

	for(size_t i = 0; i < cnt_itrs; i++) {
		multi_iter->iters.push_back(iters[i]);
	}

	multi_iter->current_iter = NULL;
	multi_iter->iters_cmp_fix_offset = iters_cmp_fix_offset;


	return multi_iter;
}


void multiiterator_reset_base(multiiterator_t* multi_iter) {
	multi_iter->current_iter = NULL;
	multi_iter->current_iter_idx = -1;
	multi_iter->valid = 0;
}


unsigned char multiiterator_seek_to_first(multiiterator_t* multi_iter, char** errptr) {
	for (std::vector<rocksdb_iterator_t*>::iterator it = multi_iter->iters.begin() ; it != multi_iter->iters.end(); ++it) {
		rocksdb_iter_seek_to_first(*it);
	}

	multiiterator_reset_base(multi_iter);
	multiiterator_next_and_set_valid(multi_iter);
}


unsigned char multiiterator_seek_to_last(multiiterator_t* multi_iter, char** errptr) {
	for (std::vector<rocksdb_iterator_t*>::iterator it = multi_iter->iters.begin() ; it != multi_iter->iters.end(); ++it) {
		rocksdb_iter_seek_to_last(*it);
	}

	multiiterator_reset_base(multi_iter);
	multiiterator_next_and_set_valid(multi_iter);
}


unsigned char multiiterator_seek(
		multiiterator_t* multi_iter,
	    const char* const* keys_list,
    	const size_t* keys_list_sizes,
    	char** errptr) {

	size_t num = multi_iter->iters.size();
	for (size_t i = 0; i < num; i++) {
		rocksdb_iterator_t* iter = multi_iter->iters[i];
		rocksdb_iter_seek(iter, keys_list[i], keys_list_sizes[i]);
	}

	multiiterator_reset_base(multi_iter);
	multiiterator_next_and_set_valid(multi_iter);
}


unsigned char multiiterator_seek_for_prev(
		multiiterator_t* multi_iter,
	    const char* const* keys_list,
    	const size_t* keys_list_sizes,
    	char** errptr) {

	size_t num = multi_iter->iters.size();
	for (size_t i = 0; i < num; i++) {
		rocksdb_iterator_t* iter = multi_iter->iters[i];
		rocksdb_iter_seek_for_prev(iter, keys_list[i], keys_list_sizes[i]);
	}

	multiiterator_reset_base(multi_iter);
	multiiterator_next_and_set_valid(multi_iter);
}


unsigned char multiiterator_valid(multiiterator_t* multi_iter) {
	return multi_iter->valid;
}


unsigned char multiiterator_key_value(
	const multiiterator_t* multi_iter,
	const char** pkey, size_t* klen,
	const char** pvalue, size_t* vlen) {
	const rocksdb_iterator_t* current_iter = multi_iter->current_iter;
	if (current_iter) {
		*pkey = rocksdb_iter_key(current_iter, klen);
		*pvalue = rocksdb_iter_value(current_iter, klen);
	}
}


unsigned char multiiterator_inner_valid(multiiterator_t* multi_iter) {
	if (multi_iter->current_iter) {
		multi_iter->valid = 1;
		return 1;
	}

	size_t num = multi_iter->iters.size();
	for (size_t i = 0; i < num; i++) {
		rocksdb_iterator_t* iter = multi_iter->iters[i];
		if (rocksdb_iter_valid(iter)) {
			multi_iter->valid = 1;
			return 1;
		}
	}

	multi_iter->valid = 0;
	return 0;
}


void multiiterator_next_and_set_valid(multiiterator_t* multi_iter) {
	rocksdb_iterator_t* current_iter = multi_iter->current_iter;
	if (current_iter && rocksdb_iter_valid(current_iter)) {
		rocksdb_iter_next(current_iter);
	}

	

	int idx = -1;
	multi_iter->iters_cmp(multi_iter, NULL, multi_iter->iters_cmp_fix_offset, multi_iter->key_cmp, &idx);
	if(idx != -1) {
		multi_iter->current_iter = multi_iter->iters[idx];
		multi_iter->current_iter_idx = idx;
		multi_iter->valid = true;
	} else {
		multi_iter->current_iter = NULL;
		multi_iter->valid = 0;
	}
}


void multiiterator_valid_next_to_buffer(
	multiiterator_t* multi_iter,
	const int64_t direction, 
	char* buffer, size_t buffer_size, 
	uint32_t* plengths, uint32_t* pindexes, size_t max_cnt, size_t* psize, size_t* pcnt, 
	size_t* pneeded, size_t* pvalid, char** errptr) {

	size_t cnt = 0;
	size_t bpos = 0;
	bool valid = false;
	for(; multiiterator_valid(multi_iter) && cnt < max_cnt; cnt++) {
		const rocksdb_iterator_t* iter = multi_iter->current_iter;
		size_t key_len;
		size_t value_len;
		const char *key = rocksdb_iter_key(iter, &key_len);
		const char *value = rocksdb_iter_value(iter, &value_len);
		if (bpos + key_len + value_len > buffer_size) {
			*pneeded = key_len + value_len;
			break;
		}

		memcpy(buffer+bpos, key, key_len);
		bpos += key_len;
		memcpy(buffer+bpos, value, value_len);
		bpos += value_len;

		size_t plength_pos = cnt*2;
		plengths[plength_pos] = (uint32_t)key_len;
		plengths[plength_pos+1] = (uint32_t)value_len;

		pindexes[cnt] = (uint32_t)multi_iter->current_iter_idx;
		multiiterator_next_and_set_valid(multi_iter);
	}

	*pcnt = cnt;
	*psize = bpos;

	if (valid) {
		*pvalid = 1;
	}
}


void multiiterator_close(multiiterator_t* multi_iter) {
	for (std::vector<rocksdb_iterator_t*>::iterator it = multi_iter->iters.begin() ; it != multi_iter->iters.end(); ++it) {
		rocksdb_iter_destroy(*it);
	}

	multi_iter->valid = 0;
	delete multi_iter;
}


void multiiterator_cmp_all_fixed_suffix(
	multiiterator_t* multi_iter,
	rocksdb_iterator_t* current_iter,
	const size_t iters_cmp_fix_offset,
	key_cmp_fn key_cmp,
	int *next_idx){

	const char* min_key;
	size_t min_key_len = 0;
	int min_idx = -1;

	const size_t num = multi_iter->iters.size();

	size_t i = 0;
	for (; i < num; i++) {
		rocksdb_iterator_t* iter = multi_iter->iters[i];
		if (rocksdb_iter_valid(iter)) {
			min_key = rocksdb_iter_key(iter, &min_key_len);
			break;
		}
	}

	min_idx = (int)i;
	i++;
	if (i > num) {
		*next_idx = -1;
		return;
	}

	for (; i < num; i++) {
		rocksdb_iterator_t* iter = multi_iter->iters[i];
		if (rocksdb_iter_valid(iter)) {
			size_t key_len;
			const char* key = rocksdb_iter_key(iter, &key_len);
			if (key_len >= iters_cmp_fix_offset) {
				if (key_len == min_key_len) {
					int cmp = key_cmp(
						&min_key[min_key_len-iters_cmp_fix_offset], 
						&key[key_len-iters_cmp_fix_offset], key_len - iters_cmp_fix_offset);
					if (cmp > 0) {
						min_idx = (int)(i);
						min_key = key;
					}
				} else if (key_len < min_key_len) {
					min_key = key;
					min_idx = (int)(i);
				}
			}

		}
	}

	*next_idx = min_idx;
}


void multiiterator_cmp_all_fixed_prefix(
	multiiterator_t* multi_iter,
	rocksdb_iterator_t* current_iter,
	const size_t iters_cmp_fix_offset,
	key_cmp_fn key_cmp,
	int *next_idx){
	const char* min_key;
	size_t min_key_len = 0;
	int min_idx = -1;

	const size_t num = multi_iter->iters.size();

	size_t i = 0;
	for (; i < num; i++) {
		rocksdb_iterator_t* iter = multi_iter->iters[i];
		if (rocksdb_iter_valid(iter)) {
			min_key = rocksdb_iter_key(iter, &min_key_len);
			break;
		}
	}

	min_idx = (int)i;
	i++;
	if (i > num) {
		*next_idx = -1;
		return;
	}

	for (;i < num; i++) {
		rocksdb_iterator_t* iter = multi_iter->iters[i];
		if (rocksdb_iter_valid(iter)) {
			size_t key_len;
			const char* key = rocksdb_iter_key(iter, &key_len);
			if (key_len >= iters_cmp_fix_offset) {
				if (key_len == min_key_len) {
					int cmp = key_cmp(
						&min_key[iters_cmp_fix_offset], 
						&key[iters_cmp_fix_offset], key_len - iters_cmp_fix_offset);
					if (cmp > 0) {
						min_idx = (int)(i);
						min_key = key;
					}
				} else if (key_len < min_key_len) {
					min_key = key;
					min_idx = (int)(i);
				} 
			}
		}
	}

	*next_idx = min_idx;
}



// cmp setter 

void multiiterator_set_cmp_fn(multiiterator_t* multi_iter, iters_cmp_fn iters_cmp) {
	multi_iter->iters_cmp = iters_cmp;
}

void multiiterator_set_cmp_fn_cmp_all_fixed_suffix(multiiterator_t* multi_iter) {
	multi_iter->iters_cmp = multiiterator_cmp_all_fixed_suffix;
}


void multiiterator_set_cmp_fn_cmp_all_fixed_prefix(multiiterator_t* multi_iter) {
	multi_iter->iters_cmp = multiiterator_cmp_all_fixed_prefix;
}





// Functions for key_cmp_fn

int key_memcmp(const char* key1, const char* key2, size_t key_len) {
	return memcmp(key1, key2, key_len);
}


// key cmp setter

void multiiterator_set_keycmp_fn(multiiterator_t* multi_iter, key_cmp_fn key_cmp) {
	multi_iter->key_cmp = key_cmp;
}

void multiiterator_set_keycmp_fn_key_memcmp(multiiterator_t* multi_iter) {
	multi_iter->key_cmp = key_memcmp;
}


}