#include "goiterator.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "rocksdb/c.h"

extern "C" {



void iter_valid_next_to_buffer(
	rocksdb_iterator_t* iter, const int64_t direction, 
	char* buffer, size_t buffer_size, 
	size_t* plengths, size_t max_cnt, size_t* psize, size_t* pcnt, 
	size_t* pneeded,  size_t* pvalid, char** errptr) {

	size_t cnt = 0;
	size_t bpos = 0;
	bool valid = false;

	iter_mover_fn move_fn;
	if (direction > 0) {
		move_fn = &rocksdb_iter_next;
	} else if (direction < 0) {
		move_fn = &rocksdb_iter_prev;
	}

	for(valid = rocksdb_iter_valid(iter); valid && cnt < max_cnt; cnt++) {
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
		plengths[plength_pos] = key_len;
		plengths[plength_pos+1] = value_len;

		move_fn(iter);
		valid = rocksdb_iter_valid(iter);
	}

	rocksdb_iter_get_error(iter, errptr);

	*pcnt = cnt;
	*psize = bpos;

	if (valid) {
		*pvalid = 1;
	}

}


}