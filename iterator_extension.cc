#include "iterator_extension.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "rocksdb/c.h"

extern "C" {

void iter_key_value(const rocksdb_iterator_t* iter, const char** key, size_t* key_len, const char** value, size_t* value_len) {
	*key = rocksdb_iter_key(iter, key_len);
	*value = rocksdb_iter_value(iter,  value_len);
}


unsigned char iter_valid_key_value(const rocksdb_iterator_t* iter, const char** key, size_t* key_len, const char** value, size_t* value_len) {
	unsigned char valid = rocksdb_iter_valid(iter);
	if (valid == 0) {
		return 0;
	}

	*key = rocksdb_iter_key(iter, key_len);
	*value = rocksdb_iter_value(iter, value_len);

	return 1;
}


unsigned char iter_next_valid_key_value(rocksdb_iterator_t* iter, const char** key, size_t* key_len, const char** value, size_t* value_len) {
	rocksdb_iter_next(iter);
	return iter_valid_key_value(iter, key, key_len, value, value_len);
}


unsigned char iter_prev_valid_key_value(rocksdb_iterator_t* iter, const char** key, size_t* key_len, const char** value, size_t* value_len) {
	rocksdb_iter_prev(iter);
	return iter_valid_key_value(iter, key, key_len, value, value_len);
}


}