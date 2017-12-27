#include "write_batch_extension.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "rocksdb/c.h"

extern "C" {




void writebatch_putv_cf(
    rocksdb_writebatch_t* b,
    size_t num_values,
    rocksdb_column_family_handle_t* column_family,
    const char* const* keys_list,
    const size_t* keys_list_sizes,
    const char* const* values_list,
    const size_t* values_list_sizes) {


	for(size_t i = 0; i < num_values; i++) {
		rocksdb_writebatch_put_cf(
			b,
			column_family,
			keys_list[i],
			keys_list_sizes[i],
			values_list[i],
			values_list_sizes[i]
		);
	}

}


void writebatch_putv_cfs(
    rocksdb_writebatch_t* b,
    size_t num_values,
    rocksdb_column_family_handle_t** column_families,
    const char* const* keys_list,
    const size_t* keys_list_sizes,
    const char* const* values_list,
    const size_t* values_list_sizes) {


	for(size_t i = 0; i < num_values; i++) {
		rocksdb_writebatch_put_cf(
			b,
			column_families[i],
			keys_list[i],
			keys_list_sizes[i],
			values_list[i],
			values_list_sizes[i]
		);
	}

}


void writebatch_deletev_cf(
    rocksdb_writebatch_t* b,
    size_t num_values,
    rocksdb_column_family_handle_t* column_family,
    const char* const* keys_list,
    const size_t* keys_list_sizes) {

	for(size_t i = 0; i < num_values; i++) {
		rocksdb_writebatch_delete_cf(
			b,
			column_family,
			keys_list[i],
			keys_list_sizes[i]
		);
	}
}


void writebatch_deletev_cfs(
    rocksdb_writebatch_t* b,
    size_t num_values,
    rocksdb_column_family_handle_t** column_families,
    const char* const* keys_list,
    const size_t* keys_list_sizes) {

	for(size_t i = 0; i < num_values; i++) {
		rocksdb_writebatch_delete_cf(
			b,
			column_families[i],
			keys_list[i],
			keys_list_sizes[i]
		);
	}
}


}