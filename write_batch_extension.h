
#ifdef __cplusplus
extern "C" {
#endif
#include <stdlib.h>
#include "rocksdb/c.h"

void writebatch_putv_cf(
    rocksdb_writebatch_t* b,
    size_t num_values,
    rocksdb_column_family_handle_t* column_family,
    const char* const* keys_list,
    const size_t* keys_list_sizes,
    const char* const* values_list,
    const size_t* values_list_sizes);


void writebatch_putv_cfs(
    rocksdb_writebatch_t* b,
    size_t num_values,
    rocksdb_column_family_handle_t** column_families,
    const char* const* keys_list,
    const size_t* keys_list_sizes,
    const char* const* values_list,
    const size_t* values_list_sizes);


void writebatch_deletev_cf(
    rocksdb_writebatch_t* b,
    size_t num_values,
    rocksdb_column_family_handle_t* column_family,
    const char* const* keys_list,
    const size_t* keys_list_sizes);



void writebatch_deletev_cfs(
    rocksdb_writebatch_t* b,
    size_t num_values,
    rocksdb_column_family_handle_t** column_families,
    const char* const* keys_list,
    const size_t* keys_list_sizes);


#ifdef __cplusplus
}  /* end extern "C" */
#endif