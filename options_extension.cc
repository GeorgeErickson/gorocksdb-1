#include "options_extension.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "rocksdb/c.h"

extern "C" {


rocksdb_readoptions_t *rocksdb_readoptions_create_setup_quick(
	unsigned char verify_checksums,
	unsigned char fill_cache,
	unsigned char tailing,
	const char* upper_bound, size_t upper_bound_len,
	size_t readahead_size,
	unsigned char pin_data) {

	rocksdb_readoptions_t* ro = rocksdb_readoptions_create();
	rocksdb_readoptions_set_verify_checksums(ro, verify_checksums);
	rocksdb_readoptions_set_fill_cache(ro, fill_cache);
	rocksdb_readoptions_set_tailing(ro, tailing);
	if (upper_bound_len) {
		rocksdb_readoptions_set_iterate_upper_bound(ro, upper_bound, upper_bound_len);
	}
	rocksdb_readoptions_set_readahead_size(ro, readahead_size),
	rocksdb_readoptions_set_pin_data(ro, pin_data);
	return ro;
}


}