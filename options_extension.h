#ifdef __cplusplus
extern "C" {
#endif
#include <stdlib.h>
#include "rocksdb/c.h"


rocksdb_readoptions_t *rocksdb_readoptions_create_setup_quick(
	unsigned char verify_checksums,
	unsigned char fill_cache,
	unsigned char tailing,
	const char* upper_bound, size_t upper_bound_len,
	size_t readahead_size,
	unsigned char pin_data);


#ifdef __cplusplus
}  /* end extern "C" */
#endif