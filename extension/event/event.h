#ifdef __cplusplus
extern "C" {
#endif
#include <stdlib.h> 
#include "rocksdb/c.h"

typedef struct topic_event_key {
	uint64_t topic_id;
	uint64_t event_id;
} topic_event_key;


typedef void (*iters_seeker_fn)(
	rocksdb_iterator_t** piters,
	size_t num_iters,
	const uint64_t* begin_topic_keys,
	uint64_t seek_event_id
);


void iters_seek(
	rocksdb_iterator_t** piters,
	size_t num_iters,
	const uint64_t* topic_keys,
	uint64_t seek_event_id
);


int key_uint64_cmp(const char* key1, const char* key2, size_t key_len);

int key_uint64_single_uint64_offs_cmp(const char* key1, const char* key2, size_t key_len);



#ifdef __cplusplus
}  /* end extern "C" */
#endif