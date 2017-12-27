#include "event.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>


extern "C" {



void iters_seek(
	rocksdb_iterator_t** piters,
	size_t num_iters,
	const uint64_t* topic_keys,
	uint64_t seek_event_id
) {
	topic_event_key tek = {0};
	tek.event_id = seek_event_id;
	for (size_t i = 0; i < num_iters; i++) {
		tek.topic_id = topic_keys[i];
		rocksdb_iter_seek(piters[i], (const char*)(&tek), sizeof(struct topic_event_key));
	}
}



int key_uint64_cmp(const char* key1, const char* key2, size_t key_len) {
	uint64_t v1 = *(uint64_t*)(key1);
	uint64_t v2 = *(uint64_t*)(key1);

	if (v1 < v2) {
		return -1;
	} else if (v1 > v2) {
		return 1;
	}

	return 0;
}


int key_uint64_single_uint64_offs_cmp(const char* key1, const char* key2, size_t key_len) {
	uint64_t v1 = *(uint64_t*)(&key1[sizeof(uint64_t)]);
	uint64_t v2 = *(uint64_t*)(&key2[sizeof(uint64_t)]);

	if (v1 < v2) {
		return -1;
	} else if (v1 > v2) {
		return 1;
	}

	return 0;
}



}