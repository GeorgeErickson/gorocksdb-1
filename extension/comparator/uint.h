/*

Comparators
for example doubleuint64comparator_t:
- 8 byte topicID
- 8 byte eventID


*/
#pragma once

#ifdef __cplusplus
extern "C" {
#endif


#include "rocksdb/c.h"

typedef struct singleuint64comparator_t singleuint64comparator_t;
singleuint64comparator_t * singleuint64comparator_new(size_t cmp_fix_offset);	

typedef struct doubleuint64comparator_t doubleuint64comparator_t;
doubleuint64comparator_t * doubleuint64comparator_new(size_t cmp_fix_offset);	

typedef struct reversesingleuint64comparator_t reversesingleuint64comparator_t;
reversesingleuint64comparator_t * reversesingleuint64comparator_new(size_t cmp_fix_offset);	



#ifdef __cplusplus
}  /* end extern "C" */
#endif