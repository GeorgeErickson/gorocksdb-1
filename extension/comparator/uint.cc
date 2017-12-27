#include "uint.h"

#include <stdlib.h>
#include "rocksdb/comparator.h"
#include "rocksdb/status.h"
#include "rocksdb/slice_transform.h"


using rocksdb::Comparator;
using rocksdb::Slice;

extern "C" {

struct singleuint64comparator_t : public Comparator {
  void* state_;
  void (*destructor_)(void*);
  int (*compare_)(
      void*,
      const char* a, size_t alen,
      const char* b, size_t blen);
  const char* (*name_)(void*);
  size_t cmp_prefix_offset;

  virtual ~singleuint64comparator_t() {

  }

  virtual int Compare(const Slice& a, const Slice& b) const override {
    uint64_t a_id1, b_id1;    
    const char *a_dat = a.data();
    const char *b_dat = b.data();

    a_id1 = *(uint64_t*)(&a_dat[cmp_prefix_offset]);
    b_id1 = *(uint64_t*)(&b_dat[cmp_prefix_offset]);
    if (a_id1 < b_id1) return -1;
    if (a_id1 > b_id1) return 1;
    
    return 0;
  }

  virtual const char* Name() const override { return "singleuint64comparator_t"; }

  // No-ops since the C binding does not support key shortening methods.
  virtual void FindShortestSeparator(std::string*,
                                     const Slice&) const override {}
  virtual void FindShortSuccessor(std::string* key) const override {}

};

singleuint64comparator_t * singleuint64comparator_new(size_t cmp_prefix_offset) {
  singleuint64comparator_t* cmp =  new singleuint64comparator_t;
  cmp->cmp_prefix_offset = cmp_prefix_offset;
  return cmp;
};



struct doubleuint64comparator_t : public Comparator {
  void* state_;
  void (*destructor_)(void*);
  int (*compare_)(
      void*,
      const char* a, size_t alen,
      const char* b, size_t blen);
  const char* (*name_)(void*);
  size_t cmp_prefix_offset;

  virtual ~doubleuint64comparator_t() {

  }

  virtual int Compare(const Slice& a, const Slice& b) const override {
    uint64_t a_id1, a_id2, b_id1, b_id2;
    const char *a_dat = a.data();
    const char *b_dat = b.data();
    
    a_id1 = *(uint64_t*)(&a_dat[cmp_prefix_offset]);
    b_id1 = *(uint64_t*)(&b_dat[cmp_prefix_offset]);
    if (a_id1 < b_id1) return -1;
    if (a_id1 > b_id1) return 1;
    
    a_id2 = *(uint64_t*)(&a_dat[cmp_prefix_offset+sizeof(uint64_t)]);
    b_id2 = *(uint64_t*)(&b_dat[cmp_prefix_offset+sizeof(uint64_t)]);
    if (a_id2 < b_id2) return -1;
    if (a_id2 > b_id2) return 1;
    
    return 0;
  }

  virtual const char* Name() const override { return "doubleuint64comparator_t"; }

  // No-ops since the C binding does not support key shortening methods.
  virtual void FindShortestSeparator(std::string*,
                                     const Slice&) const override {}
  virtual void FindShortSuccessor(std::string* key) const override {}
};

doubleuint64comparator_t * doubleuint64comparator_new(size_t cmp_prefix_offset) {
  doubleuint64comparator_t* cmp =  new doubleuint64comparator_t;
  cmp->cmp_prefix_offset = cmp_prefix_offset;
  return cmp;
};


struct reversesingleuint64comparator_t : public Comparator {
  void* state_;
  void (*destructor_)(void*);
  int (*compare_)(
      void*,
      const char* a, size_t alen,
      const char* b, size_t blen);
  const char* (*name_)(void*);
  size_t cmp_prefix_offset;

  virtual ~reversesingleuint64comparator_t() {

  }

  virtual int Compare(const Slice& a, const Slice& b) const override {
    uint64_t a_id1, b_id1;    
    const char *a_dat = a.data();
    const char *b_dat = b.data();

    a_id1 = *(uint64_t*)(&a_dat[cmp_prefix_offset]);
    b_id1 = *(uint64_t*)(&b_dat[cmp_prefix_offset]);
    if (a_id1 < b_id1) return 1;
    if (a_id1 > b_id1) return -1;
    
    return 0;
  }

  virtual const char* Name() const override { return "reversesingleuint64comparator_t"; }

  // No-ops since the C binding does not support key shortening methods.
  virtual void FindShortestSeparator(std::string*,
                                     const Slice&) const override {}
  virtual void FindShortSuccessor(std::string* key) const override {}
};

reversesingleuint64comparator_t * reversesingleuint64comparator_new(size_t cmp_prefix_offset) {
  reversesingleuint64comparator_t* cmp =  new reversesingleuint64comparator_t;
  cmp->cmp_prefix_offset = cmp_prefix_offset;
  return cmp;
};


}