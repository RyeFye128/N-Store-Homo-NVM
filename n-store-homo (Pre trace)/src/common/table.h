#pragma once

#include "schema.h"
#include "table_index.h"
#include "plist.h"
#include "storage.h"

namespace storage {

class table {
 public:
  table(const std::string& name_str, schema* _sptr, unsigned int _num_indices,
        __attribute__((unused)) config& conf, struct static_info* sp)
      : table_name(NULL),
        sptr(_sptr),
        max_tuple_size(_sptr->deser_len),
        num_indices(_num_indices),
        indices(NULL),
        pm_data(NULL) {

    size_t len = name_str.size();
    table_name = (char*) pmalloc((len+1)*sizeof(char));//new char[len + 1];
    memcpy(table_name, name_str.c_str(), len + 1);
    pmemalloc_activate(table_name);

    pm_data = new ((plist<record*>*) pmalloc(sizeof(plist<record*>))) plist<record*>(&sp->ptrs[get_next_pp()], &sp->ptrs[get_next_pp()]);
    pmemalloc_activate(pm_data);

    indices = new ((plist<table_index*>*) pmalloc(sizeof(plist<table_index*>))) plist<table_index*>(&sp->ptrs[get_next_pp()],
                                      					&sp->ptrs[get_next_pp()]);
    pmemalloc_activate(indices);
   

  }

  ~table() {
    delete table_name;
    delete sptr;

    if (indices != NULL) {
      // clean up table indices
      std::vector<table_index*> index_vec = indices->get_data();
      for (table_index* index : index_vec)
        delete index;

      delete indices;
    }
  }

 
  //private:
  char* table_name;
  schema* sptr;
  size_t max_tuple_size;
  unsigned int num_indices;

  plist<table_index*>* indices;

  storage fs_data;

  plist<record*>* pm_data;
 


};



}

