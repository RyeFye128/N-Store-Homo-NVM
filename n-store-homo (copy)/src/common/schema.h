#pragma once

#include "libpm.h"
#include "field.h"

#include <iostream>
#include <vector>
#include <iomanip>

namespace storage {

class schema {
 public:
  schema(std::vector<field_info> _columns)
      : columns(NULL),
        ser_len(0),
        deser_len(0){

    num_columns = _columns.size();
    columns = (field_info*) pmalloc(num_columns*(sizeof(field_info)));//new field_info[num_columns];
    unsigned int itr;

    for (itr = 0; itr < num_columns; itr++) {
      columns[itr] = _columns[itr];
      ser_len += columns[itr].ser_len;
      deser_len += columns[itr].deser_len;
    }

    pmemalloc_activate(columns);
  }

  ~schema() {
    delete[] columns;
  }

  void display() {
    unsigned int itr;

    for (itr = 0; itr < num_columns; itr++) {
      std::cout << std::setw(20);
      std::cout << "offset    : " << columns[itr].offset << " ";
      std::cout << "ser_len   : " << columns[itr].ser_len << " ";
      std::cout << "deser_len : " << columns[itr].deser_len << " ";
      std::cout << "type      : " << (int) columns[itr].type << " ";
      std::cout << "inlined   : " << (int) columns[itr].inlined << " ";
      std::cout << "enabled   : " << (int) columns[itr].enabled << " ";
      std::cout << "\n";
    }

    std::cout << "\n";
  }

  field_info* columns;
  size_t ser_len;
  size_t deser_len;
  unsigned int num_columns;
};

}

