// YCSB BENCHMARK

#include "ycsb_benchmark.h"
#include <ctime>

int reads, updates, records;

namespace storage {



class usertable_record : public record {
 public:
  usertable_record(schema* _sptr, int key, const std::string& val,
                   int num_val_fields, bool update_one)
      : record(_sptr) {

    set_int(0, key);

    if (val.empty())
      return;

    if (1 || !update_one) {
      for (int itr = 1; itr <= num_val_fields; itr++) {
        set_varchar(itr, val);
      }
    } else
	die();
      set_varchar(1, val);
  }

};

// USERTABLE
table* create_usertable(config& conf) {

  std::vector<field_info> cols;
  off_t offset;

  offset = 0;
  field_info key(offset, 10, 10, field_type::INTEGER, 1, 1);
  offset += key.ser_len;
  cols.push_back(key);

  for (int itr = 1; itr <= conf.ycsb_num_val_fields; itr++) {
    field_info val = field_info(offset, 12, conf.ycsb_field_size,
                                field_type::VARCHAR, 0, 1);
    offset += val.ser_len;
    cols.push_back(val);
  }

  // SCHEMA
  schema* user_table_schema = new ((schema*) pmalloc(sizeof(schema))) schema(cols);
  pmemalloc_activate(user_table_schema);

  table* user_table = new ((table*) pmalloc(sizeof(table))) table("user", user_table_schema, 1, conf, sp);
  pmemalloc_activate(user_table);

  // PRIMARY INDEX
  for (int itr = 1; itr <= conf.ycsb_num_val_fields; itr++) {
    cols[itr].enabled = 0;
  }

  schema* user_table_index_schema = new ((schema*) pmalloc(sizeof(schema))) schema(cols);
  pmemalloc_activate(user_table_index_schema);

  table_index* key_index = new ((table_index*) pmalloc(sizeof(table_index))) table_index(user_table_index_schema,	\
                                           					conf.ycsb_num_val_fields + 1, conf,	\
                                           						sp);
  pmemalloc_activate(key_index);
  user_table->indices->push_back(key_index);

  return user_table;
}

ycsb_benchmark::ycsb_benchmark(config _conf, unsigned int tid, database* _db,
                               timer* _tm, struct static_info* _sp)
    : benchmark(tid, _db, _tm, _sp),
      conf(_conf),
      txn_id(0) {

  btype = benchmark_type::YCSB;

  // Partition workload
  num_keys = conf.num_keys / conf.num_executors;
  num_txns = conf.num_txns / conf.num_executors;

  // Initialization mode
  if (sp->init == 0) {
    //cout << "Initialization Mode" << endl;
    sp->ptrs[0] = _db;

    table* usertable = create_usertable(conf);
    db->tables->push_back(usertable);
    sp->init = 1;
  } else {
    //std::cout << "Recovery Moooooooooode " << std::endl;
    database* db = (database*) sp->ptrs[0]; // We are reusing old tables
    db->reset(conf, tid);
  }

  user_table_schema = db->tables->at(USER_TABLE_ID)->sptr;

  if (conf.recovery) {
    num_txns = conf.num_txns;
    num_keys = 1000;
    conf.ycsb_per_writes = 0.5;
    conf.ycsb_tuples_per_txn = 20;
  }

  if (conf.ycsb_update_one == false) {
    for (int itr = 1; itr <= conf.ycsb_num_val_fields; itr++)
      update_field_ids.push_back(itr);
  } else {
    // Update only first field
    update_field_ids.push_back(1);
  }

  // Generate skewed dist
  simple_skew(zipf_dist, conf.ycsb_skew, num_keys,
              num_txns * conf.ycsb_tuples_per_txn);
  uniform(uniform_dist, num_txns);

}

//Initial record insertion
void ycsb_benchmark::load() {
  engine* ee = new engine(conf, tid, db, false);
  
  schema* usertable_schema = db->tables->at(USER_TABLE_ID)->sptr;
  unsigned int txn_itr;
  status ss(num_keys);

  ee->txn_begin();
  //iterate through all the keys in the database
  for (txn_itr = 0; txn_itr < num_keys; txn_itr++) {
    //Time to commit the transaction?
    if (txn_itr % conf.load_batch_size == 0) {
      ee->txn_end(true);//clears the log
      
      txn_id++;
      ee->txn_begin();
    }

    // LOAD
    int key = txn_itr;
    //the value is a char (see utils.cpp) of size ycsb_field_size. not truely random
    std::string value = get_rand_astring(conf.ycsb_field_size);
    

     //std::cout << "New record key has a value of " << value << " and a size of " << value.size() << std::endl;
    //The record contains a numerical key, from 0 to n.
    record* rec_ptr = new ((record*) pmalloc(sizeof(usertable_record))) usertable_record(usertable_schema, key, value,
                                  					conf.ycsb_num_val_fields, false);
    
    //CALL SIM CRASH HERE. Record is created with a ptr, but not committed.
    statement st(txn_id, operation_type::Insert, USER_TABLE_ID, rec_ptr);//INSERTION OPERATION
    //std::cout << "New record is ";
    //rec_ptr->display();
    ee->load(st);
    records++;
    //sim_crash();
    if (tid == 0)
      ss.display();
  }
  std::cout << conf.ycsb_num_val_fields << " Fields in this " << std::endl;//prints the number of ???
  ee->txn_end(true);
  
  delete ee;
}
/* Updates records equal to the amount of tuples allowed per transaction.
*/
void ycsb_benchmark::do_update(engine* ee) {

// UPDATE
  std::string updated_val(conf.ycsb_field_size, 'x');
  int zipf_dist_offset = txn_id * conf.ycsb_tuples_per_txn;
  txn_id++;
  int rc;

  TIMER(ee->txn_begin())

  for (int stmt_itr = 0; stmt_itr < conf.ycsb_tuples_per_txn; stmt_itr++) {

    int key = zipf_dist[zipf_dist_offset + stmt_itr];

    record* rec_ptr = new ((record*) pmalloc(sizeof(usertable_record))) usertable_record(user_table_schema, key, updated_val,
                                           					conf.ycsb_num_val_fields,
                                           					conf.ycsb_update_one);

    
    statement st(txn_id, operation_type::Update, USER_TABLE_ID, rec_ptr,
                 update_field_ids);
    std::cout << "Change record at position " << key << std::endl;
    TIMER(rc = ee->update(st))
    if (rc != 0) {
      TIMER(ee->txn_end(false))
      return;
    }
   // std::cout << "Updating some stuff...";
    updates++;
    //rec_ptr->display();
  }
  //std::cout << "Done updating some stuff...";
  TIMER(ee->txn_end(true))
}

void ycsb_benchmark::do_read(engine* ee) {

// SELECT
  
  int zipf_dist_offset = txn_id * conf.ycsb_tuples_per_txn;
  txn_id++;
  std::string empty;
  std::string rc;

  TIMER(ee->txn_begin())

  for (int stmt_itr = 0; stmt_itr < conf.ycsb_tuples_per_txn; stmt_itr++) {

    int key = zipf_dist[zipf_dist_offset + stmt_itr];

    record* rec_ptr = new usertable_record(user_table_schema, key, empty,
                                           conf.ycsb_num_val_fields, false);
    
    statement st(txn_id, operation_type::Select, USER_TABLE_ID, rec_ptr, 0,
                 user_table_schema);
   // std::cout << "Reading some stuff...";
    reads++;
    //rec_ptr->display();
    TIMER(ee->select(st))
  }
  //std::cout << "Done reading stuff...";
  TIMER(ee->txn_end(true))
}

void ycsb_benchmark::sim_crash() {
  
  engine* ee = new engine(conf, tid, db, conf.read_only);
  unsigned int txn_itr;

  // UPDATE
  std::vector<int> field_ids;

//Takes a vector and appends 1,2,3,4,5.....number of ???
//ends with 1,2,3,4...n
  for (int itr = 1; itr <= conf.ycsb_num_val_fields; itr++) {
    field_ids.push_back(itr);
     //fstd::cout << "Vector contains, at the start: " << field_ids.front() << " at the end: " << field_ids.back() << std::endl;
  }

//create a string, and add # x's equivalent to the field size.
  std::string updated_val(conf.ycsb_field_size, 'x');// (int, char)
  int zipf_dist_offset = 0;
  
  // No recovery needed
  if (conf.etype == engine_type::SP || conf.etype == engine_type::OPT_SP) {
    ee->recovery();//why run it then?
    return;
  }

  // Always in sync
  if (conf.etype == engine_type::OPT_WAL || conf.etype == engine_type::OPT_LSM)
    num_txns = 1;//number of transactions. 1 because WAL must undo 1 txn?

  ee->txn_begin();//empty function, verified.

  for (txn_itr = 0; txn_itr < num_txns; txn_itr++) {
//starting at 0, for all transactons...
    for (int stmt_itr = 0; stmt_itr < conf.ycsb_tuples_per_txn; stmt_itr++) {
//Go through all tuples per txn

      int key = zipf_dist[zipf_dist_offset + stmt_itr];
      //std::cout << "insert bad record at position " << key << std::endl;
      //create a new record, same as constructor but with updated_val
      //Inserts updated_val into each field
      record* rec_ptr = new usertable_record(user_table_schema, key,
                                             updated_val,
                                             conf.ycsb_num_val_fields,
                                             conf.ycsb_update_one);
      

      statement st(txn_id, operation_type::Update, USER_TABLE_ID, rec_ptr,
                   field_ids);
    
      //std::cout << "Updating field with id " << field_ids
      ee->update(st);
      //std::cout << "Record before recovery is ";
    //rec_ptr->display();
    
    }
  }
  
  // Recover
  printTable(ee, "About to recover");
  ee->recovery();
  delete ee;
}
void ycsb_benchmark::printTable(engine* ee, std::string message)
{
  std::cout <<"Printing table..." << message << std::endl;
   std::string empty;//what does it do?
  for(unsigned int i = 0; i <= num_keys; i++)
  {
    record* rec_ptr = new usertable_record(user_table_schema, i, empty,
                                           conf.ycsb_num_val_fields, false);
    statement st(txn_id, operation_type::Select, USER_TABLE_ID, rec_ptr, 0,
                 user_table_schema);
    std::string val = ee->select(st);
    std::cout << val.c_str() << std::endl;
  }
}

void ycsb_benchmark::execute() {
  engine* ee = new engine(conf, tid, db, conf.read_only);
  unsigned int txn_itr;
  status ss(num_txns);
  printTable(ee, "Initial Table");
  std::cout << "num_txns :::: " << num_txns << std::endl;
  //sim_crash();

  for (txn_itr = 0; txn_itr < num_txns; txn_itr++) {
    double u = uniform_dist[txn_itr];
   // std::cout << "U is " << u << std::endl;
    //std::cout << txn_itr << std::endl;
    //std::cout << num_txns << std::endl;
    if (u >= conf.ycsb_per_writes) {
      do_update(ee);//alters records
    } else {
      do_read(ee);
    }

    if (tid == 0)
      ss.display();
  }
  printTable(ee, "post recovery");
  std::cout << "duration :: " << tm->duration() << std::endl;
  std::cout << "Total reads: " << reads << " Total updates: " << updates << " Total Records: " << records <<  std::endl;
  delete ee;
}





}
