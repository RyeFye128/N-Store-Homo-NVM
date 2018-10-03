// YCSB BENCHMARK

#include "ycsb_benchmark.h"
#include <ctime>

#include <iostream>//rac
#include <fstream>//rac

int reads, updates, records;//rac

std::ofstream recorder("currentLog.txt");//log per instance, rac


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
  //std::cout << "Persistent memory pool starts at: " << std::addressof(ee->pmp) << std::endl;
  ee->txn_begin();
  //iterate through all the keys in the database
  if(conf.rebuild_table)
	{
		rebuild_table(usertable_schema, 0, ee);
	}
  else{
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
	    std::cout << "Creating record with value " << value << std::endl;
	    record* rec_ptr = new ((record*) pmalloc(sizeof(usertable_record))) usertable_record(usertable_schema, key, value,
		                          					conf.ycsb_num_val_fields, false);
	    
	    //CALL SIM CRASH HERE. Record is created with a ptr, but not committed.
	    statement st(txn_id, operation_type::Insert, USER_TABLE_ID, rec_ptr);//INSERTION OPERATION
	    //std::cout << "New record is ";
	    //rec_ptr->display();
	    ee->load(st);
	    records++;
	    if(conf.record_cur)
	    {
	    	recorder << "Load," + std::to_string(key) + "," + value[0] + "," << std::endl;
	    }
	    //sim_crash();
	    if (tid == 0)
	      ss.display();
	  }
	
  std::cout << conf.ycsb_num_val_fields << " Fields in this " << std::endl;//prints the number of ???
  ee->txn_end(true);
 
  delete ee;}
}
/* Updates records equal to the amount of tuples allowed per transaction.
*/
void ycsb_benchmark::do_update(engine* ee) {

// UPDATE
  std::string updated_val(conf.ycsb_field_size, 'u');
  int zipf_dist_offset = txn_id * conf.ycsb_tuples_per_txn;
  txn_id++;
  int rc;

  TIMER(ee->txn_begin());

  for (int stmt_itr = 0; stmt_itr < conf.ycsb_tuples_per_txn; stmt_itr++) {

    int key = zipf_dist[zipf_dist_offset + stmt_itr];

    record* rec_ptr = new ((record*) pmalloc(sizeof(usertable_record))) usertable_record(user_table_schema, key, updated_val,
                                           					conf.ycsb_num_val_fields,
                                           					conf.ycsb_update_one);

   
    statement st(txn_id, operation_type::Update, USER_TABLE_ID, rec_ptr,
                 update_field_ids);
   //std::cout << "Change record at position " << key << " with val " << updated_val <<  std::endl;
    TIMER(rc = ee->update(st))
    if (rc != 0) {
      TIMER(ee->txn_end(false));
      return;
    }
   // std::cout << "Updating some stuff...";
    updates++;
    if(conf.record_cur)
    {
   	 recorder << "Update," + std::to_string(key) + "," + updated_val[0] + "," << std::endl;
    }
    //rec_ptr->display();
  }
  //std::cout << "Done updating some stuff...";
  TIMER(ee->txn_end(true));
  //insert recorder for UPDATE (type depends on statement)
  
  
}

void ycsb_benchmark::do_read(engine* ee) {

// SELECT
  
  int zipf_dist_offset = txn_id * conf.ycsb_tuples_per_txn;
  txn_id++;
  std::string empty;
  

  TIMER(ee->txn_begin());
  std::string rc("default");
  for (int stmt_itr = 0; stmt_itr < conf.ycsb_tuples_per_txn; stmt_itr++) {

    int key = zipf_dist[zipf_dist_offset + stmt_itr];
    //if( key > 100) std::cout << "Invalid key " << std::to_string(key) << std::endl;

    record* rec_ptr = new usertable_record(user_table_schema, key, empty,
                                           conf.ycsb_num_val_fields, false);
    
    statement st(txn_id, operation_type::Select, USER_TABLE_ID, rec_ptr, 0,
                 user_table_schema);
   // std::cout << "Reading some stuff...";
    reads++;
    //rec_ptr->display();
    // Create string, store select value in and set timer RAC
    TIMER(rc=ee->select(st));
    if(conf.record_cur)
    {
     	recorder << "Read," + std::to_string(key) + "," + rc[3] + "," << std::endl;
    }
  }
  //std::cout << "Done reading stuff...";
  TIMER(ee->txn_end(true));
  //insert recorder here (READ)
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
//-----------------RAC code-------------------------------------------
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

void ycsb_benchmark::print_masterLog()
{
	std::string line;
	std::string cur_word("");
	int lines = 0;
	std::ifstream log("MasterLog.txt");
	if(log.is_open())//if the file is open
	{
		while(getline(log, line))//for each line in the log
		{
			for(unsigned int i = 0; i < line.length(); i++)//go through each char
			{
				
				if(line[i] == ',')
				{
					//std::cout << line[i] << std::endl;
					//std::cout << cur_word;
					cur_word = "";
					lines++;
					
				}
				else
				{
					cur_word+=line[i];
				}
				
					//std::cout << line[i];
			}
			//std::cout << cur_word << std::endl;
			cur_word = "";
		}
		std::cout << "Lines in file: " << lines/3 << std::endl;
		log.close();
	}
	
	
}
/*
  Check if the masterlog exists. If it does, then dont write to it - leave it alone. This signifies that
  the current run is intended to be used as the template for fault injection.
  If masterlog doesnt exist, then this is the "first" run that will be used to rebuild the table.
  TODO: See why we have txn_itr in the rebuild.
*/
bool ycsb_benchmark::existsMasterLog()
{
  std::ifstream ifile("MasterLog.txt");//see if handle is successful
  return (bool) ifile;
}

void ycsb_benchmark::rebuild_table(schema* usertable_schema, unsigned int txn_itr, engine* ee)
{
	std::cout << "Begin building table from text! " << txn_itr << std::endl;

	std::string line;
	std::string cur_word("");
	int word_num = 0;

	std::string operation;
	int key= -99;
	std::string value;
	int updates = 0;
        int inserts = 0;
	int break_here = conf.break_on_at;
	std::ifstream log("MasterLog.txt");
	if(log.is_open())//if the file is open
	{
		/*For each line, go through and set the variables:
		  operation: first word
		  key: and integer, the second word
		  value: Value of key-val pair.
		*/
		
		while(getline(log, line))//for each line in the log
		{
			
			word_num = 0;
			
			for(unsigned int i = 0; i < line.length(); i++)//go through each char
			{
				
				if(line[i] == ',')//end word
				{
					word_num++;
					switch(word_num)
					{
						case 1://op
						operation = cur_word;
						break;
						
						case 2://key
						key = std::stoi(cur_word); 
						break;

						case 3://value
						//value = cur_word;
						value = std::string(conf.ycsb_field_size, cur_word[0]);
						break;
					}

					cur_word = "";
									
				}
				else//keep building word, havent reached comma
					cur_word+=line[i];
			}
			
			//std::cout << "op " + operation + " key: " + std::to_string(key) + " val: " + value << std::endl;
			/* Begin building statements. We have the op, key, and value in variables. 
			   1. Get op type enum from statement.h, to be used in statement creation : DONE
			   2. Depending on op type, construct the needed variables
			   3. Execute statement with the built statement

			*/

			operation_type op = getOpType(operation);//Get enum op value
			
			/*Create the statement depending on the op type: in all cases, txn begin and end
			  1. Select: Created zipf var, txn_id++, create std::string empty.
			  2. Update: Created updated_val, zipf, txn_id++, generate key from zipf
			  3. Delte: not implemented
			*/

			std::string empty;
			std::string rc("default");
			int txn_itr = 0;//Dependent on keys. Update when inserting.
			//TODO:Add macro fault for select.
			switch(op)
			{
				case operation_type::Select:
				{	
  					txn_id++;
					TIMER(ee->txn_begin());
					record* rec_ptr = new usertable_record(user_table_schema, key, empty, conf.ycsb_num_val_fields, false);
					statement st(txn_id, op, USER_TABLE_ID, rec_ptr, 0, user_table_schema);
					TIMER(rc=ee->select(st));
					//std::cout << "Selecting based on built table: " << rc << std::endl;
					TIMER(ee->txn_end(true));
				}
				break;
				case operation_type::Delete:
				break;
				case operation_type::Insert:
				{
					inserts++;
					engine* ee1 = new engine(conf, tid, db, false);
					ee1->txn_begin();
					
					if (txn_itr % conf.load_batch_size == 0) 
					{
		      				ee1->txn_end(true);//clears the log
	      				      	txn_id++;
	      					ee1->txn_begin();
	 				}
					
					record* rec_ptr = new ((record*) pmalloc(sizeof(usertable_record))) usertable_record(usertable_schema, key, value, conf.ycsb_num_val_fields, false);
					//std::cout << "Record hex is " << rec_ptr << std::endl;
					statement st(txn_id, operation_type::Insert, USER_TABLE_ID, rec_ptr);
					ee1->load(st);
					/* Crash in the middle of a transaction, but after the insert is commited to the log. Should simply unroll and result in one less key.
					// If a key isnt added, then update the global key count 
					*/
					if(inserts == break_here && conf.break_on_insert && conf.break_macro && conf.macro_break_type == 1)
					{
						
						std::cout << "Breaking at Insert, position " << key << "with value: " << value << " type: mid transaction"<< std::endl;
						printTable(ee, "Before breaking");
						ee->recovery();
						printTable(ee, "After breaking");
						//conf.num_keys--;
						
					}
					ee1->txn_end(true);
					//std::cout << "Inserting record via table rebuilding: " << rec_ptr->display() << std::endl;
					
					if(inserts == conf.num_keys)
						printTable(ee, "Loaded Table");
  					delete ee1;
					//if (tid == 0)
	      					//ss.display();
				}
				break;
				case operation_type::Update:
				{
					updates++;
					std::string updated_val(conf.ycsb_field_size, value[0]);
					
					txn_id++;
					int rc;
					
					TIMER(ee->txn_begin());
					record* rec_ptr = new ((record*) pmalloc(sizeof(usertable_record))) usertable_record(user_table_schema, key, updated_val,
                                           					conf.ycsb_num_val_fields,
                                           					conf.ycsb_update_one);
					
					
					statement st(txn_id, operation_type::Update, USER_TABLE_ID, rec_ptr, update_field_ids);
					TIMER(rc = ee->update(st));
					//Type one break: an operation performed in between transactions
					if(updates == break_here && conf.break_on_update && conf.break_macro && conf.macro_break_type == 1)
					{
						
						std::cout << "Breaking at Update, position " << key << " type: mid transaction"<< std::endl;
						printTable(ee, "Before breaking");
						ee->recovery();
						printTable(ee, "After breaking");
						
					}
					if (rc != 0) {
					   TIMER(ee->txn_end(false));
					   return;
					 }
					TIMER(ee->txn_end(true));
				}
				break;
				default:
				break;
			}
			
			cur_word = "";//Reset for next line
			key+=0; //Fool compiler for now
		}
		
  	        
  
		log.close();
	}
	
	
}

/*	
								txn_id++;
								std::string empty;
								cur_word+="";
								operation_type op = operation_type::Select;
								int key = zipf_dist[zipf_dist_offset + stmt_itr];
								record* rec_ptr = new usertable_record(user_table_schema, key, empty, conf.ycsb_num_val_fields, false);	
					*/	

operation_type ycsb_benchmark::getOpType(std::string op)
{
	if(op == "Read")
		return operation_type::Select;
	if(op == "Update")
		return operation_type::Update;
	if(op == "Load")
		return operation_type::Insert;
	return operation_type::Select;
}
//--------------------RAC Code end------------------------------------

void ycsb_benchmark::execute() {
  engine* ee = new engine(conf, tid, db, conf.read_only);
  unsigned int txn_itr;
  status ss(num_txns);
  if(!conf.rebuild_table)
  {
  	printTable(ee, "Initial Table");
  }
	
  std::cout << "num_txns :::: " << num_txns << std::endl;
  //sim_crash();

  /* The below code handles random updates based on the dist. We only need this if recording.
  */
	if(!conf.rebuild_table)
	{
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
	}
  printTable(ee, "Table after benchmark complete");
  std::cout << "duration :: " << tm->duration() << std::endl;
  if(!conf.rebuild_table)
	std::cout << "Total reads: " << reads << " Total updates: " << updates << " Total Records: " << records <<  std::endl;
  print_masterLog();

  
  if(conf.record_masterLog && conf.record_cur)//Master log doesnt exist, or is empty, the currentlog is the master log
  { 
    rename("currentLog.txt", "MasterLog.txt");
  }
  
  recorder.close();

  delete ee;
}





}
