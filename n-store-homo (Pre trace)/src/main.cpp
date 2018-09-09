// TESTER
//RAC: removed options 't' and 'o', tpcc related.
#include <cassert>

#include "config.h"
#include "utils.h"
#include "libpm.h"
#include "coordinator.h"

namespace storage {

  extern struct static_info *sp;  // global persistent memory structure
  int level = 2;  // verbosity level
  extern bool pm_stats;
  bool conflict = false; //Prevent both build log and rebuild from log

  static void usage_exit(FILE *out) {
    fprintf(out, "Command line options : nstore <options> \n"
            "   -h --help              :  Print help message \n"
            "   -x --num-txns          :  Number of transactions \n"
            "   -k --num-keys          :  Number of keys \n"
            "   -e --num-executors     :  Number of executors \n"
            "   -f --fs-path           :  Path for FS \n"
            "   -g --gc-interval       :  Group commit interval \n"
            "   -a --wal-enable        :  WAL enable (traditional) \n"
            "   -w --opt-wal-enable    :  OPT WAL enable \n"
            "   -s --sp-enable         :  SP enable (traditional) \n"
            "   -c --opt-sp-enable     :  OPT SP enable \n"
            "   -m --lsm-enable        :  LSM enable (traditional) \n"
            "   -l --opt-lsm-enable    :  OPT LSM enable \n"
            "   -y --ycsb              :  YCSB benchmark \n"
            "   -d --test              :  TEST benchmark \n"
            "   -p --per-writes        :  Percent of writes \n"
            "   -u --ycsb-update-one   :  Update one field \n"
            "   -q --ycsb_zipf_skew    :  Zipf Skew \n"
            "   -z --record_cur        :  Disable log recording \n"
            "   -r --recovery          :  Recovery mode \n"
            "   -b --load-batch-size   :  Load batch size \n"
            "   -j --test_b_mode       :  Test benchmark mode \n"
            "   -n --record-masterlog  :  Recording ops \n" //RAC
            "   -t --rebuild-table     :  Rebuilding table from MasterLog.txt \n" //RAC
            "   -i --multi-executors   :  Multiple executors \n");
    exit(EXIT_FAILURE);
  }

  static struct option opts[] = {
    { "log-enable", no_argument, NULL, 'a' },
    { "sp-enable", no_argument, NULL, 's' },
    { "lsm-enable", no_argument, NULL, 'm' },
    { "opt-wal-enable", no_argument, NULL, 'w' },
    { "opt-sp-enable", no_argument, NULL, 'c' },
    { "opt-lsm-enable", no_argument, NULL, 'l' },
    { "test", no_argument, NULL, 'd' },
    { "ycsb", no_argument, NULL, 'y' },
    { "tpcc", no_argument, NULL, 't' },
    { "fs-path", optional_argument, NULL, 'f' },
    { "num-txns", optional_argument, NULL, 'x' },
    { "num-keys", optional_argument, NULL, 'k' },
    { "num-executors", optional_argument, NULL, 'e' },
    { "ycsb_per_writes", optional_argument, NULL, 'w' },
    { "ycsb_skew", optional_argument, NULL, 'q' },
    { "gc-interval", optional_argument, NULL, 'g' },
    { "verbose", no_argument, NULL, 'v' },
    { "help", no_argument, NULL, 'h' },
    { "test-mode", optional_argument, NULL, 'j' },
    { "ycsb-update-one", no_argument, NULL, 'u' },
    { "record-masterlog", no_argument, NULL, 'n' },//RAC
    { "rebuild-table", no_argument, NULL, 't' },//RAC
    { NULL, 0, NULL, 0 } };

  static void parse_arguments(int argc, char* argv[], config& state) {

    // Default Values
    state.fs_path = std::string("/mnt/pmfs/n-store/");

    state.num_keys = 10;
    state.num_txns = 10;

    state.single = false;
    state.num_executors = 8;

    state.verbose = false;

    state.gc_interval = 5;
    state.ycsb_per_writes = 0.5;

    state.merge_interval = 10000;
    state.merge_ratio = 0.05;

    state.etype = engine_type::WAL;
    state.btype = benchmark_type::YCSB;

    state.read_only = false;
    state.recovery = false;

    state.ycsb_skew = 0.1;
    state.ycsb_update_one = false;
    state.ycsb_field_size = 100;
    state.ycsb_tuples_per_txn = 50;
    state.ycsb_num_val_fields = 1; //number of fields in db;

    state.tpcc_num_warehouses = 2;
    state.tpcc_stock_level_only = false;

    state.active_txn_threshold = 10;
    state.load_batch_size = 50000;
    state.storage_stats = false;

    
    state.test_benchmark_mode = 0;

    // Parse args
    while (1) {
      int idx = 0;
      int c = getopt_long(argc, argv, "f:x:k:e:p:g:q:b:U:M:j:svwascmhludyzorint", opts,
                          &idx);

      if (c == -1)
        break;

      switch (c) {
      case 'f':
        state.fs_path = std::string(optarg);
        std::cout << "fs_path: " << state.fs_path << std::endl;
        break;
      case 'x':
        state.num_txns = atoi(optarg);
        std::cout << "num_txns: " << state.num_txns << std::endl;
        break;
      case 'k':
        state.num_keys = atoi(optarg);
        std::cout << "num_keys: " << state.num_keys << std::endl;
        break;
      case 'e':
        state.num_executors = atoi(optarg);
        std::cout << "num_executors: " << state.num_executors << std::endl;
        break;
      case 'v':
        state.verbose = true;
        level = 3;
        break;
      case 'p':
        state.ycsb_per_writes = atof(optarg);
        assert(state.ycsb_per_writes >= 0 && state.ycsb_per_writes <= 1);

        if (state.ycsb_per_writes == 0)
          state.read_only = true;
        std::cout << "per_writes: " << state.ycsb_per_writes << std::endl;
        break;
      case 'g':
        state.gc_interval = atoi(optarg);
        std::cout << "gc_interval: " << state.gc_interval << std::endl;
        break;
      case 'a':
        state.etype = engine_type::WAL;
        std::cout << "wal_enable: " << std::endl;
        break;
      case 'w':
        state.etype = engine_type::OPT_WAL;
        std::cout << "opt_wal_enable " << std::endl;
        break;
      case 's':
        state.etype = engine_type::SP;
        std::cout << "sp_enable  " << std::endl;
        break;
      case 'c':
        state.etype = engine_type::OPT_SP;
        std::cout << "opt_sp_enable " << std::endl;
        break;
      case 'm':
        state.etype = engine_type::LSM;
        std::cout << "lsm_enable " << std::endl;
        break;
      case 'l':
        state.etype = engine_type::OPT_LSM;
        std::cout << "opt_lsm_enable " << std::endl;
        break;
      case 'd':
        state.btype = benchmark_type::TEST;
        std::cout << "test_benchmark " << std::endl;
        break;
      case 'y':
        state.btype = benchmark_type::YCSB;
        std::cout << "ycsb_benchmark " << std::endl;
        break;
      case 'j':
        state.test_benchmark_mode = atoi(optarg);
        std::cout << "test_benchmark_mode: " << state.test_benchmark_mode << std::endl;
        break;
      case 'q':
        state.ycsb_skew = atof(optarg);
        std::cout << "skew: " << state.ycsb_skew << std::endl;
        break;
      case 'u':
        state.ycsb_update_one = true;
        std::cout << "ycsb_update_one " << std::endl;
        break;
      case 'z'://RAC
        state.record_cur = false;
        std::cout << "Log recording off " << std::endl;
        break;
      case 'r':
        state.recovery = true;
        std::cout << "recovery " << std::endl;
        break;
      case 'b':
        state.load_batch_size = atoi(optarg);
        std::cout << "load_batch_size: " << state.load_batch_size << std::endl;
        break;
      case 'U':
	state.break_on_update = true;
	state.break_on_at = atoi(optarg);
        break;
      case 'M':
	state.break_macro = true;
	state.macro_break_type = atoi(optarg);
        break;
      case 'h':
        usage_exit(stderr);
        break;
      case 'n': //RAC
        if(conflict) //mutually exclusive
	{
	  std::cout << "Mutually Exclusive ops: 't' and 'n'" << std::endl;
	  exit (EXIT_FAILURE);
	}
        std::cout << "Recording Operations in MasterLog.txt" << std::endl;
	state.record_masterLog = true;
        conflict = true;
        break;
       case 't': //RAC
        if(conflict)//mutually exclusive
	{
	  std::cout << "Mutually Exclusive ops: 't' and 'n'" << std::endl;
	  exit (EXIT_FAILURE);
 	}
        std::cout << "Rebuilding table from MasterLog.txt" << std::endl;
	state.rebuild_table = true;
        conflict = true;
        break;
      default:
        fprintf(stderr, "\nUnknown option: -%c-\n", c);
        usage_exit(stderr);
      }
    }
  }

}

int main(int argc, char **argv) {
  const char* path = "/dev/shm/zfile";

  size_t pmp_size = PMSIZE;
  if ((storage::pmp = storage::pmemalloc_init(path, pmp_size)) == NULL)
    std::cout << "pmemalloc_init on :" << path << std::endl;

  storage::sp = (storage::static_info *) storage::pmemalloc_static_area();

// Start
  storage::config state;
  parse_arguments(argc, argv, state);
  state.sp = storage::sp;

  storage::coordinator cc(state);
  cc.eval(state);

  //std::cout<<"STATS : "<<std::endl;
  //std::cout<<"PCOMMIT : "<<storage::pcommit<<std::endl;
  //std::cout<<"CLFLUSH : "<<storage::clflush<<std::endl;

  return 0;
}
