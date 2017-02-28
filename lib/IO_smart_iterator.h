#include "util.h"
#include "comm.h"
#include "cache_driver.h"
#include "circle.h"

#ifndef __ITERATOR__
#define __ITERATOR__
typedef bool (*cb_func)(index_t, sa_t, sa_t*, sa_t *);

class IO_smart_iterator
{
	public:
		cache_driver *cd;
		int fd_csr;

		#ifdef TIMING
		std::stringstream loadss,issuess,compss,sortss;
		#endif
		
		//Split IO and computation thread
		index_t ring_vert_count;
		long buff_max_vert;
		sa_t **buff_source;
		vertex_t **buff_src_vert;
		vertex_t **buff_dest;
		index_t num_buffs;
		circle *circ_free_buff;
		circle *circ_load_buff;

		index_t *buff_edge_count;
		
		volatile int is_io_done;
		volatile int is_bsp_done;
		
		//for sorting
		comp_t *key_h;
		sa_t *val_h;
		long num_elements;
		comp_t **keys_d;
		sa_t **vals_d;
		int *semaphore_acq;
		int *semaphore_flag;
		long sort_criterion;
		long MAX_NUM_ELEMENTS;
		int tm_out;
		int success_sort;
		int sort_req;

		//for IO loading
		index_t total_blks;
		bit_t *reqt_blk_bitmap;
		vertex_t *blk_beg_vert;
		index_t vert_per_chunk;
		index_t reqt_blk_count; // number of request to be issued
		index_t VERT_PER_BLK;
		index_t *reqt_list;
		double io_time;
		double wait_io_time;
		double wait_comp_time;

		//for user layer
		sa_t my_level;
		bool io_conserve;
		sa_t *sa_ptr;
		sa_t *sa_prev;
		index_t *beg_pos_ptr;
		vertex_t **front_queue;
		index_t *front_count;

		int num_rows;
		int num_cols;

		int comp_tid; //my thread id.
		int my_col, my_row;

		//beg and end vert_id worked by this thread
		vertex_t row_ranger_beg, row_ranger_end;
		vertex_t col_ranger_beg, col_ranger_end;
		vertex_t *col_ranger;
		cb_func is_active;

	public:
		IO_smart_iterator(){};
		
		//PageRank
		IO_smart_iterator(
				bool is_pagerank,
				vertex_t** &front_queue_ptr,
				index_t* &front_count_ptr,
				vertex_t* &col_ranger_ptr,
				const int tid, index_t *comm,
				const int num_rows,
				const int num_cols, 
				const char *beg_dir,
				const char *csr_dir,
				const char *beg_header,
				const char *csr_header,
				const index_t num_chunks,
				const size_t chunk_sz,
				sa_t * &sa,sa_t* &sa_prev,
				index_t* &beg_pos, 
				index_t num_buffs,
				index_t ring_vert_count,
				index_t MAX_USELESS,
				const index_t io_limit,
				cb_func p_func);
		
		//split IO and computation thread
		IO_smart_iterator(
				vertex_t** &front_queue_ptr,
				index_t* &front_count_ptr,
				vertex_t* &col_ranger_ptr,
				const int tid, index_t *comm,
				const int num_rows,
				const int num_cols, 
				const char *beg_dir,
				const char *csr_dir,
				const char *beg_header,
				const char *csr_header,
				const index_t num_chunks,
				const size_t chunk_sz,
				sa_t * &sa,sa_t* &sa_prev,
				index_t* &beg_pos, 
				index_t num_buffs,
				index_t ring_vert_count,
				index_t MAX_USELESS,
				const index_t io_limit,
				cb_func p_func);
		
		//Base constructor
		IO_smart_iterator(
				const int tid, index_t *comm,
				const int num_rows,
				const int num_cols, 
				const char *beg_dir,
				const char *csr_dir,
				const char *beg_header,
				const char *csr_header,
				const index_t num_chunks,
				const size_t chunk_sz,
				sa_t * &sa,sa_t* &sa_prev,
				index_t* &beg_pos, 
				index_t MAX_USELESS,
				const index_t io_limit,
				cb_func p_func);

		~IO_smart_iterator();

	public:
		void next();
		int next(int used_buff);
		void load_kv_sa(sa_t criterion);	
		void load_kv_vert(sa_t criterion);	
		void load_key(sa_t criterion);	
		void load_key_iolist(sa_t criterion);	
		void load_kv_vert_full(sa_t criterion);	

		//No sorting, put source - dest in list
		comp_t *next_nosort_kv(sa_t criterion);

		//cpu sort key
		comp_t *next_cpu_key(sa_t criterion);

		//GPU sort key/kv with B40C
		comp_t *next_gpu_key(sa_t criterion);
		comp_t *next_gpu_kv(sa_t criterion);
	
		void read_trace_to_list(char *trace_file);
		void read_trace_to_bitmap(char *trace_file);
		void req_translator(sa_t criterion);
		void req_translator_queue();
		void req_sort_gpu();
		void front_sort_cpu();
		void req_convert_list();

		void init_sort_key(	long &num_elements,
				sa_t criterion);

		void init_sort_kv(	long &num_elements,
				sa_t criterion);

		bool LOCK(int *val, int tmout);
		void priority_queue (int *acq_seq, int *seq_flag);
};

#endif

