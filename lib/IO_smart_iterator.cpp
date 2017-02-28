#include "IO_smart_iterator.h"
#include <unistd.h>
#include <asm/mman.h>
#include <omp.h>
//PageRank requires buff_source and buff_dest 
//have same type as vertex_t. 
IO_smart_iterator::IO_smart_iterator(
		bool is_pagerank,
		vertex_t** &front_queue_ptr,
		index_t* &front_count_ptr,
		vertex_t* &col_ranger_ptr,
		const int comp_tid, index_t *comm,
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
		cb_func p_func):
	IO_smart_iterator(comp_tid, comm, num_rows, num_cols, beg_dir, csr_dir,
			beg_header, csr_header, num_chunks, chunk_sz, sa, sa_prev, 
			beg_pos, MAX_USELESS, io_limit, p_func)
{
	//reqt_list
	reqt_list = (index_t *)mmap(NULL, sizeof(index_t) * total_blks * 10,
	//reqt_list = (index_t *)mmap(NULL, sizeof(index_t) * 33554432,//FOR FRIENDSTER/TWITTER
			PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS 
			| MAP_HUGETLB | MAP_HUGE_2MB, 0, 0);	
	
	assert(reqt_list != MAP_FAILED);
	//assert(sizeof(bit_t) == 1);
	//memset(reqt_blk_bitmap, 256, ((total_blks>>3)+1) * sizeof(bit_t));

	buff_max_vert = ring_vert_count;
	this->num_buffs = num_buffs;

	buff_src_vert = new vertex_t*[num_buffs];
	buff_dest = new vertex_t*[num_buffs];
	buff_edge_count = new index_t[num_buffs];
	
	is_bsp_done = false;
	is_io_done = false;

//	circ_free_buff = new circle(num_buffs);
//	circ_load_buff = new circle(num_buffs);
//	circ_free_buff -> reset_circle();
//	circ_load_buff -> reset_circle();
//	for(int i = 0; i < num_buffs; i ++)
//	{
//		buff_src_vert[i] = (vertex_t *)mmap(NULL, 
//				ring_vert_count*sizeof(vertex_t),
//				PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS 
//				| MAP_HUGETLB | MAP_HUGE_2MB, 0, 0);
//		buff_dest[i] = (vertex_t *)mmap(NULL, 
//				ring_vert_count*sizeof(vertex_t),
//				PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS 
//				| MAP_HUGETLB | MAP_HUGE_2MB, 0, 0);
//		if(buff_src_vert[i]==MAP_FAILED ||
//				buff_dest[i] == MAP_FAILED)
//			perror("buff_source buff_dest mmap");
//		
//		buff_edge_count[i] = 0;
//		circ_free_buff->en_circle(i);
//	}
	
	front_count = front_count_ptr;
	front_queue = front_queue_ptr;
	col_ranger = col_ranger_ptr;

	col_ranger_beg = col_ranger[my_row * (num_cols + 1) + my_col];
	col_ranger_end = col_ranger[my_row * (num_cols + 1) + my_col + 1];

	front_queue[comp_tid] = new vertex_t[col_ranger_end - col_ranger_beg];
	
	if(omp_get_thread_num()==0)std::cout<<"Before cd init\n";

	//allocate cache driver
	cd = new cache_driver(
			fd_csr, 
			reqt_blk_bitmap,
			reqt_list,
			&reqt_blk_count,
			total_blks,
			blk_beg_vert,
			&io_conserve,
			num_chunks, 
			chunk_sz, 
			io_limit,MAX_USELESS);
	if(omp_get_thread_num()==0)std::cout<<"Finished cd init\n";
}

IO_smart_iterator::IO_smart_iterator(
		vertex_t** &front_queue_ptr,
		index_t* &front_count_ptr,
		vertex_t* &col_ranger_ptr,
		const int comp_tid, index_t *comm,
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
		cb_func p_func):
	IO_smart_iterator(comp_tid, comm, num_rows, num_cols, beg_dir, csr_dir,
			beg_header, csr_header, num_chunks, chunk_sz, sa, sa_prev, 
			beg_pos, MAX_USELESS, io_limit, p_func)
{
	//reqt_list
	reqt_list = (index_t *)mmap(NULL, sizeof(index_t) * total_blks * 4,
	//reqt_list = (index_t *)mmap(NULL, sizeof(index_t) * 33554432,//FOR FRIENDSTER/TWITTER
			PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS 
			| MAP_HUGETLB | MAP_HUGE_2MB, 0, 0);	

	assert(reqt_list != MAP_FAILED);
	buff_max_vert = ring_vert_count;
	this->num_buffs = num_buffs;

	buff_source = new sa_t*[num_buffs];
	buff_dest = new vertex_t*[num_buffs];
	buff_edge_count = new index_t[num_buffs];
	is_bsp_done = false;
	is_io_done = false;
	
//	circ_free_buff = new circle(num_buffs);
//	circ_load_buff = new circle(num_buffs);
//	circ_free_buff -> reset_circle();
//	circ_load_buff -> reset_circle();
//	for(int i = 0; i < num_buffs; i ++)
//	{
//		buff_source[i] = (sa_t *)mmap(NULL, 
//				ring_vert_count*sizeof(sa_t),
//				PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS 
//				| MAP_HUGETLB | MAP_HUGE_2MB, 0, 0);
//		buff_dest[i] = (vertex_t *)mmap(NULL, 
//				ring_vert_count*sizeof(vertex_t),
//				PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS 
//				| MAP_HUGETLB | MAP_HUGE_2MB, 0, 0);
//
//		if(buff_source[i]==MAP_FAILED ||
//				buff_dest[i] == MAP_FAILED)
//			perror("buff_source buff_dest mmap");
//		
//		buff_edge_count[i] = 0;
//		circ_free_buff->en_circle(i);
//	}
	
	front_count = front_count_ptr;
	front_queue = front_queue_ptr;
	col_ranger = col_ranger_ptr;

	col_ranger_beg = col_ranger[my_row * (num_cols + 1) + my_col];
	col_ranger_end = col_ranger[my_row * (num_cols + 1) + my_col + 1];

	front_queue[comp_tid] = new vertex_t[col_ranger_end - col_ranger_beg];
	
	if(omp_get_thread_num()==0)std::cout<<"Before cd init\n";

	//allocate cache driver
	cd = new cache_driver(
			fd_csr, 
			reqt_blk_bitmap,
			reqt_list,
			&reqt_blk_count,
			total_blks,
			blk_beg_vert,
			&io_conserve,
			num_chunks, 
			chunk_sz, 
			io_limit,MAX_USELESS);
	if(omp_get_thread_num()==0)std::cout<<"Finished cd init\n";
}

//Base Constructor
IO_smart_iterator::IO_smart_iterator(
		const int comp_tid, index_t *comm,
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
		cb_func p_func):
		num_rows(num_rows),num_cols(num_cols),comp_tid(comp_tid),
		sa_ptr(sa),sa_prev(sa_prev),sort_criterion(sort_criterion),
		semaphore_acq(semaphore_acq),semaphore_flag(semaphore_flag)
{
	#ifdef TIMING
	loadss.str("");loadss.clear();
	issuess.str("");issuess.clear();
	compss.str("");compss.clear();
	sortss.str("");sortss.clear();
	#endif
	
	success_sort=0;
	sort_req=0;
	io_time = 0;
	wait_io_time = 0;
	wait_comp_time = 0;

	VERT_PER_BLK=BLK_SZ/sizeof(vertex_t);
	is_active=p_func;
	my_row = comp_tid/num_cols;
	my_col = comp_tid%num_cols;

	char beg_filename[256];
	char csr_filename[256];
	sprintf(beg_filename, "%s/row_%d_col_%d/%s.%d_%d_of_%dx%d.bin", 
					beg_dir, my_row, my_col, beg_header, 
					my_row, my_col, num_rows, num_cols);
		
	int fd_beg = open(beg_filename, O_RDONLY | O_NOATIME | O_DIRECT);
	if(fd_beg==-1)
	{
		perror("open");
		fprintf(stdout,"Wrong open %s\n",beg_filename);
		exit(-1);
	}

	off_t sz_beg = fsize(beg_filename);
	row_ranger_beg = 0;
	for(int i=0;i<my_row;i++)
		row_ranger_beg += comm[i * num_cols + my_col];
	row_ranger_end = row_ranger_beg + comm[comp_tid];
	
	if(sz_beg & (BLK_SZ-1)) sz_beg += BLK_SZ - (sz_beg&(BLK_SZ -1));
	
	beg_pos=(index_t *)mmap(NULL,sz_beg,
			PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS 
			| MAP_HUGETLB | MAP_HUGE_2MB, 0, 0);
	if(beg_pos == MAP_FAILED)
	{
		printf("%ld\n",sz_beg);
		perror("beg_pos alloc mmap");
		exit(-1);
	}

	//O_DIRECT requires:
	//- starting offset is aligned
	//- total read size is aligned
	//- like bricks
	index_t ptr=0;
	index_t ret=0;
	//- cannot handle huge graph, e.g., sz_beg is too big
	index_t read_sz = (sz_beg > READ_BLK ? READ_BLK:sz_beg);
	while(true)
	{
		//pread cannot do too big read, split them into small pieces
		ret=pread(fd_beg, beg_pos+ptr/sizeof(index_t), read_sz, ptr);
		if(ret<=0)
			perror("pread");
		
		if(ret!=read_sz) break;
		ptr+= read_sz;
	}

	close(fd_beg);
	
	sprintf(csr_filename, "%s/row_%d_col_%d/%s.%d_%d_of_%dx%d.bin", 
					csr_dir, my_row, my_col, csr_header, 
					my_row, my_col, num_rows, num_cols);
	
	off_t sz_csr = fsize(csr_filename);
	assert(sz_csr == (beg_pos[row_ranger_end-row_ranger_beg]) 
				* sizeof(vertex_t));

	//Attention: We are not using o_direct because of ramdisk.
	fd_csr = open(csr_filename, O_RDONLY | O_DIRECT| O_NOATIME);
	//int fd_csr = open(csr_filename, O_RDONLY);
	if(fd_csr == -1)
	{
		fprintf(stdout,"Wrong open %s\n",csr_filename);
		perror("open");
		exit(-1);
	}
	
	my_level = 0;
	io_conserve = false;
	beg_pos_ptr = beg_pos;
	reqt_blk_count = 0;
	total_blks = beg_pos_ptr[row_ranger_end-row_ranger_beg]/VERT_PER_BLK;
	
	if(total_blks & (VERT_PER_BLK-1)) ++total_blks;

	//add 64 more bits, in order for quick bitmap scan.
	reqt_blk_bitmap=(bit_t *)mmap(NULL,((total_blks>>3)+8) * sizeof(bit_t),
			PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS 
			| MAP_HUGETLB | MAP_HUGE_2MB, 0, 0);
	if(reqt_blk_bitmap == MAP_FAILED)
	{
		perror("reqt_blk_bitmap mmap");
		exit(-1);
	}
	
	blk_beg_vert=(vertex_t *)mmap(NULL,total_blks * sizeof(vertex_t),
			PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS 
			| MAP_HUGETLB | MAP_HUGE_2MB, 0, 0);
	if(blk_beg_vert == MAP_FAILED)
	{
		printf("size: %ld\n", total_blks * sizeof(vertex_t));
		perror("blk_beg_vert mmap");
		exit(-1);
	}
	memset(reqt_blk_bitmap, 0, ((total_blks>>3)+1) * sizeof(bit_t));
	memset(blk_beg_vert, 0, total_blks * sizeof(vertex_t));


	//beg vertex Id and end vertex Id of each block
	blk_beg_vert[0] = row_ranger_beg;
	for(index_t i=1; i<total_blks; i++)
	{
		for(index_t j=blk_beg_vert[i-1];j<row_ranger_end;++j)
			if(beg_pos_ptr[j+1-row_ranger_beg]>i*VERT_PER_BLK)
			{
				blk_beg_vert[i] = j;
				break;
			}
	}
}

//Dealloc function
IO_smart_iterator::~IO_smart_iterator()
{
	delete cd;
	munmap(reqt_blk_bitmap, ((total_blks>>3)+1) * sizeof(bit_t));
	munmap(blk_beg_vert, sizeof(vertex_t) * total_blks);
	munmap(beg_pos_ptr, sizeof(index_t)*(row_ranger_end - row_ranger_beg + 1));
}



void IO_smart_iterator::priority_queue(int *acq_seq, int *seq_flag) 
{
	int my_turn = __sync_fetch_and_add(acq_seq, 1);

	while(seq_flag[0] != my_turn)
	{
		cd->get_chunk();
		cd->load_chunk();
	}
	seq_flag[0]++;
}


//-Translate frontiers to requested data blks
void IO_smart_iterator::req_translator(sa_t criterion)
{
	//At the end of every level, reqt_blk_bitmap should be all 0s.
	//since all requests are satisfied.
	reqt_blk_count = 0;
	for(index_t i = row_ranger_beg; i < row_ranger_end; i++)
//	for(index_t i = col_ranger_beg; i < col_ranger_end; i++)
		if((*is_active)(i,criterion,sa_ptr, sa_prev))
		{
			index_t beg = beg_pos_ptr[i - row_ranger_beg];
			index_t end = beg_pos_ptr[i+1 - row_ranger_beg];

			//one frontier's neighbor may span 
			//-across multiple chunks
			index_t beg_blk_ptr = beg/VERT_PER_BLK;
			index_t end_blk_ptr = end/VERT_PER_BLK;

			if(end & (VERT_PER_BLK-1)) ++end_blk_ptr;
			for(index_t j=beg_blk_ptr; j<end_blk_ptr; ++j)
			{
				//assuming it is using bit_t
				if((reqt_blk_bitmap[j>>3] & (1<<(j&7))) == 0)
				{
					++reqt_blk_count;
					reqt_blk_bitmap[j>>3] |= (1<<(j&7));
				}
			}
		}
	
	//means we start a new iteration
	//we should do some IO-conserving work in next()
//	if(my_level < usr_level)
//	{
		io_conserve = true;
//		my_level = usr_level;
	//}
	
//for(int i = 0; i< 1; ++i)
//{
//	if(i == comp_tid) 
//if(comp_tid==0)		std::cout<<"Thread "<<comp_tid<<" #blk="<<reqt_blk_count<<
//			" #freechunks="<<cd->circ_free_chunk->num_elem<<
//			" #freectx="<<cd->circ_free_ctx->num_elem<<
			"\n";
//	#pragma omp barrier
//}
}

//Problematic for larger than 2^31 verts
int cmpfunc (const void * a, const void * b)
{
	//return ( *(int*)a - *(int*)b );
	return ( *(long*)a - *(long*)b );
}


//Sorting order must be ascending!!!
void IO_smart_iterator::front_sort_cpu()
{
	//frontier-queue maybe larger than gpu buffer
	//block-by-block sorting
	long offset = 0;
	const index_t num_front = this->front_count[comp_tid];
	bool is_done = false;

	qsort(this->front_queue[comp_tid], num_front, sizeof(vertex_t),
			cmpfunc);

//	if(comp_tid == 0) 
	//printf("front_count: %ld: %u, %u\n", 
	//		num_front, this->front_queue[comp_tid][0], this->front_queue[comp_tid][1]);
}

//-Translate frontiers to requested data blks
void IO_smart_iterator::req_translator_queue()
{
	//At the end of every level, reqt_blk_bitmap should be all 0s.
	//since all requests are satisfied.
	reqt_blk_count = 0;

	//Only needs to check the frontier queue whose col_ranger 
	//overlaps with my row_ranger
	for(index_t row_ptr = 0; row_ptr < num_rows; row_ptr ++)
	{
		for(index_t col_ptr = 0; col_ptr < num_cols; col_ptr ++)
		{
			vertex_t fq_ranger_beg = col_ranger[row_ptr * (num_cols + 1) + col_ptr];
			vertex_t fq_ranger_end = col_ranger[row_ptr * (num_cols + 1) + col_ptr + 1];
	
			//if the fq's col ranger overlaps MY row_ranger
			if((fq_ranger_beg >= row_ranger_beg && fq_ranger_beg < row_ranger_end) ||
			   (fq_ranger_end >= row_ranger_beg && fq_ranger_end < row_ranger_end) ||
			   (fq_ranger_end >= row_ranger_end && fq_ranger_beg <=row_ranger_beg))
			{
				for(index_t m = 0; m < front_count[row_ptr * num_cols + col_ptr]; m ++)
				{
					vertex_t i = front_queue[row_ptr * num_cols + col_ptr][m];
					if(i < row_ranger_beg || i >= row_ranger_end) continue;

					index_t beg = beg_pos_ptr[i - row_ranger_beg];
					index_t end = beg_pos_ptr[i+1 - row_ranger_beg];

					//one frontier's neighbor may span 
					//-across multiple chunks
					index_t beg_blk_ptr = beg/VERT_PER_BLK;
					index_t end_blk_ptr = end/VERT_PER_BLK;

					if(end & (VERT_PER_BLK-1)) ++end_blk_ptr;
					for(index_t j=beg_blk_ptr; j<end_blk_ptr; ++j)
					{
						//assuming it is using bit_t
						if((reqt_blk_bitmap[j>>3] & (1<<(j&7))) == 0)
						{
							++reqt_blk_count;
							reqt_blk_bitmap[j>>3] |= (1<<(j&7));
						}
					}
				}
			}
		}
	}	
	//means we start a new iteration
	//we should do some IO-conserving work in next()
//	if(my_level < usr_level)
//	{
		io_conserve = true;
//		my_level = usr_level;
	//}
	
//for(int i = 0; i< 1; ++i)
//{
//	if(comp_tid == 0) 
//		std::cout<<"Thread "<<comp_tid<<" #blk="<<reqt_blk_count<<
//			" #freechunks="<<cd->circ_free_chunk->num_elem<<
//			" #freectx="<<cd->circ_free_ctx->num_elem<<
//			"\n";
//	#pragma omp barrier
//}
}

void IO_smart_iterator::read_trace_to_list(char *trace_file)
{
	FILE *file = fopen(trace_file, "rb");
	if(file == NULL)
	{
		perror("fopen");
		exit(-1);
	}
	off_t num_blks = fsize(trace_file);
	int64_t *blk_list = new int64_t[num_blks/sizeof(int64_t)];
	//size_t fread(void *ptr, size_t size, size_t nmemb, FILE *stream);
	fread(blk_list, sizeof(int64_t), num_blks, file);
	
	reqt_blk_count = 0;
	double tm= wtime();
	for(int64_t i = 0; i < num_blks/sizeof(int64_t); i++)
	{
		int64_t blk_id = blk_list[i];
		if(blk_id >= total_blks) continue;
		reqt_list[reqt_blk_count++] = blk_id;

		if(reqt_blk_count % 16 == 0)
			qsort(reqt_list+reqt_blk_count - 16, 16, sizeof(index_t),
				cmpfunc);
	}
//	qsort(reqt_list, reqt_blk_count,sizeof(index_t),
//			cmpfunc);

	tm=wtime() -tm;

	if(!omp_get_thread_num())
		printf("Reqlist Get %ld blks, time %lf seconds\n", reqt_blk_count, tm);

}


void IO_smart_iterator::read_trace_to_bitmap(char *trace_file)
{
	FILE *file = fopen(trace_file, "rb");
	if(file == NULL)
	{
		perror("fopen");
		exit(-1);
	}
	off_t num_blks = fsize(trace_file);
	int64_t *blk_list = new int64_t[num_blks/sizeof(int64_t)];
	//size_t fread(void *ptr, size_t size, size_t nmemb, FILE *stream);
	fread(blk_list, sizeof(int64_t), num_blks, file);
	
	reqt_blk_count = 0;
	
	double tm = wtime();
	for(int64_t i = 0; i < num_blks/sizeof(int64_t); i++)
	{
		int64_t blk_id = blk_list[i];
		if(blk_id >= total_blks) continue;

		if((reqt_blk_bitmap[blk_id>>3] & (1<<(blk_id&7))) == 0)
		{
			++reqt_blk_count;
			reqt_blk_bitmap[blk_id>>3] |= (1<<(blk_id&7));
		}
	}
	tm=wtime() -tm;

	if(!omp_get_thread_num())
		printf("Bitmap Get %ld blks, time %lf seconds\n", reqt_blk_count, tm);
}



void IO_smart_iterator::req_convert_list()
{
	reqt_blk_count = 0;

	//Only needs to check the frontier queue whose col_ranger 
	//overlaps with my row_ranger
	for(index_t row_ptr = 0; row_ptr < num_rows; row_ptr ++)
	{
		for(index_t col_ptr = 0; col_ptr < num_cols; col_ptr ++)
		{
			vertex_t fq_ranger_beg = col_ranger[row_ptr * (num_cols + 1) + col_ptr];
			vertex_t fq_ranger_end = col_ranger[row_ptr * (num_cols + 1) + col_ptr + 1];
	
			//if the fq's col ranger overlaps MY row_ranger
			if((fq_ranger_beg >= row_ranger_beg && fq_ranger_beg < row_ranger_end) ||
			   (fq_ranger_end >= row_ranger_beg && fq_ranger_end < row_ranger_end) ||
			   (fq_ranger_end >= row_ranger_end && fq_ranger_beg <=row_ranger_beg))
			{
				for(index_t m = 0; m < front_count[row_ptr * num_cols + col_ptr]; m ++)
				{
					index_t i = front_queue[row_ptr * num_cols + col_ptr][m];
					if((i < row_ranger_beg) || (i >= row_ranger_end)) continue;
					
					//if(i-row_ranger_beg<0) printf("%u-beg-%u\n",i,row_ranger_beg);
					//if(i+1-row_ranger_beg >= row_ranger_end) 
					//	printf("%u-end-%u\n", i, row_ranger_end);
					
					if(m>0)
						if(front_queue[row_ptr * num_cols + col_ptr][m-1]
								== i) continue;

					index_t beg = beg_pos_ptr[i - row_ranger_beg];
					index_t end = beg_pos_ptr[i+1 - row_ranger_beg];
					assert(i - row_ranger_beg >= 0);
					assert(i+1 -row_ranger_beg <= row_ranger_end);

					//one frontier's neighbor may span 
					//-across multiple chunks
					index_t beg_blk_ptr = beg/VERT_PER_BLK;
					index_t end_blk_ptr = end/VERT_PER_BLK;
					//if(end_blk_ptr >= total_blks) printf("wrong-%ld-%ld\n",end_blk_ptr,end);

					if(end & (VERT_PER_BLK-1)) ++end_blk_ptr;
					for(index_t j=beg_blk_ptr; j<end_blk_ptr; ++j)
					{
						assert(j<total_blks);
						if(reqt_blk_count != 0)
						{
							if(j != reqt_list[reqt_blk_count-1])
							{
								reqt_list[reqt_blk_count]= j;
								++reqt_blk_count;
								assert(reqt_blk_count < (total_blks * 10));
								//assert(reqt_blk_count < 33554432);
							}
						}
						else
						{
							reqt_list[reqt_blk_count] = j;
							++reqt_blk_count;
						}
					}
				}
			}
		}
	}
	
	qsort(reqt_list, reqt_blk_count, sizeof(index_t),
			cmpfunc);
	
	for(int i = 0; i < reqt_blk_count; i++)
		assert(reqt_list[i]<total_blks);
	io_conserve = true;
	//printf("init-reqt_blk_count: %ld\n", reqt_blk_count);
}


int IO_smart_iterator::next(int used_buff)
{
	int buff_ptr_comp = -1;
	if(used_buff != -1) cd->circ_free_chunk->en_circle(used_buff);
	
	double blk_tm = wtime();
	while((buff_ptr_comp=cd->circ_load_chunk->de_circle())==-1){
		if(is_bsp_done) break;
	}
	wait_io_time += (wtime() - blk_tm);
	
	//avoid concurrent read of is_bsp_done
	if(is_bsp_done) 
	{
		if((buff_ptr_comp = cd->circ_load_chunk->de_circle())
				== -1) 
		{
			for(int i = 0; i < num_buffs; i ++)
				cd->circ_free_chunk->en_circle(i);
			cd->circ_load_chunk->reset_circle();
		}
		return buff_ptr_comp;	
	}
	return buff_ptr_comp;
}


//Pointer chasing based load key
void IO_smart_iterator::load_key(sa_t criterion)
{
//	double io_this_tm;
//	int buff_ptr_io = -1;
//	while((buff_ptr_io = circ_free_buff->de_circle())
//			== -1)
//	{
//		//printf("I%d\n", omp_get_thread_num());
//		cd->get_chunk();
//		cd->load_chunk();
//	}
//	
//	io_this_tm = wtime();
//	vertex_t *io_buff = buff_dest[buff_ptr_io];
	double blk_tm = wtime();
	while(cd->circ_free_chunk->is_empty()){}
	this->wait_comp_time += (wtime() - blk_tm);

	cd->get_chunk();
	cd->load_chunk();
	cd->get_chunk();
	cd->load_chunk();
	if((cd->circ_load_chunk->is_empty()) &&
		(cd->circ_free_chunk->is_full()) &&
		(cd->circ_free_ctx->is_full()) &&
		(this->reqt_blk_count == 0))
	{
		//std::cout<<omp_get_thread_num()<<"-done\n";
		is_bsp_done = true;
	}

	
	
	//	//Once entering here. 
//	//-dump out at least half submissions
//	//-if at half, still NULL, dump all
//	index_t num_elements = 0;
//	while(true)
//	{
//		circle *loadc=cd->get_chunk();
//		index_t processed_chunk = 0;
//		//a new loaded circle is empty
//		//we are good to go.
//		if(loadc->is_empty()) break;
//
//		while(!loadc->is_empty())
//		{
//			index_t chunk_id = loadc->de_circle();
//			struct chunk *pinst = cd->cache[chunk_id];	
//			index_t blk_beg_off = pinst->blk_beg_off;
//			index_t num_verts = pinst->load_sz;
//			vertex_t vert_id = pinst->beg_vert;
//
//			//process one chunk
//			while(true)
//			{
//				if((*is_active)(vert_id,criterion,sa_ptr, sa_prev))
//				{
//					index_t beg = beg_pos_ptr[vert_id-row_ranger_beg]-blk_beg_off;
//					index_t end = beg + beg_pos_ptr[vert_id+1-row_ranger_beg]- 
//						beg_pos_ptr[vert_id-row_ranger_beg];
//
//					//possibly vert_id starts from preceding data block.
//					//there by beg<0 is possible
//					if(beg<0) beg = 0;
//
//					if(end>num_verts) end = num_verts;
//					for( ;beg<end; ++beg)
//					{
//						//assert(pinst->buff[beg]<1262485504);
//						io_buff[num_elements++] = pinst->buff[beg];
//					}
//				}
//				++vert_id;
//
//				if(vert_id >= this->row_ranger_end) break;
//				if(beg_pos_ptr[vert_id-row_ranger_beg]-blk_beg_off > num_verts) 
//					break;
//			}
//
//			pinst->status = EVICTED;
//			cd->circ_free_chunk->en_circle(chunk_id);
//			++processed_chunk;
//
//			//issue 16 requests each time
//			if(processed_chunk==((cd->io_limit)>>1))
//			{
//				cd->load_chunk();
//				cd->get_chunk();
//				processed_chunk=0;
//			}
//			if(num_elements > buff_max_vert - cd->vert_per_chunk)
//				goto fullpoint;
//		}
//	}
//
//fullpoint:
//	cd->load_chunk();
//	cd->get_chunk();
//
//	buff_edge_count[buff_ptr_io] = num_elements;
//	//if(circ_load_buff->get_sz() > 2) 
//	//	qsort(io_buff, num_elements, sizeof(vertex_t), cmpfunc);
//	circ_load_buff->en_circle(buff_ptr_io);
//	is_bsp_done = (num_elements == 0);
//	io_time += (wtime() - io_this_tm);
//	//if(comp_tid == 0) std::cout<<"num_elements: "<<num_elements<<"\n";
	return;
}

//Pagerank based load key in full.
void IO_smart_iterator::load_kv_vert_full(sa_t criterion)
{
	double blk_tm = wtime();
	while(cd->circ_free_chunk->is_empty()){}
	this->wait_comp_time += (wtime() - blk_tm);

	cd->get_chunk();
	cd->load_chunk_full();
	cd->get_chunk();
	cd->load_chunk_full();
	
	if((cd->circ_load_chunk->is_empty()) &&
		(cd->circ_free_chunk->is_full()) &&
		(cd->circ_free_ctx->is_full()) &&
		(this->reqt_blk_count == 0))
		is_bsp_done = true;


//	int buff_ptr_io = -1;
//	double io_this_tm;
//	//while(is_io_done == true)
//	double blk_tm = wtime();
//	while((buff_ptr_io = circ_free_buff->de_circle())
//			== -1)
//	{
//		cd->get_chunk();
//		cd->load_chunk_full();
//	}
//
//	wait_comp_time += (wtime() - blk_tm);
//
//	io_this_tm = wtime();
//	vertex_t *io_buff_src = buff_src_vert[buff_ptr_io];
//	vertex_t *io_buff_dest = buff_dest[buff_ptr_io];
//	cd->load_chunk_full();
//	cd->get_chunk();
//
//	//Once entering here. 
//	//-dump out at least half submissions
//	//-if at half, still NULL, dump all
//	index_t num_elements = 0;
//	while(true)
//	{
//		cd->load_chunk_full();
//		circle *loadc=cd->get_chunk();
//		index_t processed_chunk = 0;
//		//a new loaded circle is empty
//		//we are good to go.
//		if(loadc->is_empty()) break;
//
//		while(!loadc->is_empty())
//		{
//			index_t chunk_id = loadc->de_circle();
//			struct chunk *pinst = cd->cache[chunk_id];	
//			index_t blk_beg_off = pinst->blk_beg_off;
//			index_t num_verts = pinst->load_sz;
//			vertex_t vert_id = pinst->beg_vert;
//
//			//process one chunk
//			while(true)
//			{
//				index_t beg = beg_pos_ptr[vert_id-row_ranger_beg]-blk_beg_off;
//				index_t end = beg+beg_pos_ptr[vert_id+1-row_ranger_beg]- 
//					beg_pos_ptr[vert_id-row_ranger_beg];
//
//				//possibly vert_id starts from preceding data block.
//				//there by beg<0 is possible
//				if(beg<0) beg = 0;
//
//				if(end>num_verts) end = num_verts;
//				for( ;beg<end; ++beg)
//				{
//					io_buff_src[num_elements] = vert_id;
//					io_buff_dest[num_elements++] = pinst->buff[beg];
//				}
//				++vert_id;
//
//				if(vert_id >= this->row_ranger_end) break;
//				if(beg_pos_ptr[vert_id-row_ranger_beg]-blk_beg_off > num_verts) 
//					break;
//			}
//
//			pinst->status = EVICTED;
//			cd->circ_free_chunk->en_circle(chunk_id);
//			++processed_chunk;
//
//			//issue 16 requests each time
//			if(processed_chunk==((cd->io_limit)>>1))
//			{
//				cd->load_chunk_full();
//				cd->get_chunk();
//				processed_chunk=0;
//			}
//
//			if(num_elements > buff_max_vert - cd->vert_per_chunk)
//				goto fullpoint;
//		}
//	}
//
//fullpoint:
//	cd->load_chunk_full();
//	cd->get_chunk();
//
//	buff_edge_count[buff_ptr_io] = num_elements;
//	circ_load_buff->en_circle(buff_ptr_io);
//	is_bsp_done = (num_elements == 0);
//	io_time += (wtime() - io_this_tm);
	return;
}


//IO list based load vertex
void IO_smart_iterator::load_key_iolist(sa_t criterion)
{
	double blk_tm = wtime();
	while(cd->circ_free_chunk->is_empty()){}
	this->wait_comp_time += (wtime() - blk_tm);

	cd->get_chunk();
	cd->load_chunk_iolist();
	cd->get_chunk();
	cd->load_chunk_iolist();
	if((cd->circ_load_chunk->is_empty()) &&
		(cd->circ_free_chunk->is_full()) &&
		(cd->circ_free_ctx->is_full()) &&
		(this->reqt_blk_count == 0))
		is_bsp_done = true;


//	int buff_ptr_io = -1;
//	while(is_bsp_done == false)
//	{
//		while((buff_ptr_io = circ_free_buff->de_circle())
//				== -1)
//		{
//			cd->get_chunk();
//			cd->load_chunk_iolist();
//		}
//
//		vertex_t *io_buff = buff_dest[buff_ptr_io];
//		cd->load_chunk_iolist();
//		cd->get_chunk();
//
//		//Once entering here. 
//		//-dump out at least half submissions
//		//-if at half, still NULL, dump all
//		index_t num_elements = 0;
//		while(true)
//		{
//			cd->load_chunk_iolist();
//			circle *loadc=cd->get_chunk();
//			index_t processed_chunk = 0;
//			//a new loaded circle is empty
//			//we are good to go.
//			if(loadc->is_empty()) break;
//
//			while(!loadc->is_empty())
//			{
//				index_t chunk_id = loadc->de_circle();
//				struct chunk *pinst = cd->cache[chunk_id];	
//				index_t blk_beg_off = pinst->blk_beg_off;
//				index_t num_verts = pinst->load_sz;
//				vertex_t vert_id = pinst->beg_vert;
//
//				//process one chunk
//				while(true)
//				{
//					if((*is_active)(vert_id,criterion,sa_ptr, sa_prev))
//					{
//						index_t beg = beg_pos_ptr[vert_id-row_ranger_beg]-blk_beg_off;
//						index_t end = beg+beg_pos_ptr[vert_id+1-row_ranger_beg]- 
//							beg_pos_ptr[vert_id-row_ranger_beg];
//
//						//possibly vert_id starts from preceding data block.
//						//there by beg<0 is possible
//						if(beg<0) beg = 0;
//
//						if(end>num_verts) end = num_verts;
//						for( ;beg<end; ++beg)
//						{
//							//assert(pinst->buff[beg]<1262485504);
//							io_buff[num_elements++] = pinst->buff[beg];
//						}
//					}
//					++vert_id;
//
//					if(vert_id >= this->row_ranger_end) break;
//					if(beg_pos_ptr[vert_id-row_ranger_beg]-blk_beg_off > num_verts) 
//						break;
//				}
//
//				pinst->status = EVICTED;
//				cd->circ_free_chunk->en_circle(chunk_id);
//				++processed_chunk;
//
//				//issue 16 requests each time
//				if(processed_chunk==((cd->io_limit)>>1))
//				{
//					cd->load_chunk_iolist();
//					cd->get_chunk();
//					processed_chunk=0;
//				}
//				if(num_elements > buff_max_vert - cd->vert_per_chunk)
//					goto fullpoint;
//			}
//		}
//
//fullpoint:
//		cd->load_chunk_iolist();
//		cd->get_chunk();
//
//		buff_edge_count[buff_ptr_io] = num_elements;
//		circ_load_buff->en_circle(buff_ptr_io);
//		
//		is_bsp_done = (num_elements == 0);
//	}
//	return;
}


//Used for sorting stuffs
void IO_smart_iterator::load_kv_sa(sa_t criterion)
{
	int buff_ptr_io = -1;
	double io_this_tm;
	while(is_bsp_done == false)
	{
		//while(is_io_done == true)
		double blk_tm = wtime();
		while((buff_ptr_io = circ_free_buff->de_circle())
				== -1)
		{
			//printf("I%d\n", omp_get_thread_num());
			cd->get_chunk();
			cd->load_chunk();
		}
		wait_comp_time += (wtime() - blk_tm);

		io_this_tm = wtime();
		sa_t *io_buff_src = buff_source[buff_ptr_io];
		vertex_t *io_buff_dest = buff_dest[buff_ptr_io];
		cd->get_chunk();
		cd->load_chunk();

		//Once entering here. 
		//-dump out at least half submissions
		//-if at half, still NULL, dump all
		index_t num_elements = 0;
		while(true)
		{
			circle *loadc=cd->get_chunk();
			index_t processed_chunk = 0;
			//a new loaded circle is empty
			//we are good to go.
			if(loadc->is_empty()) break;

			while(!loadc->is_empty())
			{
				index_t chunk_id = loadc->de_circle();
				struct chunk *pinst = cd->cache[chunk_id];	
				index_t blk_beg_off = pinst->blk_beg_off;
				index_t num_verts = pinst->load_sz;
				vertex_t vert_id = pinst->beg_vert;

				//process one chunk
				while(true)
				{
					if((*is_active)(vert_id,criterion,sa_ptr, sa_prev))
					{
						index_t beg = beg_pos_ptr[vert_id-row_ranger_beg]-blk_beg_off;
						index_t end = beg+beg_pos_ptr[vert_id+1-row_ranger_beg]- 
							beg_pos_ptr[vert_id-row_ranger_beg];

						//possibly vert_id starts from preceding data block.
						//there by beg<0 is possible
						if(beg<0) beg = 0;

						if(end>num_verts) end = num_verts;

						sa_t sa_src = sa_ptr[vert_id];
						for( ;beg<end; ++beg)
						{
							//assert(pinst->buff[beg]<1262485504);
							io_buff_src[num_elements] = sa_src;
							io_buff_dest[num_elements++] = pinst->buff[beg];
						}
					}
					++vert_id;

					if(vert_id >= this->row_ranger_end) break;
					if(beg_pos_ptr[vert_id-row_ranger_beg]-blk_beg_off > num_verts) 
						break;
				}

				pinst->status = EVICTED;
				cd->circ_free_chunk->en_circle(chunk_id);
				++processed_chunk;

				//issue 16 requests each time
				if(processed_chunk==((cd->io_limit)>>1))
				{
					cd->load_chunk();
					cd->get_chunk();
					processed_chunk=0;
				}
				if(num_elements > buff_max_vert - cd->vert_per_chunk)
					goto fullpoint;
			}
		}

fullpoint:
		cd->load_chunk();
		cd->get_chunk();

		buff_edge_count[buff_ptr_io] = num_elements;
		circ_load_buff->en_circle(buff_ptr_io);
		is_bsp_done = (num_elements == 0);
		//if(comp_tid == 0) std::cout<<"num_elements: "<<num_elements<<"\n";
		io_time += (wtime() - io_this_tm);
	}
	return;
}



void IO_smart_iterator::load_kv_vert(sa_t criterion)
{
	int buff_ptr_io = -1;
	while(is_bsp_done == false)
	{
		while((buff_ptr_io = circ_free_buff->de_circle())
				== -1)
		{
			cd->get_chunk();
			cd->load_chunk();
		}

		sa_t *io_buff_src = buff_source[buff_ptr_io];
		vertex_t *io_buff_dest = buff_dest[buff_ptr_io];
		cd->get_chunk();
		cd->load_chunk();

		//Once entering here. 
		//-dump out at least half submissions
		//-if at half, still NULL, dump all
		index_t num_elements = 0;
		while(true)
		{
			circle *loadc=cd->get_chunk();
			index_t processed_chunk = 0;
			//a new loaded circle is empty
			//we are good to go.
			if(loadc->is_empty()) break;

			while(!loadc->is_empty())
			{
				index_t chunk_id = loadc->de_circle();
				struct chunk *pinst = cd->cache[chunk_id];	
				index_t blk_beg_off = pinst->blk_beg_off;
				index_t num_verts = pinst->load_sz;
				vertex_t vert_id = pinst->beg_vert;

				//process one chunk
				while(true)
				{
					if((*is_active)(vert_id,criterion,sa_ptr, sa_prev))
					{
						index_t beg = beg_pos_ptr[vert_id-row_ranger_beg]-blk_beg_off;
						index_t end = beg+beg_pos_ptr[vert_id+1-row_ranger_beg]- 
							beg_pos_ptr[vert_id-row_ranger_beg];

						//possibly vert_id starts from preceding data block.
						//there by beg<0 is possible
						if(beg<0) beg = 0;

						if(end>num_verts) end = num_verts;
						for( ;beg<end; ++beg)
						{
							//assert(pinst->buff[beg]<1262485504);
							io_buff_src[num_elements] = vert_id;
							io_buff_dest[num_elements++] = pinst->buff[beg];
						}
					}
					++vert_id;

					if(vert_id >= this->row_ranger_end) break;
					if(beg_pos_ptr[vert_id-row_ranger_beg]-blk_beg_off > num_verts) 
						break;
				}

				pinst->status = EVICTED;
				cd->circ_free_chunk->en_circle(chunk_id);
				++processed_chunk;

				//issue 16 requests each time
				if(processed_chunk==((cd->io_limit)>>1))
				{
					cd->load_chunk();
					cd->get_chunk();
					processed_chunk=0;
				}
				if(num_elements > buff_max_vert - cd->vert_per_chunk)
					goto fullpoint;
			}
		}

fullpoint:
		cd->load_chunk();
		cd->get_chunk();

		buff_edge_count[buff_ptr_io] = num_elements;
		circ_load_buff->en_circle(buff_ptr_io);
		is_bsp_done = (num_elements == 0);
		//if(comp_tid == 0) std::cout<<"num_elements: "<<num_elements<<"\n";
	}
	return;
}
