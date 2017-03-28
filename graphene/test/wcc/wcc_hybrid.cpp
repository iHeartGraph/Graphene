//-----------------COMMENTS BEGINE---------------------------------
//Possible optimizations: 
//1. BFS + WCC would be better.
//2. Even in this version, we need --> take care of color pollution
// NOTIFIED AS FOLLOW SERCH "COLOR POLUTION"
//-----------------COMMENTS END-------------------------------------
//
#include "cache_driver.h"
#include "IO_smart_iterator.h"
#include <stdlib.h>
#include <sched.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <asm/mman.h>
#include "pin_thread.h"
#include "get_vert_count.hpp"
#include "get_col_ranger.hpp"
#include <algorithm>


inline bool is_bfs_active
(index_t vert_id,
sa_t criterion,
sa_t *sa, sa_t *prior)
{
	return (sa[vert_id]==criterion);
}

inline bool is_wcc_active
(index_t vert_id,
 sa_t criterion,
 sa_t *sa, sa_t *sa_prev)
{
	return (sa[vert_id] != criterion); 
}

int main(int argc, char **argv) 
{
	std::cout<<"Format: /path/to/exe " 
		<<"#row_partitions #col_partitions thread_count "
		<<"/path/to/beg_pos_dir /path/to/csr_dir "
		<<"beg_header csr_header num_chunks "
		<<"chunk_sz (#bytes) concurr_IO_ctx "
		<<"max_continuous_useless_blk ring_vert_count num_buffs\n";
	if(argc != 14)
	{
		fprintf(stdout, "Wrong input\n");
		exit(-1);
	}

	//Output input
	for(int i=0;i<argc;i++)
		std::cout<<argv[i]<<" ";
	std::cout<<"\n";

	const int row_par = atoi(argv[1]);
	const int col_par = atoi(argv[2]);
	const int NUM_THDS = atoi(argv[3]);
	const char *beg_dir = argv[4];
	const char *csr_dir = argv[5];
	const char *beg_header=argv[6];
	const char *csr_header=argv[7];
	const index_t num_chunks = atoi(argv[8]);
	const size_t chunk_sz = atoi(argv[9]);
	const index_t io_limit = atoi(argv[10]);
	const index_t MAX_USELESS = atoi(argv[11]);
	const index_t ring_vert_count = atoi(argv[12]);
	const index_t num_buffs = atoi(argv[13]);
	
	assert(NUM_THDS==(row_par*col_par*2));
	sa_t *sa=NULL;
	sa_t *color_redirect=NULL;
	index_t *comm = new index_t[NUM_THDS];
	vertex_t **front_queue_ptr;
	index_t *front_count_ptr;
	vertex_t *col_ranger_ptr;
	
	const index_t vert_count=get_vert_count
		(comm, beg_dir,beg_header,row_par,col_par);
	get_col_ranger(col_ranger_ptr, front_queue_ptr,
			front_count_ptr, beg_dir, beg_header,
			row_par, col_par);
	
	
	sa=(sa_t *)mmap(NULL,sizeof(sa_t)*vert_count,
			PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS 
			| MAP_HUGETLB | MAP_HUGE_2MB, 0, 0);
	if(sa==MAP_FAILED)
	{	
		perror("mmap");
		exit(-1);
	}

	color_redirect=(sa_t *)mmap(NULL,sizeof(sa_t)*vert_count,
			PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS 
			| MAP_HUGETLB | MAP_HUGE_2MB, 0, 0);
	if(color_redirect==MAP_FAILED)
	{	
		perror("mmap");
		exit(-1);
	}
	
	int *odeg_glb=(int *)mmap(NULL,
		sizeof(int)*vert_count,
		PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS 
		| MAP_HUGETLB | MAP_HUGE_2MB, 0, 0);
	
	if(odeg_glb==MAP_FAILED)
	{	
		perror("mmap");
		exit(-1);
	}

	const index_t vert_per_blk = chunk_sz / sizeof(vertex_t);
	if(chunk_sz&(sizeof(vertex_t) - 1))
	{
		std::cout<<"Page size wrong\n";
		exit(-1);
	}
	
	char cmd[256];
	sprintf(cmd,"%s","iostat -x 1 -k > iostat_wcc.log&");
	std::cout<<cmd<<"\n";
	//exit(-1);
	//omp_lock_t gpu_semaphore;
	//omp_init_lock(&gpu_semaphore);
	//0 1 2 3 4 5 6 7 8 9 10 11 12 13 28 29 30 31 32 33 34 35 36 37 38 39 40 41
	//14 15 16 17 18 19 20 21 22 23 24 25 26 27 42 43 44 45 46 47 48 49 50 51 52 53 54 55
	//int core_id[8]={0, 2, 4, 6, 14, 16, 18, 20};
	//int core_id[16]={0, 2, 4, 6, 8, 10, 12, 28, 14, 16, 18, 20, 22, 24, 26, 42};
	//int core_id[16]={0, 2, 4, 12, 14, 16, 1, 3, 6, 8, 10, 18, 20, 22, 7, 9};
	//int core_id[16]={0, 2, 4, 6, 8, 10, 12, 28, 30, 32, 34, 36, 38, 40, 1, 3};
	int socket_one[12]={0, 1, 2, 3, 4, 5, 12, 13, 14, 15, 16, 17};
	int socket_two[12]={6, 7, 8, 9, 10, 11, 18, 19, 20, 21, 22, 23};

	for(long i=0;i<vert_count;i++)
	{
		sa[i]=INFTY;
		color_redirect[i]=i;
	}

	sa_t global_color = 1;
	bool *is_change = new bool[NUM_THDS];
	bool *is_redirect = new bool[NUM_THDS];
	int *semaphore_acq = new int[1];
	int *semaphore_flag = new int[1];

	semaphore_acq[0] = 0;
	semaphore_flag[0] = 0;

	sa_t *sa_dummy = NULL;

	//init root value must equal vert_count!!!!
	vertex_t root = vert_count;

	double tm = 0;
	IO_smart_iterator **it_comm = new IO_smart_iterator*[NUM_THDS];
#pragma omp parallel \
	num_threads (NUM_THDS) \
	shared(sa, comm, global_color)
	{
		std::stringstream travss;
		travss.str("");
		travss.clear();

		std::stringstream fetchss;
		fetchss.str("");
		fetchss.clear();

		std::stringstream savess;
		savess.str("");
		savess.clear();
		sa_t level = 0;
		int tid = omp_get_thread_num();
		int comp_tid = tid >> 1;
		comp_t *neighbors;
		sa_t *sources;
		index_t *beg_pos;
		
		vertex_t source, dest;
		sa_t color;

		//pin_thread(core_id,tid);
		
		//use all threads in 1D partition manner.
		index_t step_1d = vert_count / NUM_THDS;
		index_t beg_1d = tid * step_1d;
		index_t end_1d = beg_1d + step_1d;
		if(tid==NUM_THDS-1) end_1d = vert_count;
		
		//Attention: Even we do BFS here, sorting may still be KV based
		//Do BFS first
		if((tid&1) == 0) 
		{
			IO_smart_iterator *it_temp = 
				new IO_smart_iterator(
						front_queue_ptr,
						front_count_ptr,
						col_ranger_ptr,
						comp_tid,comm,
						row_par,col_par,									
						beg_dir,csr_dir, 
						beg_header,csr_header,
						num_chunks,
						chunk_sz,
						sa,sa_dummy,beg_pos,
						num_buffs,
						ring_vert_count,
						MAX_USELESS,
						io_limit,
						&is_bfs_active);

			it_comm[tid] = it_temp;
			it_comm[tid]->is_bsp_done = false;
		}
#pragma omp barrier
		IO_smart_iterator *it = it_comm[(tid>>1)<<1];
		
		//if(!tid) system((const char *)cmd);
#pragma omp barrier
		if(tid==0) std::cout<<"Aggregate degree ... \n";	

		//===========================
		//Finding the max-degree vertex --> root
		//-------------------------------------------
		if((tid & 1 ) == 0)
			for(vertex_t vert=it->row_ranger_beg;
					vert < it->row_ranger_end; vert ++)
			{
				int my_degree = (int)(it->beg_pos_ptr[vert+1-it->row_ranger_beg]-
						it->beg_pos_ptr[vert-it->row_ranger_beg]);

				__sync_fetch_and_add(odeg_glb+vert,my_degree);
			}

#pragma omp barrier
		int max_deg=-1;
		vertex_t max_deg_vert=0;

		for(vertex_t vert=beg_1d; vert<end_1d; vert++)
			if(odeg_glb[vert] > max_deg)
			{
				max_deg = odeg_glb[vert];
				max_deg_vert = vert;
			}
		comm[tid] = (index_t) max_deg;
#pragma omp barrier
		for(int i = 0; i < NUM_THDS; i ++)
			if(comm[i] > max_deg) max_deg = comm[i];

#pragma omp barrier		
		if(max_deg == comm[tid]) 
		{
			if(__sync_bool_compare_and_swap(&root, vert_count, max_deg_vert))
			{
				sa[root] = 0;
				printf("Root is: %u, degree = %d\n", max_deg_vert, max_deg);
			}
		}
#pragma omp barrier

		if((tid & 1)==0)
		{
			if(root>=it->row_ranger_beg && root<it->row_ranger_end)
			{
				printf("Degree[%u]=%ld\n",root,
						it->beg_pos_ptr[root+1-it->row_ranger_beg]
						-it->beg_pos_ptr[root-it->row_ranger_beg]);
				front_queue_ptr[comp_tid][0] = root;
				front_count_ptr[comp_tid] = 1;
			}
			else
			{
				front_count_ptr[comp_tid] = 0;
			}
		}
		//----------------------
		//Root is found
		//-==================================

#pragma omp barrier
		const index_t fq_sz = it->col_ranger_end - it->col_ranger_beg;
	
		double convert_tm=0;
		index_t prev_front_count = 1;
		index_t front_count = 0;
		while(true)
		//while(false)
		{
			//- Framework gives user block to process
			//- Figures out what blocks needed next level
			if((tid & 1) == 0)
			{
				it -> io_time = 0;
				it -> wait_io_time = 0;
				it -> wait_comp_time = 0;
				it -> cd -> io_submit_time = 0;
				it -> cd -> io_poll_time = 0;
				it -> cd -> fetch_sz = 0;
			}
			
			double ltm=wtime();
			if((tid & 1) == 0)
			{
				it->is_bsp_done = false;
				convert_tm=wtime();
				if((prev_front_count * 100.0)/ vert_count > 2.0) 
					it->req_translator(level);
				else
				{
					it->req_translator_queue();
				}
				convert_tm=wtime()-convert_tm;
			}
			else it->is_io_done = false;
#pragma omp barrier

			if((tid & 1) == 0)
			{
				while(true)
				{	
					int chunk_id = -1;
					double blk_tm = wtime();
					while((chunk_id = it->cd->circ_load_chunk->de_circle())
							== -1)
					{
						if(it->is_bsp_done)
						{
							chunk_id = it->cd->circ_load_chunk->de_circle();
							break;
						}
					}
					it->wait_io_time += (wtime() - blk_tm);

					if(chunk_id == -1) break;
					struct chunk *pinst = it->cd->cache[chunk_id];	
					index_t blk_beg_off = pinst->blk_beg_off;
					index_t num_verts = pinst->load_sz;
					vertex_t vert_id = pinst->beg_vert;

					//process one chunk
					while(true)
					{
						if(sa[vert_id] == level)
						{
							index_t beg = beg_pos[vert_id - it->row_ranger_beg] 
								- blk_beg_off;
							index_t end = beg + beg_pos[vert_id + 1 - 
								it->row_ranger_beg]- 
								beg_pos[vert_id - it->row_ranger_beg];

							//possibly vert_id starts from preceding data block.
							//there by beg<0 is possible
							if(beg<0) beg = 0;

							if(end>num_verts) end = num_verts;
							for( ;beg<end; ++beg)
							{
								vertex_t nebr = pinst->buff[beg];
								if(sa[nebr] == INFTY)
								{
									sa[nebr]=level+1;
									if(front_count <= it->col_ranger_end - it->col_ranger_beg)
										it->front_queue[comp_tid][front_count] = nebr;
									front_count++;
								}
							}
						}
						++vert_id;

						if(vert_id >= it->row_ranger_end) break;
						if(beg_pos[vert_id - it->row_ranger_beg]
								- blk_beg_off > num_verts) 
							break;
					}

					pinst->status = EVICTED;
					assert(it->cd->circ_free_chunk->en_circle(chunk_id)!= -1);
				}
				
				//work-steal
			//	for(int ii = tid - (col_par * 2); ii <= tid + (col_par * 2) ; ii += (col_par *4))
			//	{
			//		if(ii < 0 || ii >= NUM_THDS) continue;
			//		IO_smart_iterator* it_work_steal = it_comm[ii];			
			//		while(true)
			//		{	
			//			int chunk_id = -1;
			//			double blk_tm = wtime();
			//			while((chunk_id = it_work_steal->cd->circ_load_chunk->de_circle())
			//					== -1)
			//			{
			//				if(it_work_steal->is_bsp_done)
			//				{
			//					chunk_id = it_work_steal->cd->circ_load_chunk->de_circle();
			//					break;
			//				}
			//			}
			//			it_work_steal->wait_io_time += (wtime() - blk_tm);

			//			if(chunk_id == -1) break;
			//			
			//			//printf("%dhelps%d-for%d\n", tid, ii, chunk_id);
			//			struct chunk *pinst = it_work_steal->cd->cache[chunk_id];	
			//			index_t blk_beg_off = pinst->blk_beg_off;
			//			index_t num_verts = pinst->load_sz;
			//			vertex_t vert_id = pinst->beg_vert;

			//			//process one chunk
			//			while(true)
			//			{
			//				if(sa[vert_id] == level)
			//				{
			//					index_t beg = it_work_steal->beg_pos_ptr[vert_id - it_work_steal->row_ranger_beg] 
			//						- blk_beg_off;
			//					index_t end = beg + it_work_steal->beg_pos_ptr[vert_id + 1 - 
			//						it_work_steal->row_ranger_beg]- 
			//						it_work_steal->beg_pos_ptr[vert_id - it_work_steal->row_ranger_beg];

			//					//possibly vert_id starts from preceding data block.
			//					//there by beg<0 is possible
			//					if(beg<0) beg = 0;

			//					if(end>num_verts) end = num_verts;
			//					for( ;beg<end; ++beg)
			//					{
			//						vertex_t nebr = pinst->buff[beg];
			//						if(sa[nebr] == INFTY)
			//						{
			//							sa[nebr]=level+1;
			//							if(front_count <= it->col_ranger_end - it->col_ranger_beg)
			//								it->front_queue[comp_tid][front_count] = nebr;
			//							front_count++;
			//						}
			//					}
			//				}
			//				++vert_id;

			//				if(vert_id >= it_work_steal->row_ranger_end) break;
			//				if(it_work_steal->beg_pos_ptr[vert_id - it_work_steal->row_ranger_beg]
			//						- blk_beg_off > num_verts) 
			//					break;
			//			}

			//			pinst->status = EVICTED;
			//			assert(it_work_steal->cd->circ_free_chunk->en_circle(chunk_id)!= -1);
			//		}
			//	}


				it->front_count[comp_tid] = front_count;
			}
			else
			{
				while(it->is_bsp_done == false)
				{
					//if(it->circ_free_buff->get_sz() == 0)
					//{
					//	printf("worked\n");
					//	int curr_buff = it->next(-1);
					//	assert(curr_buff != -1);
					//	neighbors = it -> buff_dest[curr_buff];
					//	index_t buff_edge_count = it -> buff_edge_count[curr_buff];
					//	//nebr_chk += buff_edge_count;
					//	for(long i = 0;i < buff_edge_count; i++)
					//	{
					//		vertex_t nebr = neighbors[i];
					//		if(sa[nebr]==INFTY)
					//		{
					//			//printf("new-front: %u\n", nebr);
					//			sa[nebr]=level+1;
					//			front_count++;
					//		}
					//	}
					//	it->circ_free_buff->en_circle(curr_buff);
					//}

					it->load_key(level);
					//it->load_key_iolist(level);
				}
				//printf("%d\n",tid);
				//it->load_key_iolist(level);
			}

			comm[tid] = front_count;
#pragma omp barrier
			front_count = 0;
			for(int i = 0 ;i< NUM_THDS; ++i)
				front_count += comm[i];

			ltm = wtime() - ltm;
			if(tid == 0) tm += ltm;
			
#pragma omp barrier
			comm[tid] = it->cd->fetch_sz;
#pragma omp barrier
			index_t total_sz = 0;
			for(int i = 0 ;i< NUM_THDS; ++i)
				total_sz += comm[i];
			total_sz >>= 1;//total size doubled
			
			if(!tid) std::cout<<"@level-"<<(int)level
				<<"-font-leveltime-converttm-iotm-waitiotm-waitcomptm-iosize: "
				<<front_count<<" "<<ltm<<" "<<convert_tm<<" "<<it->io_time
				<<"("<<it->cd->io_submit_time<<","<<it->cd->io_poll_time<<") "
				<<" "<<it->wait_io_time<<" "<<it->wait_comp_time<<" "
				<<total_sz<<"\n";
			
			if(front_count == 0 || level > 254) break;
			prev_front_count = front_count;
			front_count = 0;
			++level;
		}

#pragma omp barrier
		if(!tid) printf("Largest WCC is detected\n");
		for(vertex_t vert = beg_1d; vert < end_1d; vert ++)
			if(sa[vert] != INFTY)
				sa[vert] = 0;
		
		if((tid & 1) == 0)
			it->is_active = is_wcc_active;

		//Finish BFS, do Lable Propagation
		//WCC starts
		if(!tid) printf("Entering label propagation ...\n");
#pragma omp barrier
		while(true)
		{
			bool is_change_local=false;
			bool is_redirect_local=false;
			if((tid & 1) == 0)
			{
				it -> io_time = 0;
				it -> wait_io_time = 0;
				it -> wait_comp_time = 0;
				it -> cd -> io_submit_time = 0;
				it -> cd -> io_poll_time = 0;
				it -> cd -> fetch_sz = 0;
			}
			
			//- Framework gives user block to process
			//- Figures out what blocks needed next level
			double ltm=wtime();
		
			convert_tm = wtime();
			if((tid & 1) == 0)
			{
				it->req_translator(0);
				it->is_bsp_done = false;
			}
			else it->is_io_done = false;
			convert_tm = wtime() - convert_tm;

#pragma omp barrier
			
			if((tid & 1) == 0)
			{
				while(true)
				{	
					int chunk_id = -1;
					double blk_tm = wtime();
					while((chunk_id = it->cd->circ_load_chunk->de_circle())
							== -1)
					{
						if(it->is_bsp_done)
						{
							chunk_id = it->cd->circ_load_chunk->de_circle();
							break;
						}
					}
					it->wait_io_time += (wtime() - blk_tm);

					if(chunk_id == -1) break;
					struct chunk *pinst = it->cd->cache[chunk_id];	
					index_t blk_beg_off = pinst->blk_beg_off;
					index_t num_verts = pinst->load_sz;
					vertex_t vert_id = pinst->beg_vert;

					//process one chunk
					while(true)
					{
						if(sa[vert_id] != 0)
						{
							index_t beg = beg_pos[vert_id - it->row_ranger_beg] 
								- blk_beg_off;
							index_t end = beg + beg_pos[vert_id + 1 - 
								it->row_ranger_beg]- 
								beg_pos[vert_id - it->row_ranger_beg];

							//possibly vert_id starts from preceding data block.
							//there by beg<0 is possible
							if(beg<0) beg = 0;

							if(end>num_verts) end = num_verts;
							for( ;beg<end; ++beg)
							{
								dest = pinst->buff[beg];
								source = vert_id;
								sa_t dc=sa[dest];
								sa_t sc=sa[source];
								int swc = (sc!=INFTY) + ((dc!=INFTY)<<1);
								bool ret = false;

								switch(swc)
								{
									case 0://both without color
										is_change_local=true;
										color=__sync_fetch_and_add(&global_color,1);

										//possible COLOR POLUTION
										//while changing the color, maybe some other edgeds
										//all changes the color of source or dest
										sa[source] = sa[dest] = color;
										break;

									case 1://dest is not colored
										is_change_local=true;

										//possible COLOR POLUTION
										sa[dest]=sc;
										break;

										//source does not have color but dest has.
									case 2:
										is_change_local=true;
										sa[source]=dc;
										break;

									case 3:
										do{
											while(sc != color_redirect[sc])
												sc = color_redirect[sc];

											while(dc != color_redirect[dc])
												dc = color_redirect[dc];

											if(sc == dc)
												break;

											is_change_local = true;
											is_redirect_local = true;

											if(sc > dc)
											{
												ret = __sync_bool_compare_and_swap(
														color_redirect + sc, sc, dc);
											}
											else
											{
												ret = __sync_bool_compare_and_swap(
														color_redirect + dc, dc, sc);
											}
										}while(ret == false);

										break;

									default:
										break;
								}	
							}
						}
						++vert_id;

						if(vert_id >= it->row_ranger_end) break;
						if(beg_pos[vert_id - it->row_ranger_beg]
								- blk_beg_off > num_verts) 
							break;
					}

					pinst->status = EVICTED;
					assert(it->cd->circ_free_chunk->en_circle(chunk_id)!= -1);
				}
			}
			else
			{
				while(it->is_bsp_done == false)
				{
					//if(it->circ_free_buff->get_sz() == 0)
					//{
					//	printf("worked\n");
					//	int curr_buff = it->next(-1);
					//	assert(curr_buff != -1);
					//	neighbors = it -> buff_dest[curr_buff];
					//	index_t buff_edge_count = it -> buff_edge_count[curr_buff];
					//	//nebr_chk += buff_edge_count;
					//	for(long i = 0;i < buff_edge_count; i++)
					//	{
					//		vertex_t nebr = neighbors[i];
					//		if(sa[nebr]==INFTY)
					//		{
					//			//printf("new-front: %u\n", nebr);
					//			sa[nebr]=level+1;
					//			front_count++;
					//		}
					//	}
					//	it->circ_free_buff->en_circle(curr_buff);
					//}

					it->load_key(level);
					//it->load_key_iolist(level);
				}
			}
wcc_finish_point:
			is_redirect[tid]=is_redirect_local;
			is_change[tid]=is_change_local;

#pragma omp barrier
			for(index_t i=0;i<NUM_THDS;i++)
				if(is_redirect[i]) 
				{
					is_redirect_local=true;
					break;
				}

			for(index_t i=0;i<NUM_THDS;i++)
				if(is_change[i]) 
				{
					is_change_local=true;
					break;
				}
#pragma omp barrier
			
			ltm = wtime() - ltm;
			if(tid == 0) tm += ltm;

#pragma omp barrier
			comm[tid] = it->cd->fetch_sz;
#pragma omp barrier
			index_t total_sz = 0;
			for(int i = 0 ;i< NUM_THDS; ++i)
				total_sz += comm[i];
			total_sz >>= 1;//total size doubled
			
			if(!tid) std::cout<<"@level-"<<(int)level
				<<"-font-leveltime-converttm-iotm-waitiotm-waitcomptm-iosize: "
				<<front_count<<" "<<ltm<<" "<<convert_tm<<" "<<it->io_time
				<<"("<<it->cd->io_submit_time<<","<<it->cd->io_poll_time<<") "
				<<" "<<it->wait_io_time<<" "<<it->wait_comp_time<<" "
				<<total_sz<<"\n";

#pragma omp barrier
			//Check total number of unique color
			if(tid==0){
				int color_count = 0;
				for(index_t i = 0;i<global_color;i++)
				{
					if(color_redirect[i] == i)
						color_count++;
				}
				std::cout<<"WCC groups: "<<color_count<<"\n";
				//global_color=color_count;
			}
			
			if((is_change_local==false) && (is_redirect_local==false)) break;
			++level;
		}

		if(!tid) std::cout<<"Total time: "<<tm<<" second(s)\n";

		if((tid & 1) == 0) delete it;

	}
	munmap(sa,sizeof(sa_t)*vert_count);
	munmap(color_redirect,sizeof(sa_t)*vert_count);
	delete[] comm;
	delete[] is_change;
	delete[] is_redirect;
	return 0;
}
