//kcore_queue has possible problem 
	//for(vertex_t vert = beg_1d; vert < end_1d; vert ++) may not be as  
	//for(vertex_t vert=it->col_ranger_beg;vert<it->col_ranger_end;vert++)
//----------------
//be careful
//we cannot easily add from there at initial step

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

inline bool is_active
(index_t vert_id,
sa_t criterion,
sa_t *sa, sa_t *sa_prev)
{
	//Maybe multiple partition work on the same vert
	//Thereby, we cannot only count active vertex once
	
	return (sa[vert_id] == ACTIVE);
	//	if(sa[vert_id] <= criterion && sa[vert_id] != INFTY)
//	{
//		if(sa[vert_id] != ACTIVE) sa[vert_id] = ACTIVE;
//		return true;
//	}
//	else 
//		return false;
}


int main(int argc, char **argv) 
{
	std::cout<<"Format: /path/to/exe " 
		<<"#row_partitions #col_partitions thread_count "
		<<"/path/to/beg_pos_dir /path/to/csr_dir "
		<<"beg_header csr_header num_chunks "
		<<"chunk_sz (#bytes) concurr_IO_ctx "
		<<"max_continuous_useless_blk "
		<<"ring_vert_count num_buffs k (report > k)\n";
	
	if(argc != 15)
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
	const sa_t criterion = atoi(argv[14]);

	sa_t *sa = NULL;
	sa_t *sa_prev = NULL;
	assert(NUM_THDS==(row_par*col_par*2));
	vertex_t **front_queue_ptr;
	index_t *front_count_ptr;
	vertex_t *col_ranger_ptr;
	
	index_t *comm = new index_t[NUM_THDS];
	const index_t vert_count=get_vert_count
		(comm, beg_dir,beg_header,row_par,col_par);
	get_col_ranger(col_ranger_ptr, front_queue_ptr,
			front_count_ptr, beg_dir, beg_header,
			row_par, col_par);
	
	printf(" > %d K-core computation ...\n", criterion);
	sa=(sa_t *)mmap(NULL,sizeof(sa_t)*vert_count,
			PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS 
			| MAP_HUGETLB | MAP_HUGE_2MB, 0, 0);
	if(sa==MAP_FAILED)
	{	
		perror("mmap");
		exit(-1);
	}
	int *semaphore_acq = new int[1];
	int *semaphore_flag = new int[1];

	semaphore_acq[0] = 0;
	semaphore_flag[0] = 0;

	const index_t vert_per_blk = chunk_sz / sizeof(vertex_t);
	if(chunk_sz&(sizeof(vertex_t) - 1))
	{
		std::cout<<"Page size wrong\n";
		exit(-1);
	}

	char cmd[256];
	sprintf(cmd,"%s","iostat -x 1 -k > iostat_kcore.log&");
	std::cout<<cmd<<"\n";
	//exit(-1);
	//omp_lock_t gpu_semaphore;
	//omp_init_lock(&gpu_semaphore);
	//0 1 2 3 4 5 6 7 8 9 10 11 12 13 28 29 30 31 32 33 34 35 36 37 38 39 40 41
	//14 15 16 17 18 19 20 21 22 23 24 25 26 27 42 43 44 45 46 47 48 49 50 51 52 53 54 55
	//int core_id[8]={0, 2, 4, 6, 14, 16, 18, 20};
	int core_id[16]={0, 2, 4, 12, 14, 16, 1, 3, 6, 8, 10, 18, 20, 22, 7, 9};
	//int core_id[16]={0, 2, 4, 6, 8, 10, 12, 28, 14, 16, 18, 20, 22, 24, 26, 42};
	//int core_id[16]={0, 2, 4, 6, 8, 10, 12, 28, 30, 32, 34, 36, 38, 40, 1, 3};
	IO_smart_iterator **it_comm = new IO_smart_iterator*[NUM_THDS];
	for(index_t i=0;i<vert_count;i++) sa[i]=0;
	double tm = 0;

#pragma omp parallel \
	num_threads (NUM_THDS) \
	shared(sa,comm)
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
		
		index_t step_1d = vert_count / NUM_THDS;
		index_t beg_1d = step_1d * tid;
		index_t end_1d = beg_1d + step_1d;
		if(tid == NUM_THDS-1) end_1d = vert_count;

		comp_t *neighbors;
		index_t *beg_pos;
		//pin_thread(core_id,tid);
		
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
						sa,sa_prev,beg_pos,
						num_buffs,
						ring_vert_count,
						MAX_USELESS,
						io_limit,
						&is_active);

			it_comm[tid] = it_temp;
			it_comm[tid]->is_bsp_done = false;
		}
#pragma omp barrier
		
		IO_smart_iterator *it = it_comm[(tid>>1)<<1];
		
		//if(!tid) system((const char *)cmd);
#pragma omp barrier

		if(tid==0) std::cout<<"Aggregate degree ... \n";	
		
		if((tid&1) == 0) 
			for(vertex_t vert=it->row_ranger_beg;
					vert<it->row_ranger_end;vert++)
			{
				int my_degree=(int)(it->beg_pos_ptr[vert+1-it->row_ranger_beg]-
						it->beg_pos_ptr[vert-it->row_ranger_beg]);
				__sync_fetch_and_add(it->sa_ptr + vert,my_degree);
			}

#pragma omp barrier
		index_t front_count = 0;

		if((tid&1) == 0) 
		{
			for(vertex_t vert=it->col_ranger_beg;vert<it->col_ranger_end;vert++)
			{
				if(it->sa_ptr[vert] <= criterion) 
				{
					it->sa_ptr[vert] = ACTIVE;
					it->front_queue[comp_tid][front_count] = vert;
					front_count ++;
				}
			}
			it->front_count[comp_tid] = front_count;
		}
#pragma omp barrier
		
		if(tid==0) std::cout<<"Start deleting ... \n";	
		double convert_tm=0;
		while(true)
		{
			//- Framework gives user block to process
			//- Figures out what blocks needed next level
			index_t prev_front_count = front_count;
			front_count = 0;
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
#pragma omp barrier
			if((tid & 1) == 0)
			{
				it->is_bsp_done = false;

				convert_tm=wtime();
				if((prev_front_count * 100.0)/ vert_count > 2.0) 
					it->req_translator(level);
				else it->req_translator_queue();
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
						if(sa[vert_id] == ACTIVE)
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

								//1. Never touch "ACTIVE" or "INFTY" verts
								//2. Very unlikely to sub a vert to 
								//   "ACTIVE" or "INFTY"
								//3. > criterion, by default, means NOT 
								//	 "ACTIVE" or "INFTY" verts
								if(sa[nebr] > criterion)
								{
									__sync_fetch_and_sub(it->sa_ptr + nebr, 1);
									if(it->sa_ptr[nebr] <= criterion)
									{
										it->front_queue[comp_tid][front_count] = nebr;
										front_count++;
									}
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
				for(int ii = tid - (col_par * 2); ii <= tid + (col_par * 2) ; ii += (col_par *4))
				{
					if(ii < 0 || ii >= NUM_THDS) continue;
					IO_smart_iterator* it_work_steal = it_comm[ii];			
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
							if(sa[vert_id] == ACTIVE)
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

									//1. Never touch "ACTIVE" or "INFTY" verts
									//2. Very unlikely to sub a vert to 
									//   "ACTIVE" or "INFTY"
									//3. > criterion, by default, means NOT 
									//	 "ACTIVE" or "INFTY" verts
									if(sa[nebr] > criterion)
									{
										__sync_fetch_and_sub(it->sa_ptr + nebr, 1);
										if(it->sa_ptr[nebr] <= criterion)
										{
											it->front_queue[comp_tid][front_count] = nebr;
											front_count++;
										}
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
			}

finish_point:
			//To decide exit or not
			comm[tid] = front_count;

#pragma omp barrier
			for(vertex_t vert = beg_1d; vert < end_1d; vert ++)
				if(it->sa_ptr[vert] == ACTIVE) it->sa_ptr[vert]=INFTY;

#pragma omp barrier
			for(vertex_t vert = beg_1d; vert < end_1d; vert ++)
				if(it->sa_ptr[vert] <= criterion && 
				   it->sa_ptr[vert] != INFTY) 
					it->sa_ptr[vert] = ACTIVE;

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
			
			if(front_count == 0) break;
			
			prev_front_count = front_count;
			front_count = 0;
			++level;
		}

		//if(!tid)system("killall iostat");

		if(!tid) std::cout<<"Total time: "<<tm<<" second(s)\n";
		if((tid & 1) == 0) delete it;
	}

	index_t kcount=0;
	for(index_t i=0;i<vert_count;i++)
		if(sa[i]>criterion) kcount++;

	std::cout<<"K-core of K > "<<criterion<<" count: "<<kcount<<"\n";

	munmap(sa,sizeof(sa_t)*vert_count);
	delete[] comm;
	return 0;
}
