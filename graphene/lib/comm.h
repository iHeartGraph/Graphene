#ifndef __COMM_H__
#define __COMM_H__

//#define _GNU_SOURCE
#include <omp.h>
#include "wtime.h"
#include <stdio.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <aio.h>
#include <iostream>
#include <signal.h>
#include <libaio.h>
#include <assert.h>
#include <sys/mman.h>
//#include "libaio.h"
#include <limits.h>
#include <sstream>

#define errExit(msg) do { perror(msg); exit(EXIT_FAILURE); } while (0)

//#define MAX_NUM_ELEMENTS (long)134217728 
#define EVICTED			8817//only cache driver can set this value
#define LOADING			8610//cache driver
#define LOADED			1997//cache driver
#define PROCESSING	8959//iterator 
#define PROCESSED		6364//iterator

inline off_t fsize(const char *filename) {
    struct stat st; 
    if (stat(filename, &st) == 0)
        return st.st_size;
//    fprintf(stdout, "Cannot determine size of %s: %s\n",
//            filename, strerror(errno));
    return -1; 
}

//system managed io request
//--system may enforces merge
struct chunk
{
	int status;//- define the chunks status. as EVICTED, LOADING ... 
	vertex_t beg_vert;//first vert whose neighbors 
												//- are stored in this chunk 
	index_t blk_beg_off;//the begin pos of this block
	vertex_t *buff;//buffer for loading data
	index_t load_sz;//-#verts loaded in this chunk
};

struct io_req 
{
	io_context_t ctx;//aio context
	struct iocb *io_cbp;//aio call-back function pointer
	index_t *chunk_id;
	index_t num_ios;
};

#define READ_BLK 			(1<<24)
//#define NUM_THDS  		16	
//#define BLK_SZ				512
//#define VERT_PER_BLK	64
#define MAX_EVENTS	  (unsigned int)64
#endif
