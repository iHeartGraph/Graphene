#include "util.h"
#include "comm.h"

#ifndef _H_GET_VERT_COUNT_
#define _H_GET_VERT_COUNT_

inline index_t get_vert_count(
		index_t *comm,
		const char *beg_dir,
		const char *beg_header,
		const int row_par,
		const int col_par
){
	char filename[256];
	index_t vert_count=0;
	for(int i=0; i < row_par; i++)
	{
		for(int j = 0; j < col_par; j ++)
		{
			sprintf(filename,"%s/row_%d_col_%d/%s.%d_%d_of_%dx%d.bin",
					beg_dir,i,j,beg_header,i,j,row_par,col_par);

			//printf("file: %s\n",filename);

			//beg_pos is always 1 more than vert_count
			off_t fsz=fsize(filename);
			if(fsz==-1)
			{
				printf("%s wrong\n", filename);
			}

			comm[i * col_par + j] = fsz / sizeof(index_t) - 1;
			vert_count += comm[i * col_par + j];
		}
	}
	
	vert_count /= col_par;
	printf("Vertex count: %ld\n", vert_count);
	
	return vert_count;
}

#endif
