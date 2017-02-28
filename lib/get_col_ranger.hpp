#include "util.h"
#include "comm.h"

#ifndef _H_GET_COL_RANGER_
#define _H_GET_COL_RANGER_

inline index_t get_col_ranger(
		vertex_t* &col_ranger_ptr,
		vertex_t** &front_queue_ptr,
		index_t* &front_count_ptr,
		const char *beg_dir,
		const char *beg_header,
		const int num_rows,
		const int num_cols		
){

	front_queue_ptr = new vertex_t*[num_rows * num_cols];
	front_count_ptr = new index_t[num_rows * num_cols];
	col_ranger_ptr = new vertex_t[(num_cols + 1) * num_rows];

	//col_ranger_file
	char name[256];
	sprintf(name, "%s/row_0_col_0/%s-%dx%d-col-ranger.bin", 
			beg_dir, beg_header, num_rows, num_cols);
	FILE *file = fopen(name, "rb");

	if(file != NULL)
	{
		printf("col-ranger file is found\n");
		size_t ret = fread(col_ranger_ptr, sizeof(vertex_t), 
				(num_cols + 1) * num_rows, file);
		assert(ret == (num_cols + 1) * num_rows);
		fclose(file);
	}
	else
	{
		printf("%s\n",name);
		perror("col-ranger file fopen");
		exit(-1);
	}
}

#endif
