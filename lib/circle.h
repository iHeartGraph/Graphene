#ifndef __CIRCLE__
#define __CIRCLE__

#include "util.h"
#include "comm.h"

class circle
{
	public:
		int *array;
		int size;
		int head;
		int tail;
		volatile int lock_head;
		volatile int lock_tail;

		volatile int num_elem;

	public: 
		circle(){};
		circle(int size);
		~circle();

	public:
		int en_circle(int id);
		int de_circle();
		int get_sz();
		bool is_full();
		bool is_empty();
		void reset_circle();
};

#endif
