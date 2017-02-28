#ifndef _H_PIN_THREAD_
#define _H_PIN_THREAD_

#include <sched.h>

int pin_thread(
	int *core_id,
	int tid
){
	pthread_t thread;
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);

	CPU_SET(core_id[tid], &cpuset);
	thread = pthread_self();
	if(pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset))
		perror("pthread_setaffinity_pn");
	
	return 0;
}

int pin_thread_socket(int *core_id, int num_cores){

	pthread_t thread;
	cpu_set_t *cpuset = CPU_ALLOC(num_cores);
	if(cpuset == NULL)
	{
		perror("CPU_ALLOC");
		exit(-1);
	}

	CPU_ZERO_S(num_cores, cpuset);

	for(int i = 0; i < num_cores; i ++)
		CPU_SET_S(core_id[i], num_cores, cpuset);

	thread = pthread_self();
	if(pthread_setaffinity_np(thread, sizeof(cpu_set_t)*num_cores, cpuset))
		perror("pthread_setaffinity_pn");
	
	return 0;
}

#endif
