exe = aio_wcc_hybrid.bin

cc = "$(shell which g++)" 
cucc= "$(shell which nvcc)"


#cuflags= -gencode=arch=compute_35,code=\"sm_35,compute_35\"  -Xptxas -v -Xcudafe -\# -Xptxas -abi=no -m64 -I"/usr/local/cuda/bin/../include" -I. -I../../../ -I../../../b40c -O3 -D_FORCE_INLINES
flags = -I../../lib/ -I. -laio -fopenmp -O3


flags+=-DKV_BOTH_SORTING -DWCC_KV -DBLK_SZ=512 

ifeq ($(time), 1)
  cuflags+= -DTIMING
	flags += -DTIMING
endif

ifeq ($(profile),1)
	cuflags+=-DPROFILE
	flags+=-DPROFILE
endif
objs = $(patsubst %.cpp,%.o,$(wildcard ../../lib/*.cpp)) \
			$(patsubst %.cu,%.o,$(wildcard ../../lib/*.cu)) \
			$(patsubst %.cpp,%.o,$(wildcard *.cpp))

deps =	$(wildcard ../../lib/*.h) \
				$(wildcard ../../lib/*.cuh) \
				$(wildcard *.h) \
				$(wildcard *.hpp) \
				$(wildcard *.cuh) \
				Makefile

#flags+=-L$(shell dirname $(cucc))/../lib64 -lcuda -lcudart

%.o:%.cpp $(deps)
	$(cc) -c $< -o $@ $(flags)

%.o:%.cu $(deps)
	$(cucc) -c $< -o $@ $(cuflags)

$(exe):$(objs)
	$(cc) $(objs) -o $(exe) $(flags)

test:$(exe)
	./aio_bfs 65536 2097152 /home/hang/scale_16/beg_16_16.bin /home/hang/scale_16/csr_16_16.bin 128 65536 

clean:
	rm -rf $(exe) $(objs) 
