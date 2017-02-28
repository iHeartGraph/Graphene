#!/bin/bash
#-b is the block size typically: 4K = 4096
#stride = chunks size / block size = 32k / 4k = 8
#stripe-width = stride * number of disks = 8 * 8 = 64

#mkdir -p log
#for(( thd=1; thd <= 64; thd = thd*2))
#do
#	umount  /mnt/graph
#	mdadm --stop /dev/md0
#	echo "chunk size = 32"
#	mdadm --create  /dev/md0 --raid-devices=8 --level=0  --chunk=32 --metadata=1.2 /dev/sda /dev/sdb /dev/sdc /dev/sdd /dev/sdg /dev/sdh /dev/sdi /dev/sdj --run
#	sfdisk /dev/md0 < my.layout 
#	mkfs.ext4 -v -m 0.1 -b 4096 -E stride=8,stripe-width=64 /dev/md0p1
#	mount /dev/md0p1 /mnt/graph
#	cp -r /home/hang/scale_25 /mnt/graph/

#	str="#define NUM_THDS $thd"
#	sed -i "s/#define NUM_THDS .*$/$str/g" aio_omp_bfs.h 
#	grep "$str" aio_omp_bfs.h
	make
	free && sync && echo 3 > /proc/sys/vm/drop_caches && free
	#echo 3 > /proc/sys/vm/drop_caches
	#./tc -s25 -i /mnt/graph/graph_pradeep/csr_25_16.dat -c1 -o /mnt/g/csr_25_16.dat -t0
	#free && sync && echo 3 > /proc/sys/vm/drop_caches && free
	iostat 1 -x > log/iostat_28_1024chunk_128_limit.log &
	#./aio_bfs 25 16 /home/hang/scale_25/ /mnt/graph/ 256 8192 8192
	#./aio_bfs 25 16 /mnt/graph/2d/ /mnt/graph/2d 4 2 256 8192 8192 
	#./aio_bfs 1073741824 4 4 /mnt/graph/2d/ /mnt/graph/2d/ 128 65536
	./aio_bfs 268435456 28 16 4 2 /mnt/graph/2d/ /mnt/graph/2d/ 1024 65536 128
	#numactl --cpunodebind=1 --membind=1 ./aio_bfs 25 16 /mnt/graph/2d/ /mnt/graph/2d/ 4 2 128 8192 8192
	killall -w  iostat
#done
