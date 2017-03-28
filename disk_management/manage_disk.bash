#/bin/bash

echo "./exe row_par col_par dest_point_dir"
if [ ! $# -eq 3 ]
then
	echo "Wrong input"
	exit
fi

row_par=$1;
col_par=$2;
dest_dir=$3;
echo row_par=$row_par, col_par=$col_par, graph_dir=$dest_dir

mkdir -p $dest_dir 

str=$(/usr/bin/lsscsi | grep "Samsung SSD 850" | sed -e 's/^.*\///g' |sed ':a;N;$!ba;s/\n/ /g')
darr=(`echo ${str}`);
printf "Correct-list: "
/usr/bin/lsscsi | grep "Samsung SSD 850" | sed -e 's/^.*\///g' | sed ':a;N;$!ba;s/\n//g'
printf "My-List:      "
printf "%s " ${darr[@]}
echo

read -r -p "1. Match?; 2. prefix for content aware mount [y/N] " response
if [[ $response =~ ^([yY][eE][sS]|[yY])$ ]]
then
	echo you are good to go!
else
	exit;
fi

#partition a device
#for dev in ${darr[@]}
#do
#
#	umount /dev/"$dev"2
#	echo ------------------
#	echo "/dev/$dev"
#	echo --------------------------------
##	#To destroy the software raid metadata
##	
##
##	mdadm --zero-superblock	/dev/"$dev"1
##	mdadm --zero-superblock	/dev/"$dev"2
##	dd if=/dev/zero of=/dev/$dev bs=1M count=1024
##	echo -e "o\nd\nd\nd\nd\n\n\n\n\nw" | fdisk /dev/$dev #two primary partitions
###	
###	#fdisk /dev/$dev
###echo -e "o\nn\n\n\n\n\n\n\nw\w" | fdisk /dev/$dev #one primary partition
##	echo -e "o\nn\np\n1\n\n488386583\nn\np\n\n\n\nw" | fdisk /dev/$dev #two primary partitions
#	mkfs.ext4 /dev/"$dev"2
##	
##dd bs=512 if=/dev/zero of=/dev/sdq count=1024000 seek=$((`blockdev --getsz /dev/sdq` - 1024000))
##
#done
	
#for disk in ${darr[@]};
#do
#	echo ------------------
#	echo "/dev/$disk"
#	echo --------------------------------
#	dd bs=512 if=/dev/zero of=/dev/$disk count=1024000 seek=$((`blockdev --getsz /dev/$disk` - 1024000))
#
#	#	echo rm second partition @ $disk
#	parted /dev/$disk rm 1
#	parted /dev/$disk rm 2
##
##	echo increase the first partition @ $disk
##	parted /dev/$disk resizepart 1 400GB
#
##	echo resize the filesystem @ $disk
##	/sbin/e2fsck -f /dev/"$disk"1
##	/sbin/resize2fs /dev/"$disk"1
#
##	echo creating 2nd partition @ $disk
#	parted /dev/$disk mkpart primary ext4 0 400GB
#	parted /dev/$disk mkpart primary ext4 400GB 100% 
#done
#

#printf "\n\n\n"
echo "----------------------"
echo "Now do unmounting ... "
echo "----------------------"
for ((row=0;row<$row_par;row++))
do
	for ((col=0;col<$col_par;col++))
	do
		echo umount /dev/"${darr[$((row*col_par))+col]}"1
		umount /dev/"${darr[$((row*col_par))+col]}"1
	done
done

printf "\n\n\n"
echo "----------------------"
echo "Now do mounting ... "
echo "----------------------"
for ((row=0;row<$row_par;row++))
do
	for ((col=0;col<$col_par;col++))
	do
		echo "mount "${darr[$((row*col_par))+col]}"1 -> /home/hang/graph/2d/row_"$row"_col_"$col"/"
		mkdir -p "$dest_dir"/row_"$row"_col_"$col"/
		mount /dev/"${darr[$((row*col_par))+col]}"1 "$dest_dir"/row_"$row"_col_"$col"/
	done
done



#echo the mapping is possibly wrong, we are adjust the mappings
map=();

ptr=0;

printf "\n\n\n"
echo "----------------------"
echo "Figuring out correct disk <-> folder map ... "
echo "----------------------"
prefix=scale-30-degree-16-out_beg
#prefix=beg_28_16
echo "Constructing the dev map for adjusting"
for ((row=0;row<$row_par;row++))
do
	for ((col=0;col<$col_par;col++))
	do
		dir=$(dirname $(find "$dest_dir" -name $prefix"."$row"_"$col"_of_"$row_par"x"$col_par".bin"))
		dev=$(df -l | grep "$dir"|awk -F " " '{print $1}')
		echo $dir contains $prefix"."$row"_"$col"_of_"$row_par"x"$col_par".bin" "@" $dev
		map[$ptr]=$dev
		ptr=$((ptr+1))
	done
done

printf "\n\n\n"
echo "----------------------"
echo "Unmounting the incorrect map ... "
echo "----------------------"
for disk in ${map[@]}
do
	echo umount $disk
	umount $disk
done

ptr=0;
printf "\n\n\n"
echo "----------------------"
echo "Mounting the correct map ... "
echo "----------------------"
for ((row=0;row<$row_par;row++))
do
	for ((col=0;col<$col_par;col++))
	do
		echo mount ${map[$ptr]} "$dest_dir"/row_"$row"_col_"$col"
		mount ${map[$ptr]} "$dest_dir"/row_"$row"_col_"$col"
		#mount -o ro,noload ${map[$ptr]} "$dest_dir"/row_"$row"_col_"$col"
#		mount ${map[$ptr]} /home/hang/graph/flashgraph/ssd"$ptr"

		ptr=$((ptr+1))
	done
done

printf "\n\n\n"
echo "Done!"
