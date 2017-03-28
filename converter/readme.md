--------
Converter Specification
-------------

This converter tells you how to convert a text edge tuple into binary CSR format in parallel

----
Steps
---
- Create a folder to store dataset: **mkdir -p sample_dataset; cd sample_dataset**
- Download a text tuple list: **wget *https://snap.stanford.edu/data/bigdata/communities/com-orkut.ungraph.txt.gz***
- Uncompress the tuple list file: **gzip -d com-orkut.ungraph.txt.gz**
- To improve processing parallelism, you should split the file into a number of smaller files with our provided bash script: **../split_rename.bash com-orkut.ungraph.txt**
- Count number of smaller files we get from prior step: **ls -l ./com-orkut.ungraph.txt-split\* | wc -l**
- Convert those files into binary files: **../tuple_to_bin.multithread/text_to_bin.bin ./com-orkut.ungraph.txt-split 201 32 1**. Here you can type **../tuple_to_bin.multithread/text_to_bin.bin** to check the meanings of all input parameters.
- Convert those edge tuples into binary CSR: **../multi_bin_to_2d_csr/multi_bin_to_2d_csr.bin ./com-orkut.ungraph.txt-split 201 1 1 32**. Again, you can type **../multi_bin_to_2d_csr/multi_bin_to_2d_csr.bin** to see the meaning of each input parameter.
