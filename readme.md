----
Specification
-----
To run graphene, you need to do three things: **partition a graph**, **format and mount disks and distribute graph data on disks**, **run graphene**. These three steps also constitute the entire Graphene project structure as below.

----
Project structure
-----
- **converter**: convert a text tuple list into *row-column balanced 2d partition* format of the graph.
- **disk_management**: format and mount disks to the server and further distribute the partitioned dataset into these disks.
- **Graphene** contains *lib and test* two source code.
Please find the detailed specification in the sub folders.


**Should you have any questions about this project, please contact us by iheartgraph@gmail.com.**

-----
Reference
-------
[FAST '17] Graphene: Fine-Grained IO Management for Graph Computing[[PDF](https://www.usenix.org/system/files/conference/fast17/fast17-liu.pdf)] [[Slides](https://www.usenix.org/sites/default/files/conference/protected-files/fast17_slides_liu.pdf)]

