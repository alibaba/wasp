Wasp[1]:Megastore[2]&F1[3]-like system
With the development of NoSQL, HBase gradually become the mainstream of the NoSQL system 
products. The advantages of HBase is very obvious, but defect is also very obvious. 
These weaknesses include large data platform business by SQL to NoSQL migration is more 
complex and application personnel learning cost is quite high, 
can't support affairs and multidimensional index, eventually making 
many business can't enjoy from NoSQL system linear development ability. 
Google internal MegaStore system complements Bigtable,it supports SQL, transactions, 
indexing, cross-cluster replication in the upper layer of the Bigtable, 
and became famous applications's storage engine, such as Gmail, APPEngine, 
and the Android Market.Therefore, we decided to explore providing cross-row transactions, 
indexes, SQL function without sacrificing the linear expansion of capacity 
in the upper layer of the HBase by theoretical model MegaStore. 
The system provides simple user interface: SQL, the user can do not need to pay attention to 
the hbase schema design, greatly simplifies the user's data migration and learning costs. 
To see what's supported, go to our language reference guide, and read more on our wiki[4].

Mission
Become a standard distributed relational database,which's storage engine is nosql system, 
for example the hbase.

The latest Wasp can be downloaded from an Apache Mirror [5].

The source code can be found at [6]

The Wasp issue tracker is at [7]

Apache Wasp is made available under the Apache License, version 2.0 [8]

The Wasp mailing lists and archives are listed here [9].

[1] https://github.com/alibaba/wasp
[2] http://research.google.com/pubs/pub36971.html
[3] http://research.google.com/pubs/pub38125.html
[4] https://github.com/alibaba/wasp/wiki
[5]
[6]
[7]
[8]
[9]