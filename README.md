# Cloud_Computing_PA2
TeraSort application implementation in C-programming, Hadoop and Spark (CS553) using Amazon EC2

steps to run :
gcc externalSort.c -o sort -lpthread
./gensort -a 1300000000 inputfile_128GB
./sort -n 13 -p 100000000 -t 2 -s 1
./gensort -a 10000000000 inputfile_1TB
./sort -n 10 -p 1000000000 -t 16 -s 2
./sort -n 10 -p 1000000000 -t 24 -s 3

Note: thread count needs a check on actual machine

part1(sort 128GB dataset) :
Instance=i3.large -> 15.25GB memory
Number of partitions = 13 
Each partition size = 10GB
Number of ASCII records in each file = 10GB/100Bytes = 100000000 

part2(sort 1TB dataset) :
Instance=i3.4xlarge -> 122GB memory
Number of partitions = 10 
Each partition size = 100GB
Number of ASCII records in each file = 100GB/100Bytes = 1000000000 

part3(sort 1TB dataset on 8 nodes) :
Instance=i3.4xlarge -> 15.25GB memory
Number of nodes=8; Therefore total memory = 15.25*8 = 122GB
Number of partitions = 10 
Each partition size = 100GB
Number of ASCII records in each file = 100GB/100Bytes = 1000000000 
