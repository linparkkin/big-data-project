#!bash

# dataset /data/vectors-50-1000000.txt.bz2
spark-submit --total-executor-cores 4  --executor-cores 2 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-1000000.txt.bz2 20 32 >> 4_2_1M_20_32.log 

spark-submit --total-executor-cores 8  --executor-cores 2 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-1000000.txt.bz2 20 32 >> 8_2_1M_20_32.log 

spark-submit --total-executor-cores 8  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-1000000.txt.bz2 20 32 >> 8_4_1M_20_32.log 

spark-submit --total-executor-cores 8  --executor-cores 8 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-1000000.txt.bz2 20 32 >> 8_8_1M_20_32.log

spark-submit --total-executor-cores 16  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-1000000.txt.bz2 20 32 >> 16_4_1M_20_32.log

spark-submit --total-executor-cores 16  --executor-cores 8 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-1000000.txt.bz2 20 32 >> 16_8_1M_20_32.log

spark-submit --total-executor-cores 32  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-1000000.txt.bz2 20 32 >> 32_4_1M_20_32.log

spark-submit --total-executor-cores 32  --executor-cores 8 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-1000000.txt.bz2 20 32 >> 32_8_1M_20_32.log

spark-submit --total-executor-cores 64  --executor-cores 8 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-1000000.txt.bz2 20 32 >> 64_8_1M_20_32.log

spark-submit --total-executor-cores 64  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-1000000.txt.bz2 20 32 >> 64_4_1M_20_32.log



# dataset /data/vectors-50-2000000.txt.bz2
spark-submit --total-executor-cores 4  --executor-cores 2 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-2000000.txt.bz2 20 32 >> 4_2_2M_20_32.log 

spark-submit --total-executor-cores 8  --executor-cores 2 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-2000000.txt.bz2 20 32 >> 8_2_2M_20_32.log 

spark-submit --total-executor-cores 8  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-2000000.txt.bz2 20 32 >> 8_4_2M_20_32.log 

spark-submit --total-executor-cores 8  --executor-cores 8 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-2000000.txt.bz2 20 32 >> 8_8_2M_20_32.log

spark-submit --total-executor-cores 16  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-2000000.txt.bz2 20 32 >> 16_4_2M_20_32.log

spark-submit --total-executor-cores 16  --executor-cores 8 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-2000000.txt.bz2 20 32 >> 16_8_2M_20_32.log

spark-submit --total-executor-cores 32  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-2000000.txt.bz2 20 32 >> 32_4_2M_20_32.log

spark-submit --total-executor-cores 32  --executor-cores 8 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-2000000.txt.bz2 20 32 >> 32_8_2M_20_32.log

spark-submit --total-executor-cores 64  --executor-cores 8 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-2000000.txt.bz2 20 32 >> 64_8_2M_20_32.log

spark-submit --total-executor-cores 64  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-2000000.txt.bz2 20 32 >> 64_4_2M_20_32.log


# dataset /data/vectors-50-3000000.txt.bz2
spark-submit --total-executor-cores 4  --executor-cores 2 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-3000000.txt.bz2 20 32 >> 4_2_3M_20_32.log 

spark-submit --total-executor-cores 8  --executor-cores 2 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-3000000.txt.bz2 20 32 >> 8_2_3M_20_32.log 

spark-submit --total-executor-cores 8  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-3000000.txt.bz2 20 32 >> 8_4_3M_20_32.log 

spark-submit --total-executor-cores 8  --executor-cores 8 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-3000000.txt.bz2 20 32 >> 8_8_3M_20_32.log

spark-submit --total-executor-cores 16  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-3000000.txt.bz2 20 32 >> 16_4_3M_20_32.log

spark-submit --total-executor-cores 16  --executor-cores 8 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-3000000.txt.bz2 20 32 >> 16_8_3M_20_32.log

spark-submit --total-executor-cores 32  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-3000000.txt.bz2 20 32 >> 32_4_3M_20_32.log

spark-submit --total-executor-cores 32  --executor-cores 8 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-3000000.txt.bz2 20 32 >> 32_8_3M_20_32.log

spark-submit --total-executor-cores 64  --executor-cores 8 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-3000000.txt.bz2 20 32 >> 64_8_3M_20_32.log

spark-submit --total-executor-cores 64  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-3000000.txt.bz2 20 32 >> 64_4_3M_20_32.log



# dataset /data/vectors-50-500000.txt.bz2
spark-submit --total-executor-cores 4  --executor-cores 2 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-500000.txt.bz2 20 32 >> 4_2_500k_20_32.log 

spark-submit --total-executor-cores 8  --executor-cores 2 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-500000.txt.bz2 20 32 >> 8_2_500k_20_32.log 

spark-submit --total-executor-cores 8  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-500000.txt.bz2 20 32 >> 8_4_500k_20_32.log 

spark-submit --total-executor-cores 8  --executor-cores 8 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-500000.txt.bz2 20 32 >> 8_8_500k_20_32.log

spark-submit --total-executor-cores 16  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-500000.txt.bz2 20 32 >> 16_4_500k_20_32.log

spark-submit --total-executor-cores 16  --executor-cores 8 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-500000.txt.bz2 20 32 >> 16_8_500k_20_32.log

spark-submit --total-executor-cores 32  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-500000.txt.bz2 20 32 >> 32_4_500k_20_32.log

spark-submit --total-executor-cores 32  --executor-cores 8 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-500000.txt.bz2 20 32 >> 32_8_500k_20_32.log

spark-submit --total-executor-cores 64  --executor-cores 8 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-500000.txt.bz2 20 32 >> 64_8_500k_20_32.log

spark-submit --total-executor-cores 64  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-500000.txt.bz2 20 32 >> 64_4_500k_20_32.log

# dataset /data/vectors-50-all.txt.bz2

spark-submit --total-executor-cores 4  --executor-cores 2 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-all.txt.bz2 20 32 >> 4_2_all_20_32.log 

spark-submit --total-executor-cores 8  --executor-cores 2 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-all.txt.bz2 20 32 >> 8_2_all_20_32.log 

spark-submit --total-executor-cores 8  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-all.txt.bz2 20 32 >> 8_4_all_20_32.log 

spark-submit --total-executor-cores 8  --executor-cores 8 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-all.txt.bz2 20 32 >> 8_8_all_20_32.log

spark-submit --total-executor-cores 16  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-all.txt.bz2 20 32 >> 16_4_all_20_32.log

spark-submit --total-executor-cores 16  --executor-cores 8 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-all.txt.bz2 20 32 >> 16_8_all_20_32.log

spark-submit --total-executor-cores 32  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-all.txt.bz2 20 32 >> 32_4_all_20_32.log

spark-submit --total-executor-cores 32  --executor-cores 8 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-all.txt.bz2 20 32 >> 32_8_all_20_32.log

spark-submit --total-executor-cores 64  --executor-cores 8 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-all.txt.bz2 20 32 >> 64_8_all_20_32.log

spark-submit --total-executor-cores 64  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-all.txt.bz2 20 32 >> 64_4_all_20_32.log

#increasing partitions
spark-submit --total-executor-cores 32  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-all.txt.bz2 20 4 >> 32_4_all_20_4.log

spark-submit --total-executor-cores 32  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-all.txt.bz2 20 8 >> 32_4_all_20_8.log

spark-submit --total-executor-cores 32  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-all.txt.bz2 20 16 >> 32_4_all_20_16.log

spark-submit --total-executor-cores 32  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-all.txt.bz2 20 32 >> 32_4_all_20_32.log

spark-submit --total-executor-cores 32  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-all.txt.bz2 20 64 >> 32_4_all_20_64.log

spark-submit --total-executor-cores 32  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-all.txt.bz2 20 128 >> 32_4_all_20_128.log

spark-submit --total-executor-cores 32  --executor-cores 4 --class it.unipd.dei.bdc1718.G18HM4 HW4-all.jar /data/vectors-50-all.txt.bz2 20 256 >> 32_4_all_20_256.log


