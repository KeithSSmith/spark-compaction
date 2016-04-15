# Spark Compaction
When streaming data into HDFS, small messages are written to a large number of files that if left unchecked will cause unnecessary strain on the HDFS NameNode.  To handle this situation, it is good practice to run a compaction job on directories that contain many small files to help reduce the resource strain of the NameNode by ensuring HDFS blocks are filled efficiently.  It is common practice to do this type of compaction with MapReduce or on Hive tables / partitions and this tool is designed to accomplish the same type of task utilizing Spark.


## Compression Math

At a high level this class will calculate the number of output files to efficiently fill the default HDFS block size on the cluster taking into consideration the size of the data, compression type, and serialization type.

**Compression Ratio Assumptions**
```vim
SNAPPY_RATIO = 1.7;     // (100 / 1.7) = 58.8 ~ 40% compression rate on text
LZO_RATIO = 2.0;        // (100 / 2.0) = 50.0 ~ 50% compression rate on text
GZIP_RATIO = 2.5;       // (100 / 2.5) = 40.0 ~ 60% compression rate on text
BZ2_RATIO = 3.33;       // (100 / 3.3) = 30.3 ~ 70% compression rate on text

AVRO_RATIO = 1.6;       // (100 / 1.6) = 62.5 ~ 40% compression rate on text
PARQUET_RATIO = 2.0;    // (100 / 2.0) = 50.0 ~ 50% compression rate on text
```

**Compression Ratio Formula**
```vim
Input Compression Ratio * Input Serialization Ratio * Input File Size = Input File Size Inflated
Input File Size Inflated / ( Output Compression Ratio * Output Serialization Ratio ) = Output File Size
Output File Size / Block Size of Output Directory = Number of Blocks Filled
FLOOR( Number of Blocks Filled ) + 1 = Efficient Number of Files to Store
```


### Text Compaction

**Text to Text Calculation**
```vim
Read Input Directory Total Size = x
Detect Output Directory Block Size = 134217728 => y

Output Files: FLOOR( x / y ) + 1 = # of Mappers
```

**Text to Text Snappy Calculation**
```vim
Default Block Size = 134217728 => y
Read Input Directory Total Size = x
Compression Ratio = 1.7 => r

Output Files: FLOOR( x / (r * y) ) + 1 = # of Mappers
```


**Execution Options**

```vim
spark-submit \
  --class com.cloudera.spark_compaction.Compact \
  --master local[2] \
  ${JAR_PATH}/spark-compaction-0.0.1-SNAPSHOT.jar \
  --input-path ${INPUT_PATH} \
  --output-path ${OUTPUT_PATH} \
  --input-compression [none snappy gzip bz2 lzo] \
  --input-serialization [text parquet avro] \
  --output-compression [none snappy gzip bz2 lzo] \
  --output-serialization [text parquet avro]
```

**Execution Example (Text to Text):**

```vim
spark-submit \
  --class com.cloudera.spark_compaction.Compact \
  --master local[2] \
  ${JAR_PATH}/spark-compaction-0.0.1-SNAPSHOT.jar \
  -i ${INPUT_PATH} \
  -o ${OUTPUT_PATH} \
  -ic [input_compression] \
  -is [input_serialization] \
  -oc [output_compression] \
  -os [output_serialization]

spark-submit \
  --class com.cloudera.spark_compaction.Compact \
  --master local[2] \
  ~/cloudera/jars/spark-compaction-0.0.1-SNAPSHOT.jar \
  -i /landing/compaction/input \
  -o /landing/compaction/output \
  -ic none \
  -is text \
  -oc none \
  -os text
```

To elaborate further, the following example has an input directory consisting of 9,999 files consuming 440 MB of space.  Using the default block size, the resulting output files are 146 MB in size, easily fitting into a data block.

**Input (Text to Text)**
```vim
$ hdfs dfs -du -h /landing/compaction/input | wc -l
9999
$ hdfs dfs -du -h /landing/compaction
440.1 M  440.1 M  /landing/compaction/input

440.1 * 1024 * 1024 = 461478298 - Input file size.
461478298 / 134217728 = 3.438
FLOOR( 3.438 ) + 1 = 4 files

440.1 MB / 4 files ~ 110 MB
```

**Output (Text to Text)**
```vim
$ hdfs dfs -du -h /landing/compaction/output
110.0 M  110.0 M  /landing/compaction/output/part-00000
110.0 M  110.0 M  /landing/compaction/output/part-00001
110.0 M  110.0 M  /landing/compaction/output/part-00002
110.0 M  110.0 M  /landing/compaction/output/part-00003
```


### Parquet Compaction

**Input (Parquet Snappy to Parquet Snappy)**
```vim
$ hdfs dfs -du -h /landing/compaction/input_snappy_parquet | wc -l
9999
$ hdfs dfs -du -h /landing/compaction
440.1 M  440.1 M  /landing/compaction/input_snappy_parquet

440.1 * 1024 * 1024 = 461478298 - Total input file size.
1.7 * 2 = 3.4 - Total compression ratio.
(3.4 * 461478298) / (3.4 * 134217728) = 3.438
FLOOR( 3.438 ) + 1 = 4 files

440.1 MB / 2 files ~ 110 MB
```

**Output (Parquet Snappy to Parquet Snappy)**
```vim
$ hdfs dfs -du -h /landing/compaction/output_snappy_parquet
110 M  110 M  /landing/compaction/output_snappy_parquet/part-00000.snappy.parquet
110 M  110 M  /landing/compaction/output_snappy_parquet/part-00001.snappy.parquet
110 M  110 M  /landing/compaction/output_snappy_parquet/part-00002.snappy.parquet
110 M  110 M  /landing/compaction/output_snappy_parquet/part-00003.snappy.parquet
```



## Multiple Directory Compaction

**Sub Directory Processing**
```vim
$ hdfs dfs -du -h /landing/compaction/partition/
293.4 M  293.4 M  /landing/compaction/partition/date=2016-01-01

$ hdfs dfs -du -h /landing/compaction/partition/date=2016-01-01
48.9 M  48.9 M  /landing/compaction/partition/date=2016-01-01/hour=00
48.9 M  48.9 M  /landing/compaction/partition/date=2016-01-01/hour=01
48.9 M  48.9 M  /landing/compaction/partition/date=2016-01-01/hour=02
48.9 M  48.9 M  /landing/compaction/partition/date=2016-01-01/hour=03
48.9 M  48.9 M  /landing/compaction/partition/date=2016-01-01/hour=04
48.9 M  48.9 M  /landing/compaction/partition/date=2016-01-01/hour=05

$ hdfs dfs -du -h /landing/compaction/partition/date=2016-01-01/* | wc -l
6666
```

**Output Directory**
```vim
$ hdfs dfs -du -h /landing/compaction/partition/output_2016-01-01
293.4 M  293.4 M  /landing/compaction/partition/output_2016-01-01

$ hdfs dfs -du -h /landing/compaction/partition/output_2016-01-01/* | wc -l
3
```

**Wildcard for Multiple Sub Directory Compaction**
```vim
spark-submit \
  --class com.cloudera.spark_compaction.Compact \
  --master local[2] \
  ~/cloudera/jars/spark-compaction-0.0.1-SNAPSHOT.jar \
  --input-path /landing/compaction/partition/date=2016-01-01/hour=* \
  --output-path /landing/compaction/partition/output_2016-01-01 \
  --input-compression none \
  --input-serialization text \
  --output-compression none \
  --output-serialization text
```
