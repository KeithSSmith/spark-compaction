package com.cloudera.spark_compaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class Compact {

    private static final String AVRO = "avro";
    private static final String BLOCK = "BLOCK";
    private static final String BZ2 = "bz2";
    private static final String GZIP = "gzip";
    private static final String LZO = "lzo";
    private static final String NONE = "none";
    private static final String PARQUET = "parquet";
    private static final String SNAPPY = "snappy";
    private static final String SHOULD_COMPRESS_OUTPUT = "spark.hadoop.mapred.output.compress";
    private static final String OUTPUT_COMPRESSION_CODEC = "spark.hadoop.mapred.output.compression.codec";
    private static final String COMPRESSION_TYPE = "spark.hadoop.mapred.output.compression.type";
    private static final String SPARK_COMPRESSION_CODEC = "spark.sql.parquet.compression.codec";
    private static final String TEXT = "text";
    private static final String TRUE = "true";
    private static final String INPUT_PATH = "input-path";
    private static final String OUTPUT_PATH = "output-path";
    private static final String INPUT_COMPRESSION = "input-compression";
    private static final String INPUT_SERIALIZATION = "input-serialization";
    private static final String OUTPUT_COMPRESSION = "output-compression";
    private static final String OUTPUT_SERIALIZATION = "output-serialization";

    private static final double SNAPPY_RATIO = 1.7;     // (100 / 1.7) = 58.8 ~ 40% compression rate on text
    private static final double LZO_RATIO = 2.0;        // (100 / 2.0) = 50.0 ~ 50% compression rate on text
    private static final double GZIP_RATIO = 2.5;       // (100 / 2.5) = 40.0 ~ 60% compression rate on text
    private static final double BZ2_RATIO = 3.33;       // (100 / 3.3) = 30.3 ~ 70% compression rate on text

    private static final double AVRO_RATIO = 1.6;       // (100 / 1.6) = 62.5 ~ 40% compression rate on text
    private static final double PARQUET_RATIO = 2.0;    // (100 / 2.0) = 50.0 ~ 50% compression rate on text
    private static final double TEXT_RATIO = 1.0;

    private static Options options;
    
    private HashMap<String, Double> compression_ratios;

    public Compact() {
        compression_ratios = new HashMap<>();
        compression_ratios.put(makeKey(AVRO, null), AVRO_RATIO * 1.0);
        compression_ratios.put(makeKey(AVRO, BZ2), AVRO_RATIO * BZ2_RATIO);
        compression_ratios.put(makeKey(AVRO, GZIP), AVRO_RATIO * GZIP_RATIO);
        compression_ratios.put(makeKey(AVRO, LZO), AVRO_RATIO * LZO_RATIO);
        compression_ratios.put(makeKey(AVRO, NONE), AVRO_RATIO);
        compression_ratios.put(makeKey(AVRO, SNAPPY), AVRO_RATIO * SNAPPY_RATIO);

        compression_ratios.put(makeKey(PARQUET, null), PARQUET_RATIO * 1.0);
        compression_ratios.put(makeKey(PARQUET, BZ2), PARQUET_RATIO * BZ2_RATIO);
        compression_ratios.put(makeKey(PARQUET, GZIP), PARQUET_RATIO * GZIP_RATIO);
        compression_ratios.put(makeKey(PARQUET, LZO), PARQUET_RATIO * LZO_RATIO);
        compression_ratios.put(makeKey(PARQUET, NONE), PARQUET_RATIO);
        compression_ratios.put(makeKey(PARQUET, SNAPPY), PARQUET_RATIO * SNAPPY_RATIO);

        compression_ratios.put(makeKey(TEXT, null), TEXT_RATIO * 1.0);
        compression_ratios.put(makeKey(TEXT, BZ2), TEXT_RATIO * BZ2_RATIO);
        compression_ratios.put(makeKey(TEXT, GZIP), TEXT_RATIO * GZIP_RATIO);
        compression_ratios.put(makeKey(TEXT, LZO), TEXT_RATIO * LZO_RATIO);
        compression_ratios.put(makeKey(TEXT, NONE), TEXT_RATIO);
        compression_ratios.put(makeKey(TEXT, SNAPPY), TEXT_RATIO * SNAPPY_RATIO);
    }

    public void outputCompressionProperties(String outputCompression) {
        if (outputCompression.toLowerCase().equals(NONE)) {
            System.setProperty(SHOULD_COMPRESS_OUTPUT, "false");
            System.setProperty(SPARK_COMPRESSION_CODEC, "uncompressed");
        } else if (outputCompression.toLowerCase().equals(SNAPPY)) {
            System.setProperty(SHOULD_COMPRESS_OUTPUT, TRUE);
            System.setProperty(OUTPUT_COMPRESSION_CODEC, "org.apache.hadoop.io.compress.SnappyCodec");
            System.setProperty(COMPRESSION_TYPE, BLOCK);
            System.setProperty(SPARK_COMPRESSION_CODEC, SNAPPY);
        } else if (outputCompression.toLowerCase().equals(GZIP)) {
            System.setProperty(SHOULD_COMPRESS_OUTPUT, TRUE);
            System.setProperty(OUTPUT_COMPRESSION_CODEC, "org.apache.hadoop.io.compress.GzipCodec");
            System.setProperty(COMPRESSION_TYPE, BLOCK);
            System.setProperty(SPARK_COMPRESSION_CODEC, GZIP);
        } else if (outputCompression.toLowerCase().equals(BZ2)) {
            System.setProperty(SHOULD_COMPRESS_OUTPUT, TRUE);
            System.setProperty(OUTPUT_COMPRESSION_CODEC, "org.apache.hadoop.io.compress.BZip2Codec");
            System.setProperty(COMPRESSION_TYPE, BLOCK);
        } else if (outputCompression.toLowerCase().equals(LZO)) {
            System.setProperty(SHOULD_COMPRESS_OUTPUT, TRUE);
            System.setProperty(OUTPUT_COMPRESSION_CODEC, "com.hadoop.compression.lzo.LzoCodec");
            System.setProperty(COMPRESSION_TYPE, BLOCK);
            System.setProperty(SPARK_COMPRESSION_CODEC, LZO);
        }
    }

    public double splitRatio(String compressionType, String serializationType) throws IOException {
        return compression_ratios.get(makeKey(serializationType, compressionType));
    }

    public long inputSize(FileSystem fs, String inputPath, String inputCompression, String inputSerialization) throws IOException {
        Path hdfsPath = new Path(inputPath);
        FileStatus[] fsArray = fs.globStatus(hdfsPath);
        long fileSize = 0;
        
        for (FileStatus fileStatus : fsArray) {
        	fileSize += fs.getContentSummary(fileStatus.getPath()).getSpaceConsumed();
        }
        
        return (long) (fileSize * splitRatio(inputCompression, inputSerialization));
    }

    public int splitSize(FileSystem fs, String outputPath, long inputSize, double splitRatio) throws IOException {
        Path targetPath = new Path(outputPath);
        double inputSizeDouble = (double) inputSize;
        double defaultBlockSize = new Long(fs.getDefaultBlockSize(targetPath)).doubleValue();
        return (int) (Math.floor(((inputSizeDouble / splitRatio) / defaultBlockSize)) + 1.0);
    }
    
    public String makeInputPath(FileSystem fs, String inputPath) throws IOException {
    	List<String> resultList = new ArrayList<String>();
    	Path hdfsPath = new Path(inputPath);
    	FileStatus[] fsArray = fs.globStatus(hdfsPath);
    	
    	for(int i = 0; i < fsArray.length; i++) {
    		resultList.add(fsArray[i].getPath().toString());
        }
    	    	
    	return StringUtils.join(resultList, ",");
    }
    
    public void compact(String inputPath, String outputPath, String outputSerialization, int splitCount) {
        // Defining Spark Context with a generic Spark Configuration.
        SparkConf sparkConf = new SparkConf().setAppName("Spark Compaction");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        if (outputSerialization.toLowerCase().equals(TEXT)) {
            JavaRDD<String> textFile = sc.textFile(inputPath);
            textFile.coalesce(splitCount).saveAsTextFile(outputPath);
        } else if (outputSerialization.toLowerCase().equals(PARQUET)) {
            SQLContext sqlContext = new SQLContext(sc);
            DataFrame parquetFile = sqlContext.read().parquet(inputPath);
            parquetFile.coalesce(splitCount).write().parquet(outputPath);
        } else if (outputSerialization.toLowerCase().equals(AVRO)) {
            // For this to work the files must end in .avro
            SQLContext sqlContext = new SQLContext(sc);
            DataFrame avroFile = sqlContext.read().format("com.databricks.spark.avro").load(inputPath);
            avroFile.coalesce(splitCount).write().format("com.databricks.spark.avro").save(outputPath);
        } else {
            System.out.println("Did not match any serialization type, text, parquet, or avro.  Recieved: " +
                    outputSerialization.toLowerCase());
        }
    }

    public static void main(String[] args) throws IOException {
        // Defining HDFS Configuration and File System definition.
        Configuration conf = new Configuration();
        conf.addResource(new Path("file:///etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("file:///etc/hadoop/conf/hdfs-site.xml"));
        FileSystem fs = FileSystem.get(conf);
        
        // Defining Compact variable to process this compaction logic.
        Compact splits = new Compact();        
        CommandLine line = splits.parseCli(args);
        line = splits.validateCompressionAndSerializationOptions(line);

        if(null != line) {
            splits.outputCompressionProperties(line.getOptionValue(OUTPUT_COMPRESSION));
            splits.compact(splits.makeInputPath(fs, line.getOptionValue(INPUT_PATH)),
                    line.getOptionValue(OUTPUT_PATH),
                    line.getOptionValue(OUTPUT_SERIALIZATION),
                    splits.splitSize(fs, line.getOptionValue(OUTPUT_PATH), splits.inputSize(fs,
                            line.getOptionValue(INPUT_PATH), line.getOptionValue(INPUT_COMPRESSION),
                            line.getOptionValue(INPUT_SERIALIZATION)),
                            splits.splitRatio(line.getOptionValue(OUTPUT_COMPRESSION),
                                    line.getOptionValue(OUTPUT_SERIALIZATION))));
        }
    }

    private String makeKey(String serializationType, String compressionType) {
        String result;
        if(null == compressionType) {
            result = serializationType + "_";
        } else {
            result = serializationType + "_" + compressionType;
        }
        return result;
    }

    private void initializeOptions() {
        options = new Options();

        Option option = new Option("i", INPUT_PATH, true,
                "The input file path where files need to be compacted\n(required : true)");
        option.setRequired(true);
        options.addOption(option);

        option = new Option("o", OUTPUT_PATH, true,
                "The output directory where the files will be compacted to\n(required : true)");
        option.setRequired(true);
        options.addOption(option);

        option = new Option("is", INPUT_SERIALIZATION, true,
                "The serialization used on the files for the input path provided\n(avro, parquet, text)\n(required : true)");
        option.setRequired(true);
        options.addOption(option);

        option = new Option("ic", INPUT_COMPRESSION, true,
                "The compression used on the files for the input path provided\n(none, snappy, gzip, bz2, lzo)\n(required : true)");
        option.setRequired(true);
        options.addOption(option);

        option = new Option("os", OUTPUT_SERIALIZATION, true,
                "The serialization used on the files generated by the compaction process\n(avro, parquet, text)\n(required : true)");
        option.setRequired(true);
        options.addOption(option);

        option = new Option("oc", OUTPUT_COMPRESSION, true,
                "The compression used on the files generated by the compaction process, (none, snappy, gzip, bz2, lzo), (required : true)");
        option.setRequired(true);
        options.addOption(option);
    }

    private void printHelp(String additionalMessage) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("SparkCompaction [options] " + additionalMessage, options);
        System.exit(1);
    }

    private CommandLine parseCli(String[] args) {
        initializeOptions();

        CommandLineParser parser = new GnuParser();
        CommandLine line = null;

        try {
            line = parser.parse(options, args);
        } catch (ParseException e) {
            printHelp("");
        }
        return line;
    }

    private CommandLine validateCompressionAndSerializationOptions(CommandLine line) {
        String errorMsg = null;
        String is = line.getOptionValue(INPUT_SERIALIZATION);
        if(!is.equals(AVRO) && !is.equals(PARQUET) && !is.equals(TEXT)) {
            errorMsg = "Invalid input serialization format specified!";
        }
        String os = line.getOptionValue(OUTPUT_SERIALIZATION);
        if(null == errorMsg && !os.equals(AVRO) && !os.equals(PARQUET) && !os.equals(TEXT)) {
            errorMsg = "Invalid output serialization format specified!";
        }
        String ic = line.getOptionValue(INPUT_COMPRESSION);
        if(null == errorMsg && !ic.equals(BZ2) && !ic.equals(GZIP) && !ic.equals(LZO) && !ic.equals(NONE)  && !ic.equals(SNAPPY)) {
            errorMsg = "Invalid input compression format specified!";
        }
        String oc = line.getOptionValue(OUTPUT_COMPRESSION);
        if(null == errorMsg && !oc.equals(BZ2) && !oc.equals(GZIP) && !oc.equals(LZO) && !oc.equals(NONE)  && !oc.equals(SNAPPY)) {
            errorMsg = "Invalid output compression format specified!";
        }
        CommandLine result = null;
        if(null != errorMsg) {
            printHelp(errorMsg);
        } else {
            result = line;
        }
        return result;
    }

}
