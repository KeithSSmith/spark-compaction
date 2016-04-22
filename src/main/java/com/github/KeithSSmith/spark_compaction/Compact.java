package com.github.KeithSSmith.spark_compaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class Compact {

    private static Configuration conf = new Configuration();
    private static CompressionCodecFactory codecFactory = null;
    FileSystem fs = null;
    FileStatus[] fsArray = null;
    
    private static final String AVRO = "avro";
    private static final String BLOCK = "BLOCK";
    private static final String BZ2 = "bzip2";
    private static final String GZIP = "gzip";
    private static final String LZO = "lzo";
    private static final String NONE = "none";
    private static final String PARQUET = "parquet";
    private static final String SNAPPY = "snappy";
    private static final String SHOULD_COMPRESS_OUTPUT = "spark.hadoop.mapred.output.compress";
    private static final String OUTPUT_COMPRESSION_CODEC = "spark.hadoop.mapred.output.compression.codec";
    private static final String COMPRESSION_TYPE = "spark.hadoop.mapred.output.compression.type";
    private static final String SPARK_PARQUET_COMPRESSION_CODEC = "spark.sql.parquet.compression.codec";
    private static final String SPARK_AVRO_COMPRESSION_CODEC = "spark.sql.avro.compression.codec";
    private static final String AVRO_COMPRESSION_CODEC = "avro.output.codec";
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
    
    private HashMap<String, Double> compressionRatios;
    private HashMap<CompressionCodec, String> compressionTypes;
    private HashMap<String, String> serializationExtensions;
    
    private String inputPath;
    private long inputPathSize;
    private String inputCompression;
    private String inputSerialization;
    private String outputPath;
    private String outputCompression;
    private String outputSerialization;
    private double outputBlockSize;
    private int splitSize;
    private double inputCompressionRatio;
    private double outputCompressionRatio;
    private Path inputCompressionPath;
    

    public Compact() {
        // Defining HDFS Configuration and File System definition.
        conf.addResource(new Path("file:///etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("file:///etc/hadoop/conf/hdfs-site.xml"));
        
        codecFactory = new CompressionCodecFactory(conf);
        
        this.setInputPathSize(0);
    	this.setInputCompression(NONE);
    	this.setInputSerialization(TEXT);
    	this.setOutputCompression(NONE);
    	this.setOutputSerialization(TEXT);
    	this.setOutputBlockSize(0);
    	this.setSplitSize(0);
    	this.setInputCompressionRatio(0);
    	this.setOutputCompressionRatio(0);
    	this.setInputCompressionPath((Path) null);
    	
    	compressionRatios = new HashMap<>();
        compressionRatios.put(makeKey(AVRO, null), AVRO_RATIO * 1.0);
        compressionRatios.put(makeKey(AVRO, BZ2), AVRO_RATIO * BZ2_RATIO);
        compressionRatios.put(makeKey(AVRO, GZIP), AVRO_RATIO * GZIP_RATIO);
        compressionRatios.put(makeKey(AVRO, LZO), AVRO_RATIO * LZO_RATIO);
        compressionRatios.put(makeKey(AVRO, NONE), AVRO_RATIO);
        compressionRatios.put(makeKey(AVRO, SNAPPY), AVRO_RATIO * SNAPPY_RATIO);

        compressionRatios.put(makeKey(PARQUET, null), PARQUET_RATIO * 1.0);
        compressionRatios.put(makeKey(PARQUET, BZ2), PARQUET_RATIO * BZ2_RATIO);
        compressionRatios.put(makeKey(PARQUET, GZIP), PARQUET_RATIO * GZIP_RATIO);
        compressionRatios.put(makeKey(PARQUET, LZO), PARQUET_RATIO * LZO_RATIO);
        compressionRatios.put(makeKey(PARQUET, NONE), PARQUET_RATIO);
        compressionRatios.put(makeKey(PARQUET, SNAPPY), PARQUET_RATIO * SNAPPY_RATIO);

        compressionRatios.put(makeKey(TEXT, null), TEXT_RATIO * 1.0);
        compressionRatios.put(makeKey(TEXT, BZ2), TEXT_RATIO * BZ2_RATIO);
        compressionRatios.put(makeKey(TEXT, GZIP), TEXT_RATIO * GZIP_RATIO);
        compressionRatios.put(makeKey(TEXT, LZO), TEXT_RATIO * LZO_RATIO);
        compressionRatios.put(makeKey(TEXT, NONE), TEXT_RATIO);
        compressionRatios.put(makeKey(TEXT, SNAPPY), TEXT_RATIO * SNAPPY_RATIO);
        
        compressionTypes = new HashMap<>();
        compressionTypes.put(null, NONE);
        compressionTypes.put(codecFactory.getCodecByName(BZ2), BZ2);
        compressionTypes.put(codecFactory.getCodecByName(GZIP), GZIP);
//        compressionTypes.put(codecFactory.getCodecByName(LZO), LZO);       // This is not a default codec and will not be supported at this time.
        compressionTypes.put(codecFactory.getCodecByName(SNAPPY), SNAPPY);
        
        serializationExtensions = new HashMap<>();
        serializationExtensions.put(PARQUET, ".parquet");
        serializationExtensions.put(AVRO, ".avro");
    }

    public void outputCompressionProperties(String outputCompression) {
        if (outputCompression.toLowerCase().equals(NONE)) {
            System.setProperty(SHOULD_COMPRESS_OUTPUT, "false");
            System.setProperty(SPARK_PARQUET_COMPRESSION_CODEC, "uncompressed");
        } else if (outputCompression.toLowerCase().equals(SNAPPY)) {
            System.setProperty(SHOULD_COMPRESS_OUTPUT, TRUE);
            System.setProperty(OUTPUT_COMPRESSION_CODEC, "org.apache.hadoop.io.compress.SnappyCodec");
            System.setProperty(COMPRESSION_TYPE, BLOCK);
            System.setProperty(SPARK_PARQUET_COMPRESSION_CODEC, SNAPPY);
            System.setProperty(SPARK_AVRO_COMPRESSION_CODEC, SNAPPY);
            System.setProperty(AVRO_COMPRESSION_CODEC, SNAPPY);
        } else if (outputCompression.toLowerCase().equals(GZIP)) {
            System.setProperty(SHOULD_COMPRESS_OUTPUT, TRUE);
            System.setProperty(OUTPUT_COMPRESSION_CODEC, "org.apache.hadoop.io.compress.GzipCodec");
            System.setProperty(COMPRESSION_TYPE, BLOCK);
            System.setProperty(SPARK_PARQUET_COMPRESSION_CODEC, GZIP);
            System.setProperty(SPARK_AVRO_COMPRESSION_CODEC, GZIP);
            System.setProperty(AVRO_COMPRESSION_CODEC, GZIP);
        } else if (outputCompression.toLowerCase().equals(BZ2)) {
            System.setProperty(SHOULD_COMPRESS_OUTPUT, TRUE);
            System.setProperty(OUTPUT_COMPRESSION_CODEC, "org.apache.hadoop.io.compress.BZip2Codec");
            System.setProperty(COMPRESSION_TYPE, BLOCK);
            System.setProperty(SPARK_PARQUET_COMPRESSION_CODEC, BZ2);   //This will throw an error when Parquet + BZ2 is set b/c BZ2 is not supported in the upstream package.
            System.setProperty(SPARK_AVRO_COMPRESSION_CODEC, BZ2);      //This will throw an error when Avro + BZ2 is set b/c BZ2 is not supported in the upstream package.
            System.setProperty(AVRO_COMPRESSION_CODEC, BZ2);
        }
//        } else if (outputCompression.toLowerCase().equals(LZO)) {
//            System.setProperty(SHOULD_COMPRESS_OUTPUT, TRUE);
//            System.setProperty(OUTPUT_COMPRESSION_CODEC, "com.hadoop.compression.lzo.LzoCodec");
//            System.setProperty(COMPRESSION_TYPE, BLOCK);
//            System.setProperty(SPARK_PARQUET_COMPRESSION_CODEC, LZO);
//            System.setProperty(SPARK_AVRO_COMPRESSION_CODEC, LZO);
//            System.setProperty(AVRO_COMPRESSION_CODEC, LZO);
//        }
    }
    
    public void compact(String inputPath, String outputPath) throws IOException {
        this.setCompressionAndSerializationOptions(inputPath, outputPath);
        this.outputCompressionProperties(this.outputCompression);
        
    	// Defining Spark Context with a generic Spark Configuration.
        SparkConf sparkConf = new SparkConf().setAppName("Spark Compaction");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
        if (this.outputSerialization.equals(TEXT)) {
            JavaRDD<String> textFile = sc.textFile(this.concatInputPath(inputPath));
            textFile.coalesce(this.splitSize).saveAsTextFile(outputPath);
        } else if (this.outputSerialization.equals(PARQUET)) {
            SQLContext sqlContext = new SQLContext(sc);
            DataFrame parquetFile = sqlContext.read().parquet(this.concatInputPath(inputPath));
            parquetFile.coalesce(this.splitSize).write().parquet(outputPath);
        } else if (this.outputSerialization.equals(AVRO)) {
            // For this to work the files must end in .avro
        	// Another issue is that when using compression the compression codec extension is not being added to the file name.
            SQLContext sqlContext = new SQLContext(sc);
            DataFrame avroFile = sqlContext.read().format("com.databricks.spark.avro").load(this.concatInputPath(inputPath));
            avroFile.coalesce(this.splitSize).write().format("com.databricks.spark.avro").save(outputPath);
        } else {
            System.out.println("Did not match any serialization type: text, parquet, or avro.  Recieved: " +
                    this.outputSerialization);
        }
    }
    
    public void compact(String[] args) throws IOException {
    	this.setCompressionAndSerializationOptions(this.parseCli(args));
    	this.outputCompressionProperties(this.outputCompression);
        
    	// Defining Spark Context with a generic Spark Configuration.
        SparkConf sparkConf = new SparkConf().setAppName("Spark Compaction");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
                
        if (this.outputSerialization.equals(TEXT)) {
            JavaRDD<String> textFile = sc.textFile(this.concatInputPath(inputPath));
            textFile.coalesce(this.splitSize).saveAsTextFile(outputPath);
        } else if (this.outputSerialization.equals(PARQUET)) {
            SQLContext sqlContext = new SQLContext(sc);
            DataFrame parquetFile = sqlContext.read().parquet(this.concatInputPath(inputPath));
            parquetFile.coalesce(this.splitSize).write().parquet(outputPath);
        } else if (this.outputSerialization.equals(AVRO)) {
            // For this to work the files must end in .avro
            SQLContext sqlContext = new SQLContext(sc);
            DataFrame avroFile = sqlContext.read().format("com.databricks.spark.avro").load(this.concatInputPath(inputPath));
            avroFile.coalesce(this.splitSize).write().format("com.databricks.spark.avro").save(outputPath);
        } else {
            System.out.println("Did not match any serialization type: text, parquet, or avro.  Recieved: " +
                    this.outputSerialization);
        }
    }
    
    public static void main(String[] args) throws IOException {
        // Defining Compact variable to process this compaction logic and parse the CLI arguments.
        Compact splits = new Compact();
        // Example of calling the CLI.
        splits.compact(args);
        // Example of using the API with input and output directories passed.
//        splits.compact("hdfs:///landing/compaction/input", "hdfs:///landing/compaction/output_text_none");
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
    
    private String concatInputPath(String inputPath) throws IOException {
    	List<String> resultList = new ArrayList<String>();
    	
    	for(FileStatus fileStatus : fsArray) {
    		if (fileStatus.getPath().getName().startsWith("_") || fileStatus.getPath().getName().startsWith(".")) {
    			continue;
    		}
    		resultList.add(fileStatus.getPath().toString());
        }
    	
    	return StringUtils.join(resultList, ",");
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
                "The serialization used on the files for the input path provided\n(avro, parquet, text)\n(required : false)");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("ic", INPUT_COMPRESSION, true,
                "The compression used on the files for the input path provided\n(none, snappy, gzip, bzip2)\n(required : false)");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("os", OUTPUT_SERIALIZATION, true,
                "The serialization used on the files generated by the compaction process\n(avro, parquet, text)\n(required : false)");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("oc", OUTPUT_COMPRESSION, true,
                "The compression used on the files generated by the compaction process, (none, snappy, gzip, bzip2), (required : false)");
        option.setRequired(false);
        options.addOption(option);
    }

    private void printHelp(String additionalMessage) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("SparkCompaction [options] " + additionalMessage, options);
        System.exit(1);
    }

    private CommandLine parseCli(String[] args) throws IllegalArgumentException, IOException {
        this.initializeOptions();

        CommandLineParser parser = new GnuParser();
        CommandLine line = null;

        try {
            line = parser.parse(options, args);
        } catch (ParseException e) {
            printHelp("");
        }
        
        this.setInputPath(line.getOptionValue(INPUT_PATH));
        this.setOutputPath(line.getOptionValue(OUTPUT_PATH));
        this.setOutputBlockSize(this.outputPath);
        
        return line;
    }
    
	private void setCompressionAndSerializationOptions(CommandLine line) throws IOException {
    	String ic = line.getOptionValue(INPUT_COMPRESSION);
    	if (ic == null) {
    		this.setInputCompression(new Path(this.getInputPath()));
    	}
    	
    	String is = line.getOptionValue(INPUT_SERIALIZATION);
    	if (is != null) {
    		this.setInputSerialization(is);
    	}
    	
    	String oc = line.getOptionValue(OUTPUT_COMPRESSION);
    	if (oc == null) {
    		this.setOutputCompression(this.getInputCompression());        // Output Compression will the be same as the Input if left null.
    	} else {
    		this.setOutputCompression(oc);
    	}
    	
    	String os = line.getOptionValue(OUTPUT_SERIALIZATION);
    	if (os == null) {
    		this.setOutputSerialization(this.getInputSerialization());    // Output Serialization will the be same as the Input if left null.
    	} else {
    		this.setOutputSerialization(os);
    	}
    	
    	this.validateCompressionAndSerializationOptions();
    }

    private void setCompressionAndSerializationOptions(String inputPath, String outputPath) throws IOException {
    	this.setInputPath(inputPath);
    	this.setInputCompression(new Path(this.getInputPath()));
        this.setOutputPath(outputPath);
        this.setOutputBlockSize(outputPath);
    	this.setOutputCompression(this.getInputCompression());        // Output Compression will the be same as the Input if left null.
    	this.setOutputSerialization(this.getInputSerialization());    // Output Serialization will the be same as the Input if left null.
        
    	this.validateCompressionAndSerializationOptions();
    }

    private void validateCompressionAndSerializationOptions() throws IllegalArgumentException, IOException {
        String errorMsg = null;
        
        String ic = this.getInputCompression();
        if(null == errorMsg && !ic.equals(BZ2) && !ic.equals(GZIP) && !ic.equals(NONE)  && !ic.equals(SNAPPY)) {
            errorMsg = "Invalid input compression format specified!";
        }
        String is = this.getInputSerialization();
        if(!is.equals(AVRO) && !is.equals(PARQUET) && !is.equals(TEXT)) {
            errorMsg = "Invalid input serialization format specified!";
        }
        String oc = this.getOutputCompression();
        if(null == errorMsg && !oc.equals(BZ2) && !oc.equals(GZIP) && !oc.equals(NONE)  && !oc.equals(SNAPPY)) {
            errorMsg = "Invalid output compression format specified!";
        }
        String os = this.getOutputSerialization();
        if(null == errorMsg && !os.equals(AVRO) && !os.equals(PARQUET) && !os.equals(TEXT)) {
            errorMsg = "Invalid output serialization format specified!";
        }
        
        if(null != errorMsg) {
            printHelp(errorMsg);
        }
        
        this.setInputCompressionRatio(this.inputCompression, this.inputSerialization);
        this.setOutputCompressionRatio(this.outputCompression, this.outputSerialization);
        this.setInputPathSize(this.inputPath);
        this.setSplitSize(this.outputPath);
    }

	public long getInputPathSize() {
		return inputPathSize;
	}
	
    public void setInputPathSize(long inputSize) {
		this.inputPathSize = inputSize;
	}
	
    public void setInputPathSize(String inputPath) throws IOException {
        long fileSize = 0;
    	
    	for (FileStatus fileStatus : this.fsArray) {
        	if (fileStatus.getPath().getName().startsWith("_") || fileStatus.getPath().getName().startsWith(".")) {
        		continue;
        	}
        	fileSize = fileSize + this.fs.getContentSummary(fileStatus.getPath()).getSpaceConsumed();
        }
        
        this.inputPathSize = (long) (fileSize * this.inputCompressionRatio);
    }
    
    public void setInputPathSize(String inputPath, String inputCompression, String inputSerialization) throws IOException {
        long fileSize = 0;
    	this.setInputCompressionRatio(inputCompression, inputSerialization);
    	
    	for (FileStatus fileStatus : this.fsArray) {
        	if (fileStatus.getPath().getName().startsWith("_") || fileStatus.getPath().getName().startsWith(".")) {
        		continue;
        	}
        	fileSize = fileSize + this.fs.getContentSummary(fileStatus.getPath()).getSpaceConsumed();
        }
        
        this.inputPathSize = (long) (fileSize * this.inputCompressionRatio);
    }

	public int getSplitSize() {
		return splitSize;
	}

	public void setSplitSize(int splitSize) {
		this.splitSize = splitSize;
	}

	public void setSplitSize(String outputPath) throws IOException {
        this.setOutputBlockSize(outputPath);
        double inputPathSizeDouble = (double) this.inputPathSize;
        this.splitSize = (int) (Math.floor(((inputPathSizeDouble / this.outputCompressionRatio) / this.outputBlockSize)) + 1.0);
    }

	public void setSplitSize(String outputPath, long inputPathSize, double outputCompressionRatio) throws IOException {
		this.setOutputBlockSize(outputPath);
        double inputPathSizeDouble = (double) inputPathSize;
        this.splitSize = (int) (Math.floor(((inputPathSizeDouble / outputCompressionRatio) / this.outputBlockSize)) + 1.0);
    }

	public double getInputCompressionRatio() {
		return inputCompressionRatio;
	}

	public void setInputCompressionRatio(double inputCompressionRatio) {
		this.inputCompressionRatio = inputCompressionRatio;
	}

	public void setInputCompressionRatio(String compressionType, String serializationType) {
    	this.inputCompressionRatio = this.compressionRatios.get(this.makeKey(serializationType, compressionType));
    }
	
	public double getOutputCompressionRatio() {
		return outputCompressionRatio;
	}

	public void setOutputCompressionRatio(double outputCompressionRatio) {
		this.outputCompressionRatio = outputCompressionRatio;
	}

	public void setOutputCompressionRatio(String compressionType, String serializationType) {
    	this.outputCompressionRatio = this.compressionRatios.get(this.makeKey(serializationType, compressionType));
    }

	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(String inputPath) throws IllegalArgumentException, IOException {
		this.inputPath = inputPath;
		this.fs = new Path(this.inputPath).getFileSystem(conf);
        this.fsArray = fs.globStatus(new Path(this.inputPath));
	}

	public String getInputCompression() {
		return inputCompression;
	}

    public void setInputCompression(Path inputPath) throws IOException {
    	CompressionCodec fileCodec = null;
    	
    	if (fsArray[0].isDirectory()) {
    		fsArray = fs.listStatus(inputPath);
    	}
    	
        for (FileStatus fileStatus : fsArray) {
        	if (fileStatus.getPath().getName().startsWith("_") || fileStatus.getPath().getName().startsWith(".")) {
    			continue;
    		}
        	
        	this.inputCompressionPath = (Path) null;
        	this.getInputSerialization(fileStatus.getPath().toString());
        	fileCodec = codecFactory.getCodec(this.inputCompressionPath);
        	
        	if (fileCodec != null) {
        		this.inputCompression = this.compressionTypes.get(codecFactory.getCodec(this.inputCompressionPath));
        		break;
        	}
        }
    }

	public void setInputCompression(String inputCompression) {
		this.inputCompression = inputCompression.toLowerCase();
	}

	public void setInputCompression(CompressionCodec inputCompression) {
		this.inputCompression = compressionTypes.get(inputCompression);
	}
	
	public String getInputSerialization() {
		return inputSerialization;
	}

	public String getInputSerialization(String serializationPath) {
    	for (Entry<String, String> serialization : serializationExtensions.entrySet()) {
    		if (serializationPath.endsWith(serialization.getValue())) {
    			this.inputSerialization = serialization.getKey();
    			this.setInputCompressionPath(serializationPath.substring(0, serializationPath.length() - serialization.getValue().length()));
    			break;
    		}
    	}
    	
    	if (inputCompressionPath == null) {
    		this.setInputCompressionPath(serializationPath);
    	}
    	
    	return inputSerialization;
    }

	public void setInputSerialization(String inputSerialization) {
		this.inputSerialization = inputSerialization.toLowerCase();
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}
	
	public String getOutputCompression() {
		return outputCompression;
	}

	public void setOutputCompression(String outputCompression) {
		this.outputCompression = outputCompression.toLowerCase();
	}

	public String getOutputSerialization() {
		return outputSerialization;
	}

	public void setOutputSerialization(String outputSerialization) {
		this.outputSerialization = outputSerialization.toLowerCase();
	}
	
	public Path getInputCompressionPath() {
		return inputCompressionPath;
	}

	public void setInputCompressionPath(String inputCompressionPath) {
		this.inputCompressionPath = new Path(inputCompressionPath);
	}
	
	public void setInputCompressionPath(Path inputCompressionPath) {
		this.inputCompressionPath = inputCompressionPath;
	}

	public double getOutputBlockSize() {
		return outputBlockSize;
	}

	public void setOutputBlockSize(double outputBlockSize) {
		this.outputBlockSize = outputBlockSize;
	}

	public void setOutputBlockSize(Path outputPath) {
		this.outputBlockSize = new Long(fs.getDefaultBlockSize(outputPath)).doubleValue();
	}

	public void setOutputBlockSize(String outputPath) {
		this.outputBlockSize = new Long(fs.getDefaultBlockSize(new Path(outputPath))).doubleValue();
	}

}
