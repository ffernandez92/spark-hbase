package com.spark.hbase.resources.hfile;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HFileOutput {
    
    private static final String PROPPATH = "META-INF/kbrconf.properties";
    
    private static final Logger logger = LoggerFactory.getLogger(HFileOutput.class);
    
    private static final String HBASESITE = "hbase-site.xml";
    
    public static void main(String[] args) {
	
	Properties prop = new Properties();
	final Configuration configuration = HBaseConfiguration.create();
	configuration.addResource(SparkFiles.get(HBASESITE));
	
	try(InputStream input = new FileInputStream(PROPPATH);){
	    prop.load(input);
	    String inputPath =  args[0];
	    String outputPath = args[1];
	    String table = args[2];
	    HFileOutputService hos = new HFileOutputService(inputPath,outputPath,table);
	    hos.saveHFile();
	    
	    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(configuration);
	    HTable htable = new HTable(configuration, Bytes.toBytes(table));
	    Path path = new Path(outputPath);
	    loader.doBulkLoad(path, htable);
	    
	}catch(Exception exception) {
	    logger.error("Error: "+exception , exception);
	}
	
	
    }

}
