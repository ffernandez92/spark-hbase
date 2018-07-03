package com.spark.hbase.resources.hfile;

import java.io.IOException;
import java.io.Serializable;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spark.hbase.resources.hfile.entity.BasicModel;
import com.spark.hbase.resources.hfile.utils.HBaseRecord;

import scala.Tuple2;

public class HFileOutputService implements Serializable{

    private static final long serialVersionUID = 1L;
    
    private static final Logger logger = LoggerFactory.getLogger(HFileOutputService.class);
    
    private static final String BINASSTRING = "spark.sql.parquet.binaryAsString";
    
    private static final String HBASESITE = "hbase-site.xml";
    
    private static final String KRBUSER = "kerberos.user";
    
    private static final String KRBKEYTAB = "kerberos.keytab";
    
    private final transient JavaSparkContext sc;
    
    private final transient SQLContext sqlc;
    
    private final String inputPath;
    
    private final String outputPath;
    
    private final String table;
    
    private final boolean kerberos;
    
    private final Properties prop;
    
    
    public HFileOutputService(final String inputPath, final String outputPath, final String table, final Properties prop) {
	this(inputPath, outputPath, table, prop, Boolean.FALSE);
    }
    
    public HFileOutputService(final String inputPath, final String outputPath, final String table, final Properties prop, boolean kerberos) {
	this.sc = new JavaSparkContext();
	this.sqlc = new SQLContext(sc);
	sqlc.setConf(BINASSTRING, "true");
	this.inputPath = inputPath;
	this.outputPath = outputPath;
	this.table = table;
	this.kerberos = kerberos;
	this.prop = prop;
    }
    
    /**
     * <p>Executes the save action. It uses Kerberos option only on secured clusters <b>(under user demand)</b>, otherwise 
     * there is no authentication at all.</p>
     * @throws IOException
     */
    public void saveHFile() throws IOException {
	if(kerberos) {
	   UserGroupInformation.loginUserFromKeytabAndReturnUGI(prop.getProperty(KRBUSER), prop.getProperty(KRBKEYTAB))
	   .doAs(new PrivilegedAction<Void>() {
	       @Override
	       public Void run() {
		generateHFile();
		return null;
		}
		    
	   });
	}else {
	   generateHFile();
	}
    }
    
    private void generateHFile() {
	final Configuration configuration = HBaseConfiguration.create();
	configuration.addResource(SparkFiles.get(HBASESITE));	
	try (Connection connection = ConnectionFactory.createConnection(configuration);
		final Table hTable = connection.getTable(TableName.valueOf(table));
		final RegionLocator locator = connection.getRegionLocator(TableName.valueOf(table))) {
	    	final Job job = Job.getInstance(configuration, "");
		HFileOutputFormat2.configureIncrementalLoad(job, hTable, locator);
		Configuration innerC = job.getConfiguration();
		sortByHBaseRecord(mapParquet()).saveAsNewAPIHadoopFile(outputPath, ImmutableBytesWritable.class,
			KeyValue.class, HFileOutputFormat2.class, innerC);
    	} catch (final Exception exception) {
    	    logger.error("Error on getting HBase : " + exception.getMessage(), exception);
    	    throw new RuntimeException(exception.getMessage(), exception);
    	}
    }

    
    private JavaPairRDD<ImmutableBytesWritable, KeyValue> sortByHBaseRecord(final JavaRDD<Put> rdd){
	
	return rdd.flatMapToPair(new PairFlatMapFunction<Put,HBaseRecord, HBaseRecord>(){
	    
	    private static final long serialVersionUID = 1L;

	    @Override
	    public Iterable<Tuple2<HBaseRecord, HBaseRecord>> call(Put put) throws Exception {
		return getIterable(put);
	    }
	    
	}).sortByKey(new HBaseRecord()).mapToPair(new PairFunction<Tuple2<HBaseRecord,HBaseRecord>,ImmutableBytesWritable,KeyValue>(){

	    private static final long serialVersionUID = 1L;

	    @Override
	    public Tuple2<ImmutableBytesWritable, KeyValue> call(Tuple2<HBaseRecord, HBaseRecord> tuple) throws Exception {
		HBaseRecord hBaseCell = tuple._2;
		final KeyValue keyValue = new KeyValue(hBaseCell.getRowKey(), hBaseCell.getColumnFamily(),hBaseCell.getQualifier(), hBaseCell.getValue());
		return new Tuple2<>(new ImmutableBytesWritable(hBaseCell.getRowKey()),keyValue);
	    }
	    
	});
    }
    
    
    
    private JavaRDD<Put> mapParquet() {
	
	return sqlc.read().parquet(inputPath).javaRDD().map(new Function<Row,Put>(){

	    private static final long serialVersionUID = 1L;

	    @Override
	    public Put call(Row row) throws Exception {
		return BasicModel.getInstance().rowToPut(row);
	    }
	    
	});
	
    }
    
    private HBaseRecord fromCellToHBaseCell(final Cell cell) {
	HBaseRecord hbcell = new HBaseRecord();
	hbcell.setRowKey(CellUtil.cloneRow(cell));
	hbcell.setColumnFamily(CellUtil.cloneFamily(cell));
	hbcell.setQualifier(CellUtil.cloneQualifier(cell));
	hbcell.setValue(CellUtil.cloneValue(cell));
	return hbcell;
    }

    private Iterable<Tuple2<HBaseRecord, HBaseRecord>> getIterable(Put put) {
	List<Tuple2<HBaseRecord, HBaseRecord>> listKeyVal = new ArrayList<>();
	    for (Entry<byte[], List<Cell>> entry : put.getFamilyCellMap().entrySet()) {
		List<Cell> cells = entry.getValue();
		for (Cell cell : cells) {
		    HBaseRecord hbCell = fromCellToHBaseCell(cell);
		    listKeyVal.add(new Tuple2<>(hbCell, hbCell));
		}
	    }
	    return listKeyVal;
    }

}
