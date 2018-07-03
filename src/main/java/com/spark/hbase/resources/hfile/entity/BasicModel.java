package com.spark.hbase.resources.hfile.entity;

import java.io.Serializable;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Row;

public class BasicModel implements RowMapper, Serializable{

    private static final long serialVersionUID = 1L;
    
    private static final byte[] CF = Bytes.toBytes("column");
    
    private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");
    
    private static final BasicModel instance = new BasicModel();
    
    private BasicModel() {
	
    }

    public static BasicModel getInstance() {
	return instance;
    }

    @Override
    public Put rowToPut(Row row) {
	Put p = new Put(Bytes.toBytes(row.get(0).toString()));
	p.addColumn(CF, QUALIFIER, Bytes.toBytes(row.get(1).toString()));
	return p;
    }

}
