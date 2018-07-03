package com.spark.hbase.resources.hbase.hfile.utils;

import java.io.Serializable;
import java.util.Comparator;

public class HBaseRecord implements Serializable, Comparator<HBaseRecord>{

    private static final long serialVersionUID = 1L;

    private byte[] rowKey;

    private byte[] columnFamily;

    private byte[] qualifier;

    private byte[] value;
    
    
    @Override
    public int compare(HBaseRecord o1, HBaseRecord o2) {
	final int firstLevel = new String(o1.getRowKey()).compareTo(new String(o2.getRowKey()));
	    if (firstLevel == 0) {
		final int secondLevel = new String(o1.getColumnFamily()).compareTo(new String(o2.getColumnFamily()));
		if (secondLevel == 0) {
			return new String(o1.getQualifier()).compareTo(new String(o2.getQualifier()));
		}
		return secondLevel;
	}
	return firstLevel;
    }
    
    public byte[] getRowKey() {
	return this.rowKey;
    }
    
    public byte[] getColumnFamily() {
	return this.columnFamily;
    }
    
    public byte[] getQualifier() {
	return this.qualifier;
    }
    
    public byte[] getValue() {
	return this.value;
    }
    
    public void setRowKey(byte[] rowKey) {
	this.rowKey = rowKey;
    }
    
    public void setColumnFamily(byte[] columnFamily) {
	this.columnFamily = columnFamily;
    }
   
    public void setQualifier(byte[] qualifier) {
	this.qualifier = qualifier;
    }
    
    public void setValue(byte[] value) {
	this.value = value;
    }
    

}
