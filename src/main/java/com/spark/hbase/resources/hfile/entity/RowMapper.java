package com.spark.hbase.resources.hfile.entity;

import org.apache.hadoop.hbase.client.Put;
import org.apache.spark.sql.Row;

public interface RowMapper {
    
    public Put rowToPut(final Row row);

}
