package com.impetus.client.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

public class BatchPutRequest extends TableRequest<Void> {
    private final List<Put> puts;

    public BatchPutRequest(TableName tableName, List<Put> puts) {
        super(tableName);
        this.puts = puts;
    }

    protected Void execute(Table table) throws IOException {
        table.put(puts);
        return null;
    }
}
