package com.impetus.client.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;

public abstract class TableRequest<T> implements HBaseRequest<T> {
    private final TableName tableName;

    public TableRequest(TableName tableName) {
        this.tableName = tableName;
    }

    public TableRequest(String tableName) {
        this(TableName.valueOf(tableName));
    }

    @Override
    public T execute(Connection connection) throws IOException {
        try(Table table = connection.getTable(tableName)) {
            return execute(table);
        }
    }

    protected abstract T execute(Table table) throws IOException;
}
