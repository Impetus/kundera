package com.impetus.client.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Connection;

public interface HBaseRequest<T> {
    T execute(Connection connection) throws IOException;
}
