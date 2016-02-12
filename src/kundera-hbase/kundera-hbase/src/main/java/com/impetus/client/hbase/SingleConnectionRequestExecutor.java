package com.impetus.client.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Connection;

public class SingleConnectionRequestExecutor implements RequestExecutor {
    private final Connection connection;

    public SingleConnectionRequestExecutor(Connection connection) {
        this.connection = connection;
    }

    @Override
    public <T> T execute(HBaseRequest<T> request) throws IOException {
        return request.execute(connection);
    }

    @Override
    public void close() throws IOException {
        connection.close();
    }
}
