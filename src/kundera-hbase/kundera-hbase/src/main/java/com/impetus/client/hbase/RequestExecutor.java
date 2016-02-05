package com.impetus.client.hbase;

import java.io.IOException;

public interface RequestExecutor {
    <T> T execute(HBaseRequest<T> request) throws IOException;
}
