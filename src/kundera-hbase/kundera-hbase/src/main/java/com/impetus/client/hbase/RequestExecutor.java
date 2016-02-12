package com.impetus.client.hbase;

import java.io.Closeable;
import java.io.IOException;

public interface RequestExecutor extends Closeable {
    <T> T execute(HBaseRequest<T> request) throws IOException;
}
