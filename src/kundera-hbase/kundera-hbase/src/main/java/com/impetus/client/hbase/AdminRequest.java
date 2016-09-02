package com.impetus.client.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

public abstract class AdminRequest implements HBaseRequest<Void> {
    public Void execute(Connection connection) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            execute(admin);
            return null;
        }
    }
    protected abstract void execute(Admin admin) throws IOException;
}
