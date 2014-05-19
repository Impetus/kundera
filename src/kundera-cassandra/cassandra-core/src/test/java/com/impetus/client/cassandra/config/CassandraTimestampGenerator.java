package com.impetus.client.cassandra.config;

import com.impetus.kundera.utils.TimestampGenerator;

public class CassandraTimestampGenerator implements TimestampGenerator
{

    @Override
    public long getTimestamp()
    {
        return 10 * System.currentTimeMillis();
    }

}
