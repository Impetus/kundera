package com.impetus.client.cassandra.common;

import com.impetus.client.cassandra.service.CassandraHost;

public interface CassandraClientFactory 
{

	boolean addCassandraHost(CassandraHost host);
}
