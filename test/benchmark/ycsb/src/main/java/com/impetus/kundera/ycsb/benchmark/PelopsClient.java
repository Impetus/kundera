/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.ycsb.benchmark;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.IConnection;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import common.Logger;

/**
 * @author Kuldeep Mishra
 * 
 */
public class PelopsClient extends DB
{

    private static Logger logger = Logger.getLogger(PelopsClient.class);

    static Random random = new Random();

    private static final int Ok = 0;

    private static final int Error = -1;

    private String column_family;

    private static final String COLUMN_FAMILY_PROPERTY = "columnfamilyOrTable";

    private static final String COLUMN_FAMILY_PROPERTY_DEFAULT = "data";

    private static final String READ_CONSISTENCY_LEVEL_PROPERTY = "cassandra.readconsistencylevel";

    private static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

    private static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.writeconsistencylevel";

    private static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

    private static String _keyspace = "kundera";

    private static String _host = "localhost";

    private static int _port = 9160;

    private static String poolName = _host + ":" + _port + ":" + _keyspace; 
    private ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.ONE;

    private ConsistencyLevel readConsistencyLevel = ConsistencyLevel.ONE;

    static
    {

        Pelops.getDbConnPool(getPoolName());
        String[] contactNodes = new String[] { _host };
        Cluster cluster = new Cluster(contactNodes, new IConnection.Config(_port, true, -1), false);
        Pelops.addPool(getPoolName(), cluster, _keyspace);
    }

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    public synchronized void init() throws DBException
    {
        String hosts = getProperties().getProperty("hosts");
        if (hosts == null)
        {
            throw new DBException("Required property \"hosts\" missing for CassandraClient");
        }

        column_family = "pelopsuser";
        // column_family = getProperties().getProperty(COLUMN_FAMILY_PROPERTY,
        // COLUMN_FAMILY_PROPERTY_DEFAULT);

        readConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY,
                READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));

        writeConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY,
                WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));

        String[] allhosts = hosts.split(",");
        _host = allhosts[random.nextInt(allhosts.length)];

        /*
         * if (Pelops.getDbConnPool(getPoolName()) == null) {
         * System.out.println("calling");
         * 
         * }
         */
    }

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one
     * DB instance per client thread.
     */
    public void cleanup() throws DBException
    {
        // Pelops.shutdown();
        // Pelops.removePool(getPoolName());
    }

    /**
     * Read a record from the database. Each field/value pair from the result
     * will be stored in a HashMap.
     * 
     * @param table
     *            The name of the table
     * @param key
     *            The record key of the record to read.
     * @param fields
     *            The list of fields to read, or null for all of them
     * @param result
     *            A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result)
    {
        try
        {
            List<ByteBuffer> keys = new ArrayList<ByteBuffer>();
            keys.add(ByteBufferUtil.bytes(key));
            Selector selector = Pelops.createSelector(getPoolName());
            Map<ByteBuffer, List<ColumnOrSuperColumn>> columns = selector./*getColumnsFromRow(column_family, key,
                    Selector.newColumnsPredicateAll(true, 1000), readConsistencyLevel);*/
             getColumnOrSuperColumnsFromRows(new ColumnParent(column_family),
             keys,
             Selector.newColumnsPredicateAll(true, 10000),
             readConsistencyLevel);

            assert columns != null;
            return Ok;
        }
        catch (Exception e)
        {
            logger.error(e);
            return Error;
        }
    }

    /**
     * Perform a range scan for a set of records in the database. Each
     * field/value pair from the result will be stored in a HashMap.
     * 
     * @param table
     *            The name of the table
     * @param startkey
     *            The record key of the first record to read.
     * @param recordcount
     *            The number of records to read
     * @param fields
     *            The list of fields to read, or null for all of them
     * @param result
     *            A Vector of HashMaps, where each HashMap is a set field/value
     *            pairs for one record
     * @return Zero on success, a non-zero error code on error
     */
    public int scan(String table, String startkey, int recordcount, Set<String> fields,
            Vector<HashMap<String, ByteIterator>> result)
    {
        return Ok;
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key, overwriting any existing values with the same field name.
     * 
     * @param table
     *            The name of the table
     * @param key
     *            The record key of the record to write.
     * @param values
     *            A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int update(String table, String key, HashMap<String, ByteIterator> values)
    {
        return insert(table, key, values);
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key.
     * 
     * @param table
     *            The name of the table
     * @param key
     *            The record key of the record to insert.
     * @param values
     *            A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int insert(String table, String key, HashMap<String, ByteIterator> values)
    {
        Mutator mutator = Pelops.createMutator(getPoolName());
        try
        {
            List<Column> columns = new ArrayList<Column>();
            for (Map.Entry<String, ByteIterator> entry : values.entrySet())
            {
                Column col = new Column();
                col.setName(ByteBuffer.wrap(entry.getKey().getBytes("UTF-8")));
                col.setValue(ByteBuffer.wrap(entry.getValue().toArray()));
                col.setTimestamp(System.currentTimeMillis());

                columns.add(col);
            }

            mutator.writeColumns(column_family, Bytes.fromUTF8(key), columns);
            mutator.execute(writeConsistencyLevel);

            return Ok;
        }
        catch (Exception e)
        {
            logger.error(e);
            return Error;
        }
    }

    /**
     * Delete a record from the database.
     * 
     * @param table
     *            The name of the table
     * @param key
     *            The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error
     */
    public int delete(String table, String key)
    {
        return Error;
    }

    public static void main(String[] args)
    {
        PelopsClient cli = new PelopsClient();

        Properties props = new Properties();

        props.setProperty("hosts", "localhost");
        cli.setProperties(props);

        try
        {
            cli.init();
        }
        catch (Exception e)
        {
            logger.error(e);
            System.exit(0);
        }

        HashMap<String, ByteIterator> vals = new HashMap<String, ByteIterator>();
        vals.put("age", new StringByteIterator("57"));
        vals.put("middlename", new StringByteIterator("bradley"));
        vals.put("favoritecolor", new StringByteIterator("blue"));
        int res = cli.insert("usertable", "BrianFrankCooper", vals);
        System.out.println("Result of insert: " + res);

        HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
        HashSet<String> fields = new HashSet<String>();
        fields.add("middlename");
        fields.add("age");
        fields.add("favoritecolor");
        res = cli.read("usertable", "BrianFrankCooper", null, result);
        System.out.println("Result of read: " + res);
        for (String s : result.keySet())
        {
            System.out.println("[" + s + "]=[" + result.get(s) + "]");
        }

        res = cli.delete("usertable", "BrianFrankCooper");
        System.out.println("Result of delete: " + res);
    }

    protected static String getPoolName()
    {
        return poolName;
    }
}
