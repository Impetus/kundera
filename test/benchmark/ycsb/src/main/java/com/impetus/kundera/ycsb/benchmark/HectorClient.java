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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import common.Logger;

/**
 * @author Kuldeep Mishra
 * 
 */
public class HectorClient extends DB
{

    private static Logger logger = Logger.getLogger(HectorClient.class);

    private static Random random = new Random();

    private static final int Ok = 0;

    private static final int Error = -1;

    private static String column_family;

    private static final String COLUMN_FAMILY_PROPERTY = "columnfamilyOrTable";

//    private static final String COLUMN_FAMILY_PROPERTY_DEFAULT = "columnfamilyOrTable";

//    private static String _keyspace = "kundera";

    private static Cluster cluster;

    private static Keyspace keyspace;

    private static String createKeyspace;

    private static final String CLUSTER = "hectorpoccluster";

//    private static final String PORT = "9160";

    static
    {
        }

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    public synchronized void init() throws DBException
    {
        if (cluster == null)
        {
            String hosts = getProperties().getProperty("hosts");
            if (hosts == null)
            {
                throw new DBException("Required property \"hosts\" missing for CassandraClient");
            }

            column_family = getProperties().getProperty(COLUMN_FAMILY_PROPERTY);
            
            System.out.println(column_family);
            String[] allhosts = hosts.split(",");
            String myhost = allhosts[random.nextInt(allhosts.length)];
            
            System.out.println(myhost);
            cluster = HFactory.getOrCreateCluster(CLUSTER, myhost + ":" + getProperties().getProperty("port"));
            keyspace = HFactory.createKeyspace(getProperties().getProperty("schema"), cluster, new AllOneConsistencyLevelPolicy());
            if (createKeyspace == null)
            {

                // Define column family...
                BasicColumnFamilyDefinition columnFamilyDefinition = new BasicColumnFamilyDefinition();
                columnFamilyDefinition.setKeyspaceName(getProperties().getProperty("schema"));
                columnFamilyDefinition.setName(column_family);

                ColumnFamilyDefinition cfDefStandard = new ThriftCfDef(columnFamilyDefinition);

                KeyspaceDefinition keyspaceDefinition = HFactory.createKeyspaceDefinition(getProperties().getProperty("schema"),
                        "org.apache.cassandra.locator.SimpleStrategy", 1, Arrays.asList(cfDefStandard));

                System.out.println("creating keyspace definition");
                try
                {
                    cluster.dropKeyspace(keyspace.getKeyspaceName());
//                    createKeyspace = cluster.addKeyspace(keyspaceDefinition);
//                    System.out.println("Added keyspace" + keyspace.getKeyspaceName());
//                    System.out.println("Added keyspace" + column_family);
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                    logger.info(e);
                    createKeyspace = cluster.addKeyspace(keyspaceDefinition);
                }
            }
        }
/*
         * cluster = HFactory.getOrCreateCluster(CLUSTER, myhost + ":" + PORT);
         * keyspace = HFactory.createKeyspace(_keyspace, cluster, new
         * AllOneConsistencyLevelPolicy()); if (createKeyspace == null) { //
         * Define column family... BasicColumnFamilyDefinition
         * columnFamilyDefinition = new BasicColumnFamilyDefinition();
         * columnFamilyDefinition.setKeyspaceName(_keyspace);
         * columnFamilyDefinition.setName(column_family);
         * 
         * ColumnFamilyDefinition cfDefStandard = new ThriftCfDef(
         * columnFamilyDefinition);
         * 
         * KeyspaceDefinition keyspaceDefinition = HFactory
         * .createKeyspaceDefinition(_keyspace,
         * "org.apache.cassandra.locator.SimpleStrategy", 1,
         * Arrays.asList(cfDefStandard)); try { createKeyspace =
         * cluster.addKeyspace(keyspaceDefinition); } catch (Exception e) {
         * createKeyspace = cluster.addKeyspace(keyspaceDefinition);
         * logger.error(e); } }
         */
    }

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one
     * DB instance per client thread.
     */
    public void cleanup() throws DBException
    {
//        HFactory.shutdownCluster(cluster);
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
            QueryResult<ColumnSlice<String, String>> queryResult;
            SliceQuery<String, String, String> sliceQuery = HFactory.createSliceQuery(keyspace, StringSerializer.get(),
                    StringSerializer.get(), StringSerializer.get());

            sliceQuery.setKey(key);
            sliceQuery.setColumnFamily(column_family);
            if (fields != null)
            {
                sliceQuery.setColumnNames(fields.toArray(new String[] {}));
            }
            else
            {
                sliceQuery.setRange(new String(new byte[0]), new String(new byte[0]), false, Integer.MAX_VALUE);
            }
            queryResult = sliceQuery.execute();
            assert queryResult.get() != null;
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
        return Error;
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
        return Error;
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
        try
        {
            Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());

            for (Map.Entry<String, ByteIterator> entry : values.entrySet())
            {
                mutator.addInsertion(key, column_family,
                        HFactory.createStringColumn(entry.getKey(), entry.getValue().toString()));
            }
            mutator.execute();

            return Ok;
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.error(e);
            return Error;
        } finally
        {
            values.clear();
            values=null;
            values = new HashMap<String, ByteIterator>();
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
        Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());
        mutator.addDeletion(key, column_family);
        mutator.execute();
        return Ok;
    }

    public static void main(String[] args)
    {
        HectorClient cli = new HectorClient();

        Properties props = new Properties();

        props.setProperty("hosts", "localhost");
        cli.setProperties(props);

        try
        {
            cli.init();
        }
        catch (Exception e)
        {
            e.printStackTrace();
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

//        res = cli.delete("usertable", "BrianFrankCooper");
        System.out.println("Result of delete: " + res);
    }
}
