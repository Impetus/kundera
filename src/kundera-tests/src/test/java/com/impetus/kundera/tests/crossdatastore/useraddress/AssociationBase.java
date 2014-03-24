/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.kundera.tests.crossdatastore.useraddress;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.persistence.EntityManager;
import javax.persistence.metamodel.Metamodel;

import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.http.HttpHost;
import org.apache.http.client.HttpClient;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import com.impetus.client.couchdb.utils.CouchDBTestUtils;
import com.impetus.client.crud.RDBMSCli;
import com.impetus.client.mongodb.MongoDBClient;
import com.impetus.client.redis.RedisPropertyReader;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.CoreMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.proxy.cglib.CglibLazyInitializerFactory;
import com.impetus.kundera.tests.cli.CleanupUtilities;
import com.impetus.kundera.tests.crossdatastore.useraddress.dao.UserAddressDaoImpl;
import com.mongodb.DB;

/**
 * The Class AssociationBase.
 * 
 * @author vivek.mishra
 */
public abstract class AssociationBase
{
    private static final String KEYSPACE = "KunderaTests";

    public static final boolean RUN_IN_EMBEDDED_MODE = true;

    public static final boolean AUTO_MANAGE_SCHEMA = false;

    protected Map propertyMap = new HashMap();

    // public static final String[] ALL_PUs_UNDER_TEST = new String[] {
    // "addCassandra"};

    // public static final String[] ALL_PUs_UNDER_TEST = new
    // String[]{/*"rdbms",*/ "twissandra", "twihbase","twingo"};
    /** The em. */
    protected EntityManager em;

    /** The dao. */
    protected UserAddressDaoImpl dao;

    protected KunderaMetadata kunderaMetadata;

    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(AssociationBase.class);

    /** The col families. */
    private String[] colFamilies;

    protected List<Object> col = new ArrayList<Object>();

    // private String persistenceUnits =
    // "redis,rdbms,addMongo,oracle_kvstore,piccandra,secIdxAddCassandra,picongo,addCassandra";
    // protected static final String[] ALL_PUs_UNDER_TEST = new String[] {
    // "addMongo", "rdbms", "redis", "addCassandra",
    // "oracle_kvstore" , "addHbase"};

    private String persistenceUnits = "rdbms,addMongo,oracle_kvstore,addCassandra,piccandra,secIdxAddCassandra,picongo,redis,addCouchdb";

    protected static final String[] ALL_PUs_UNDER_TEST = new String[] { "addMongo", "rdbms", "redis", "addCassandra",
            "oracle_kvstore", "addCouchdb" /* , "addHbase" */};

    protected RDBMSCli cli;

    private HttpClient httpClient;

    private HttpHost httpHost;

    /**
     * Sets the up internal.
     * 
     * @param colFamilies
     *            the new up internal
     */
    protected void setUpInternal(String... colFamilies)
    {
        try
        {
            cli = new RDBMSCli(KEYSPACE);
            cli.createSchema(KEYSPACE);
            CassandraCli.cassandraSetUp();
            CassandraCli.createKeySpace("Pickr");

            dao = new UserAddressDaoImpl(persistenceUnits);

            dao.closeEntityManager();
            dao.closeEntityManagerFactory();

            em = dao.getEntityManager(persistenceUnits, propertyMap);
            kunderaMetadata = ((EntityManagerFactoryImpl) em.getEntityManagerFactory()).getKunderaMetadataInstance();
            this.colFamilies = colFamilies;

            httpClient = CouchDBTestUtils.initiateHttpClient(kunderaMetadata, "addCouchdb");
            httpHost = new HttpHost("localhost", 5984);
        }
        catch (Exception e)
        {
            log.error("Error caused by ", e);
        }
    }

    /*    *//**
     * Switch over persistence units.
     * 
     * @param entityPuCol
     *            the entity pu col
     */
    /*
     * protected void switchPersistenceUnits(Map<Class, String> entityPuCol) {
     * if (entityPuCol != null) { Iterator<Class> iter =
     * entityPuCol.keySet().iterator(); log.warn("Invocation for:"); while
     * (iter.hasNext()) { Class clazz = iter.next(); String pu =
     * entityPuCol.get(clazz); Map<String, Metamodel> metaModels =
     * kunderaMetadata.getApplicationMetadata().getMetamodelMap();
     * EntityMetadata mAdd = null; for (Metamodel m : metaModels.values()) {
     * mAdd = ((MetamodelImpl) m).getEntityMetadataMap().get(clazz); if (mAdd !=
     * null) { break; } } // EntityMetadata mAdd = //
     * KunderaMetadataManager.getMetamodel
     * (pu).getEntityMetadataMap().get(clazz); mAdd.setPersistenceUnit(pu);
     * KunderaMetadataManager.getMetamodel(pu).getEntityMetadataMap().put(clazz,
     * mAdd); log.warn("persistence unit:" + pu + "class::" +
     * clazz.getCanonicalName()); } } }
     */

    /**
     * Switch over persistence units.
     * 
     * @param entityPuCol
     *            the entity pu col
     * @throws SchemaDisagreementException
     * @throws TimedOutException
     * @throws UnavailableException
     * @throws InvalidRequestException
     * @throws TException
     * @throws IOException
     */
    protected void switchPersistenceUnits(Map<Class, String> entityPuCol) throws IOException, TException,
            InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException
    {
        if (entityPuCol != null)
        {
            Iterator<Class> iter = entityPuCol.keySet().iterator();
            log.warn("Invocation for:");
            while (iter.hasNext())
            {
                Class clazz = iter.next();
                String pu = entityPuCol.get(clazz);
                // EntityMetadata mAdd = KunderaMetadataManager
                // .getEntityMetadata(clazz);

                Map<String, Metamodel> metaModels = kunderaMetadata.getApplicationMetadata().getMetamodelMap();

                EntityMetadata mAdd = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, clazz);
                for (Metamodel m : metaModels.values())
                {
                    mAdd = ((MetamodelImpl) m).getEntityMetadataMap().get(clazz.getName());
                    if (mAdd != null)
                    {
                        break;
                    }
                }

                // for a particular persistence unit, change keyspace.

                mAdd.setPersistenceUnit(pu);
                Map<String, List<String>> clazzToPu = new HashMap<String, List<String>>(1);
                List<String> pus = new ArrayList<String>(1);
                pus.add(pu);
                clazzToPu.put(clazz.getName(), pus);
                kunderaMetadata.getApplicationMetadata().setClazzToPuMap(clazzToPu);

                Metamodel metaModel = kunderaMetadata.getApplicationMetadata().getMetamodel(pu);
                ((MetamodelImpl) metaModel).addEntityMetadata(clazz, mAdd);
                kunderaMetadata.getApplicationMetadata().getMetamodelMap().put(pu, metaModel);
                // kunderaMetadata.getApplicationMetadata().addEntityMetadata(pu,
                // clazz, mAdd);
                PersistenceUnitMetadata puMetadata = kunderaMetadata.getApplicationMetadata()
                        .getPersistenceUnitMetadata(pu);

                CoreMetadata coreMetadata = new CoreMetadata();
                coreMetadata.setLazyInitializerFactory(new CglibLazyInitializerFactory());
                kunderaMetadata.setCoreMetadata(coreMetadata);

                String client = puMetadata.getProperties().getProperty(PersistenceProperties.KUNDERA_CLIENT_FACTORY);
                if (client.equalsIgnoreCase("com.impetus.client.cassandra.pelops.PelopsClientFactory")
                        || client.equalsIgnoreCase("com.impetus.client.cassandra.thrift.ThriftClientFactory"))
                {
                    if (RUN_IN_EMBEDDED_MODE)
                    {
                        CassandraCli.cassandraSetUp();
                        CassandraCli.initClient();
                    }

                    if (AUTO_MANAGE_SCHEMA)
                    {
                        if (mAdd.getTableName().equalsIgnoreCase("ADDRESS")
                                || mAdd.getTableName().equalsIgnoreCase("ADDRESS_INT"))
                        {
                            loadDataForHABITAT();
                        }
                        else if (mAdd.getTableName().equalsIgnoreCase("PERSONNEL")
                                || mAdd.getTableName().equalsIgnoreCase("PERSONNEL_INT"))
                        {
                            loadDataForPERSONNEL();
                        }
                    }

                }
                if (client.equalsIgnoreCase("com.impetus.client.hbase.HBaseClientFactory") && RUN_IN_EMBEDDED_MODE)
                {
                    // HBaseCli.startCluster();
                    // HBaseCli.createTable("PERSONNEL");
                    // HBaseCli.addColumnFamily("PERSONNEL", "PERSON_NAME");
                    // HBaseCli.addColumnFamily("PERSONNEL", "ADDRESS_ID");
                    //
                    // HBaseCli.createTable("ADDRESS");
                    // HBaseCli.addColumnFamily("ADDRESS", "STREET");
                    // HBaseCli.addColumnFamily("ADDRESS", "PERSON_ID");
                    //
                    // HBaseCli.createTable("PERSONNEL_ADDRESS");
                    // HBaseCli.addColumnFamily("PERSONNEL_ADDRESS",
                    // "ADDRESS_ID");
                    // HBaseCli.addColumnFamily("PERSONNEL_ADDRESS",
                    // "PERSON_ID");
                    // HBaseCli.addColumnFamily("PERSONNEL_ADDRESS",
                    // "JoinColumns");

                }
                if (client.equalsIgnoreCase("com.impetus.client.rdbms.RDBMSClientFactory"))
                {
                    try
                    {
                        createSchemaForPERSONNEL();
                        createSchemaForHABITAT();
                    }
                    catch (Exception e)
                    {
                        log.error("error during creating table in HSQLDB", e);
                    }

                }

                if (client.equalsIgnoreCase("com.impetus.client.couchdb.CouchDBClientFactory"))
                {
                    try
                    {
                        CouchDBTestUtils.createDatabase("KunderaTests".toLowerCase(), httpClient, httpHost);
                        CouchDBTestUtils.createViews(new String[] { "STREET", "ADDRESS_ID", "PERSON_ID" }, "ADDRESS",
                                httpHost, "KunderaTests".toLowerCase(), httpClient);
                        CouchDBTestUtils.createViews(new String[] { "PERSON_NAME", "PERSON_ID", "ADDRESS_ID" },
                                "PERSONNEL", httpHost, "KunderaTests".toLowerCase(), httpClient);
                        CouchDBTestUtils.createViews(new String[] { "PERSON_ID", "ADDRESS_ID" }, "PERSONNEL_ADDRESS",
                                httpHost, "KunderaTests".toLowerCase(), httpClient);

                        CouchDBTestUtils.createViews(new String[] { "STREET", "ADDRESS_ID", "PERSON_ID" },
                                colFamilies[0], httpHost, "KunderaTests".toLowerCase(), httpClient);
                        CouchDBTestUtils.createViews(new String[] { "PERSON_NAME", "PERSON_ID", "ADDRESS_ID" },
                                colFamilies[1], httpHost, "KunderaTests".toLowerCase(), httpClient);
                        if (colFamilies.length == 3)
                        {
                            CouchDBTestUtils.createViews(new String[] { "PERSON_ID", "ADDRESS_ID" }, colFamilies[2],
                                    httpHost, "KunderaTests".toLowerCase(), httpClient);
                        }
                    }
                    catch (Exception e)
                    {
                        log.error("error during creating design document in couchDB", e);
                    }
                }

                String schema = puMetadata.getProperty(PersistenceProperties.KUNDERA_KEYSPACE);
                mAdd.setSchema(schema != null ? schema : KEYSPACE);
                // mAdd.setSchema(schema)

                log.warn("persistence unit:" + pu + " and class:" + clazz.getCanonicalName());
            }
        }

        dao.closeEntityManager();
        em = dao.getEntityManager(persistenceUnits, propertyMap);
    }

    /**
     * Tear down internal.
     * 
     * @param ALL_PUs_UNDER_TEST
     * 
     * @throws InvalidRequestException
     *             the invalid request exception
     * @throws SchemaDisagreementException
     *             the schema disagreement exception
     */
    protected void tearDownInternal(String[] ALL_PUs_UNDER_TEST) throws InvalidRequestException,
            SchemaDisagreementException
    {
        if (!em.isOpen())
        {
            em = dao.getEntityManager(persistenceUnits, propertyMap);
        }

        // if (/*AUTO_MANAGE_SCHEMA*/true)
        // {
        truncateColumnFamily();
        truncateRdbms();

        truncateMongo();

        truncateRedis();
        // }

        for (String pu : ALL_PUs_UNDER_TEST)
        {
            CleanupUtilities.cleanLuceneDirectory(kunderaMetadata.getApplicationMetadata().getPersistenceUnitMetadata(
                    pu));
        }

        CouchDBTestUtils.dropDatabase("KunderaTests".toLowerCase(), httpClient, httpHost);
        // HBaseCli.stopCluster();
        // dao.closeEntityManagerFactory();

    }

    /**
     * 
     */
    private void truncateColumnFamily()
    {
        String[] columnFamily = new String[] { "ADDRESS", "PERSONNEL", "PERSONNEL_ADDRESS"/*
                                                                                           * ,
                                                                                           * "PERSONNEL_INT"
                                                                                           * ,
                                                                                           * "ADDRESS_INT"
                                                                                           */};
        CassandraCli.truncateColumnFamily(KEYSPACE, columnFamily);
    }

    /**
     * 
     */
    private void truncateMongo()
    {
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        MongoDBClient client = (MongoDBClient) clients.get("addMongo");
        if (client != null)
        {
            try
            {
                Field db = client.getClass().getDeclaredField("mongoDb");
                if (!db.isAccessible())
                {
                    db.setAccessible(true);
                }
                DB mongoDB = (DB) db.get(client);
                if (mongoDB.collectionExists("PERSONNEL"))
                {
                    mongoDB.getCollection("PERSONNEL").drop();
                }
                if (mongoDB.collectionExists("ADDRESS"))
                {
                    mongoDB.getCollection("ADDRESS").drop();
                }
                if (mongoDB.collectionExists("PERSONNEL_ADDRESS"))
                {
                    mongoDB.getCollection("PERSONNEL_ADDRESS").drop();
                }
                if (mongoDB.collectionExists("PERSONNEL_INT"))
                {
                    mongoDB.getCollection("PERSONNEL_INT").drop();
                }
                if (mongoDB.collectionExists("ADDRESS_INT"))
                {
                    mongoDB.getCollection("ADDRESS_INT").drop();
                }

            }
            catch (SecurityException e)
            {
                log.error("Error while truncating db", e);
            }
            catch (NoSuchFieldException e)
            {
                log.error("Error while truncating db", e);
            }
            catch (IllegalArgumentException e)
            {
                log.error("Error while truncating db", e);
            }
            catch (IllegalAccessException e)
            {
                log.error("Error while truncating db", e);
            }
        }

    }

    private void truncateRedis()

    {
        if (RedisPropertyReader.rsmd != null)
        {
            PersistenceUnitMetadata puMetadata = kunderaMetadata.getApplicationMetadata().getPersistenceUnitMetadata(
                    "redis");
            Properties props = puMetadata.getProperties();
            String contactNode = RedisPropertyReader.rsmd.getHost() != null ? RedisPropertyReader.rsmd.getHost()
                    : (String) props.get(PersistenceProperties.KUNDERA_NODES);
            String defaultPort = RedisPropertyReader.rsmd.getPort() != null ? RedisPropertyReader.rsmd.getPort()
                    : (String) props.get(PersistenceProperties.KUNDERA_PORT);
            String password = RedisPropertyReader.rsmd.getPassword() != null ? RedisPropertyReader.rsmd.getPassword()
                    : (String) props.get(PersistenceProperties.KUNDERA_PASSWORD);
            Jedis connection = new Jedis(contactNode, Integer.valueOf(defaultPort));
            connection.auth(password);
            connection.connect();
            connection.flushDB();
        }
    }

    /**
     * 
     */
    private void truncateRdbms()
    {
        try
        {
            cli.update("DELETE FROM KUNDERATESTS.PERSONNEL");
            // cli.update("DROP TABLE KUNDERATESTS.PERSONNEL");
            cli.update("DELETE FROM KUNDERATESTS.ADDRESS");
            // cli.update("DROP TABLE KUNDERATESTS.ADDRESS");
            cli.update("DELETE FROM KUNDERATESTS.PERSONNEL_ADDRESS");
            // cli.update("DROP TABLE KUNDERATESTS.PERSONNEL_ADDRESS");

        }
        catch (Exception e)
        {
            // do nothing..weird!!
        }

    }

    protected void addKeyspace(KsDef ksDef, List<CfDef> cfDefs) throws InvalidRequestException,
            SchemaDisagreementException, TException
    {
        ksDef = new KsDef(KEYSPACE, SimpleStrategy.class.getSimpleName(), cfDefs);
        // Set replication factor
        if (ksDef.strategy_options == null)
        {
            ksDef.strategy_options = new LinkedHashMap<String, String>();
        }
        // Set replication factor, the value MUST be an integer
        ksDef.strategy_options.put("replication_factor", "1");
        CassandraCli.client.system_add_keyspace(ksDef);
    }

    /**
     * Truncates schema.
     * 
     * @throws InvalidRequestException
     *             the invalid request exception
     * @throws SchemaDisagreementException
     *             the schema disagreement exception
     */
    protected void truncateSchema() throws InvalidRequestException, SchemaDisagreementException
    {
        log.warn("Truncating....");

        CassandraCli.dropColumnFamily("PERSONNEL", KEYSPACE);
        CassandraCli.dropColumnFamily("ADDRESS", KEYSPACE);
        CassandraCli.dropColumnFamily("PERSONNEL_ADDRESS", KEYSPACE);
        CassandraCli.dropKeySpace(KEYSPACE);
        // CassandraCli.truncateColumnFamily(KEYSPACE, "PERSONNEL",
        // "ADDRESS","PERSONNEL_ADDRESS");
    }

    protected abstract void loadDataForPERSONNEL() throws TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException;

    protected abstract void loadDataForHABITAT() throws TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException;

    protected abstract void createSchemaForPERSONNEL() throws SQLException;

    protected abstract void createSchemaForHABITAT() throws SQLException;

    protected void shutDownRdbmsServer() throws SQLException
    {
        if (cli != null)
        {
            try
            {
                cli.dropSchema(KEYSPACE);
                cli.shutdown();
            }
            catch (Exception e)
            {
                cli.closeConnection();
            }
            finally
            {
                cli.closeConnection();
            }

        }
    }
}
