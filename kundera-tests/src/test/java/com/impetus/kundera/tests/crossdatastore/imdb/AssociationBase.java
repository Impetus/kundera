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
package com.impetus.kundera.tests.crossdatastore.imdb;

import java.io.IOException;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import redis.clients.jedis.Jedis;

import com.impetus.client.crud.RDBMSCli;
import com.impetus.client.redis.RedisPropertyReader;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.tests.cli.CassandraCli;
import com.impetus.kundera.tests.cli.CleanupUtilities;
import com.impetus.kundera.tests.crossdatastore.imdb.dao.IMDBDaoImpl;

/**
 * The Class AssociationBase.
 * 
 * @author vivek.mishra
 */
public abstract class AssociationBase
{
    protected static final String KEYSPACE = "imdb";

    protected static final String MOVIE = "MOVIE";

    protected static final String ACTOR = "ACTOR";

    public static final boolean RUN_IN_EMBEDDED_MODE = true;

    public static final boolean AUTO_MANAGE_SCHEMA = true;

    protected EntityManager em;

    protected IMDBDaoImpl dao;

    /** the log used by this class. */
    private static Log log = LogFactory.getLog(AssociationBase.class);

    /** The col families. */
    private String[] colFamilies;

    protected RDBMSCli cli;

    protected List<Object> col = new ArrayList<Object>();

    public static final String CASSANDRA_PU = "imdbCassandra";

    public static final String HBASE_PU = "imdbHbase";

    public static final String MONGO_PU = "addMongo";

    public static final String RDBMS_PU = "imdbRDBMS";

    public static final String REDIS_PU = "redis";

    public static final String NEO4J_PU = "imdbNeo4J";

    public static String persistenceUnits = "piccandra,picongo,secIdxAddCassandra,";

    public static final String[] ALL_PUs_UNDER_TEST = new String[] { NEO4J_PU, CASSANDRA_PU, MONGO_PU, REDIS_PU,/*
                                                                                                                 * HBASE_PU
                                                                                                                 * ,
                                                                                                                 */
    RDBMS_PU };

    // public static final String[] ALL_PUs_UNDER_TEST = new String[] {
    // NEO4J_PU, HBASE_PU};

    public static void buildPersistenceUnitsList()
    {
        for (String pu : ALL_PUs_UNDER_TEST)
        {
            persistenceUnits = persistenceUnits + pu + ",";
        }
        persistenceUnits = persistenceUnits.substring(0, persistenceUnits.length() - 1);
    }

    /**
     * Sets the up internal.
     * 
     * @param colFamilies
     *            the new up internal
     */
    protected void setUpInternal(String... colFamilies)
    {
        // if (persistenceUnits.indexOf(HBASE_PU) > 0 && AUTO_MANAGE_SCHEMA)
        //
        // {
        // if (!HBaseCli.isStarted())
        // {
        // HBaseCli.startCluster();
        // }
        // HBaseCli.createTable("MOVIE");
        // HBaseCli.addColumnFamily("MOVIE", "TITLE");
        // HBaseCli.addColumnFamily("MOVIE", "YEAR");
        // }

        dao = new IMDBDaoImpl(persistenceUnits);
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
        KunderaMetadata.INSTANCE.setCoreMetadata(null);
        em = null;
        dao.closeEntityManager();
        dao.closeEntityManagerFactory();

        em = dao.getEntityManager(persistenceUnits);
        this.colFamilies = colFamilies;

    }

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
            log.warn("\n\nInvocation for:\n--------------------------------");
            while (iter.hasNext())
            {
                Class clazz = iter.next();
                String pu = entityPuCol.get(clazz);
                Map<String, Metamodel> metaModels = KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodelMap();

                EntityMetadata mAdd = KunderaMetadataManager.getEntityMetadata(clazz);
                for (Metamodel m : metaModels.values())
                {
                    mAdd = ((MetamodelImpl) m).getEntityMetadataMap().get(clazz);
                    if (mAdd != null)
                    {
                        break;
                    }
                }
                mAdd.setPersistenceUnit(pu);
                Map<String, List<String>> clazzToPu = new HashMap<String, List<String>>(1);
                List<String> pus = new ArrayList<String>(1);
                pus.add(pu);
                clazzToPu.put(clazz.getName(), pus);
                KunderaMetadata.INSTANCE.getApplicationMetadata().setClazzToPuMap(clazzToPu);

                Metamodel metaModel = KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(pu);
                ((MetamodelImpl) metaModel).addEntityMetadata(clazz, mAdd);
                KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodelMap().put(pu, metaModel);
                // KunderaMetadata.INSTANCE.getApplicationMetadata().addEntityMetadata(pu,
                // clazz, mAdd);
                PersistenceUnitMetadata puMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                        .getPersistenceUnitMetadata(pu);

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
                        if (mAdd.getTableName().equalsIgnoreCase(MOVIE))
                        {
                            loadDataForMovie();
                        }
                        else if (mAdd.getTableName().equalsIgnoreCase(ACTOR))
                        {
                            loadDataForActor();
                        }
                    }
                }

                if (client.equalsIgnoreCase("com.impetus.client.rdbms.RDBMSClientFactory"))
                {
                    createRDBMSSchema();

                }
                String schema = puMetadata.getProperty(PersistenceProperties.KUNDERA_KEYSPACE);
                mAdd.setSchema(schema != null ? schema : KEYSPACE);
                log.warn(clazz.getSimpleName() + " in " + pu);
            }
        }

        dao.closeEntityManager();
        em = dao.getEntityManager(persistenceUnits);
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
            em = dao.getEntityManager(persistenceUnits);
        }

        if (AUTO_MANAGE_SCHEMA)
        {
            if (persistenceUnits.indexOf(CASSANDRA_PU) > 0)
                truncateCassandra();

            if (persistenceUnits.indexOf(RDBMS_PU) > 0)
            {

                truncateRdbms();
                shutDownRdbmsServer();
            }
            if (persistenceUnits.indexOf(REDIS_PU) > 0)
                truncateRedis();
        }

        for (String pu : ALL_PUs_UNDER_TEST)
        {
            CleanupUtilities.cleanLuceneDirectory(pu);
        }
    }

    /**
     * 
     */
    private void truncateCassandra()
    {
        String[] columnFamily = new String[] { ACTOR, MOVIE };
        CassandraCli.truncateColumnFamily(KEYSPACE, columnFamily);
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
        CassandraCli.dropColumnFamily("ACTOR", KEYSPACE);
        CassandraCli.dropColumnFamily("MOVIE", KEYSPACE);
        CassandraCli.dropKeySpace(KEYSPACE);
    }

    protected abstract void loadDataForActor() throws TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException;

    protected abstract void loadDataForMovie() throws TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException;

    protected void shutDownRdbmsServer()
    {
        if (cli != null)
        {
            try
            {
                if (AUTO_MANAGE_SCHEMA)
                {
                    cli.dropSchema(KEYSPACE);
                }
            }
            catch (Exception e)
            {
                try
                {
                    cli.closeConnection();
                }
                catch (SQLException e1)
                {
                }
            }
            finally
            {
                try
                {
                    cli.closeConnection();
                }
                catch (SQLException e)
                {
                }
            }

        }
    }

    private void truncateRdbms()
    {
        try
        {
            cli.update("DELETE FROM IMDB.MOVIE");
            cli.update("DROP TABLE IMDB.MOVIE");

        }
        catch (Exception e)
        {

        }
    }

    /**
     * 
     */
    protected void createRDBMSSchema()
    {
        try
        {
            if (AUTO_MANAGE_SCHEMA)
            {
                cli = new RDBMSCli(KEYSPACE);
                cli.createSchema(KEYSPACE);

                try
                {
                    cli.update("CREATE TABLE IMDB.MOVIE (MOVIE_ID VARCHAR(256) PRIMARY KEY, TITLE VARCHAR(256), YEAR VARCHAR(256))");
                }
                catch (Exception e)
                {
                    cli.update("DELETE FROM IMDB.MOVIE");
                    cli.update("DROP TABLE IMDB.MOVIE");
                    cli.update("CREATE TABLE IMDB.MOVIE (MOVIE_ID VARCHAR(256) PRIMARY KEY, TITLE VARCHAR(256), YEAR VARCHAR(256))");
                }

            }

        }
        catch (Exception e)
        {
            log.error("Error in RDBMS cli ", e);
        }
    }

    private void truncateRedis()

    {
        PersistenceUnitMetadata puMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata("redis");
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
