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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;

import com.impetus.client.mongodb.MongoDBClient;
import com.impetus.client.rdbms.HibernateClient;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.tests.cli.CassandraCli;
import com.impetus.kundera.tests.cli.CleanupUtilities;
import com.impetus.kundera.tests.cli.HBaseCli;
import com.impetus.kundera.tests.crossdatastore.useraddress.dao.UserAddressDaoImpl;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

/**
 * The Class AssociationBase.
 * 
 * @author vivek.mishra
 */
public abstract class AssociationBase
{
    public static final boolean RUN_IN_EMBEDDED_MODE = true;

    public static final boolean AUTO_MANAGE_SCHEMA = true;

    // public static final String[] ALL_PUs_UNDER_TEST = new String[] {
    // /*,"rdbms"*/ "addCassandra","addHbase", "addMongo" };

    // public static final String[] ALL_PUs_UNDER_TEST = new String[] {
    // "addCassandra"};

    // public static final String[] ALL_PUs_UNDER_TEST = new
    // String[]{/*"rdbms",*/ "twissandra", "twihbase","twingo"};
    /** The em. */
    protected EntityManager em;

    /** The dao. */
    protected UserAddressDaoImpl dao;

    /** the log used by this class. */
    private static Log log = LogFactory.getLog(AssociationBase.class);

    /** The col families. */
    private String[] colFamilies;

    protected List<Object> col = new ArrayList<Object>();

    private String persistenceUnits = "rdbms,addCassandra,addMongo";

    // private String persistenceUnits = "rdbms,addHbase";

    /**
     * Sets the up internal.
     * 
     * @param colFamilies
     *            the new up internal
     */
    protected void setUpInternal(String... colFamilies)
    {

        // String persistenceUnits = "rdbms,twissandra";
        dao = new UserAddressDaoImpl(persistenceUnits);
        em = dao.getEntityManager(persistenceUnits);
        this.colFamilies = colFamilies;
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
     * KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodelMap();
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

                Map<String, Metamodel> metaModels = KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodelMap();
                EntityMetadata mAdd = null;
                for (Metamodel m : metaModels.values())
                {
                    mAdd = ((MetamodelImpl) m).getEntityMetadataMap().get(clazz);
                    if (mAdd != null)
                    {
                        break;
                    }
                }
                mAdd.setPersistenceUnit(pu);

                PersistenceUnitMetadata puMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                        .getPersistenceUnitMetadata(pu);

                String client = puMetadata.getProperties().getProperty(PersistenceProperties.KUNDERA_CLIENT_FACTORY);
                if (client.equalsIgnoreCase("com.impetus.client.cassandra.pelops.PelopsClientFactory"))
                {
                    if (RUN_IN_EMBEDDED_MODE)
                    {
                        CassandraCli.cassandraSetUp();
                        CassandraCli.initClient();

                        // HBaseCli cli = new HBaseCli();
                        // cli.startCluster();
                    }

                    if (AUTO_MANAGE_SCHEMA)
                    {
                        if (mAdd.getTableName().equalsIgnoreCase("ADDRESS"))
                        {
                            loadDataForHABITAT();
                        }
                        else if (mAdd.getTableName().equalsIgnoreCase("PERSONNEL"))
                        {
                            loadDataForPERSONNEL();
                        }
                    }

                }
                if (client.equalsIgnoreCase("com.impetus.client.hbase.HBaseClientFactory") && !RUN_IN_EMBEDDED_MODE)
                {
                    if (!HBaseCli.isStarted())
                    {
                        HBaseCli.startCluster();
                    }
                    HBaseCli.createTable("PERSONNEL");
                    HBaseCli.addColumnFamily("PERSONNEL", "PERSON_NAME");
                    HBaseCli.addColumnFamily("PERSONNEL", "ADDRESS_ID");

                    HBaseCli.createTable("ADDRESS");
                    HBaseCli.addColumnFamily("ADDRESS", "STREET");
                    HBaseCli.addColumnFamily("ADDRESS", "PERSON_ID");

                    HBaseCli.createTable("PERSONNEL_ADDRESS");
                    HBaseCli.addColumnFamily("PERSONNEL_ADDRESS", "ADDRESS_ID");
                    HBaseCli.addColumnFamily("PERSONNEL_ADDRESS", "PERSON_ID");
                    HBaseCli.addColumnFamily("PERSONNEL_ADDRESS", "JoinColumns");
                }

                String schema = puMetadata.getProperty(PersistenceProperties.KUNDERA_KEYSPACE);
                mAdd.setSchema(schema != null ? schema : "test");
                // mAdd.setSchema(schema)

                log.warn("persistence unit:" + pu + " and class:" + clazz.getCanonicalName());
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
        truncateRdbms();

        truncateMongo();

        if (AUTO_MANAGE_SCHEMA)
        {
            truncateColumnFamily();
        }

        for (String pu : ALL_PUs_UNDER_TEST)
        {
            CleanupUtilities.cleanLuceneDirectory(pu);
        }

        dao.closeEntityManagerFactory();

    }

    /**
     * 
     */
    private void truncateColumnFamily()
    {
        String[] columnFamily = new String[] { "ADDRESS", "PERSONNEL", "PERSONNEL_ADDRESS" };
        CassandraCli.truncateColumnFamily("KunderaTests", columnFamily);
    }

    /**
     * 
     */
    private void truncateMongo()
    {
        
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        MongoDBClient client = (MongoDBClient) clients.get("addMongo");
        if(client != null)
        {
            try
            {
                Field db = client.getClass().getDeclaredField("mongoDb");
                if(!db.isAccessible())
                {
                    db.setAccessible(true);
                }
                DB mongoDB =  (DB) db.get(client);
                mongoDB.getCollection("PERSONNEL").drop();
                mongoDB.getCollection("ADDRESS").drop();
                mongoDB.getCollection("PERSONNEL_ADDRESS").drop();
            }
            catch (SecurityException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (NoSuchFieldException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (IllegalArgumentException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (IllegalAccessException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        
    }

    /**
     * 
     */
    private void truncateRdbms()
    {
        Map<String, Client> clients = (Map<String, Client>) em.getDelegate();
        HibernateClient client = (HibernateClient) clients.get("rdbms");
        try
        {
            Field sessionFactory = client.getClass().getDeclaredField("sf");
            if (!sessionFactory.isAccessible())
            {
                sessionFactory.setAccessible(true);
            }
            SessionFactory sf = (SessionFactory) sessionFactory.get(client);
            StatelessSession s = sf.openStatelessSession();
            s.createSQLQuery("delete from ADDRESS").executeUpdate();
            s.createSQLQuery("delete from PERSONNEL").executeUpdate();
            s.createSQLQuery("delete from PERSONNEL_ADDRESS").executeUpdate();
            s.close();
            // sf.close();
        }
        catch (SecurityException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (NoSuchFieldException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (IllegalArgumentException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (IllegalAccessException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    protected void addKeyspace(KsDef ksDef, List<CfDef> cfDefs) throws InvalidRequestException,
            SchemaDisagreementException, TException
    {
        ksDef = new KsDef("KunderaTests", SimpleStrategy.class.getSimpleName(), cfDefs);
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

        CassandraCli.dropColumnFamily("PERSONNEL", "KunderaTests");
        CassandraCli.dropColumnFamily("ADDRESS", "KunderaTests");
        CassandraCli.dropColumnFamily("PERSONNEL_ADDRESS", "KunderaTests");
        CassandraCli.dropKeySpace("KunderaTests");
    }

    protected abstract void loadDataForPERSONNEL() throws TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException;

    protected abstract void loadDataForHABITAT() throws TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException;

}
