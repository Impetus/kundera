package com.impetus.client.persistence;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.persistence.EntityManager;

import junit.framework.Assert;

import org.apache.cassandra.cli.CliParser.newColumnFamily_return;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.processor.TableProcessor;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

public class PersistWithNullField
{
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.createKeySpace("KunderaExamples");
        Cassandra.Client client = CassandraCli.getClient();
        client.set_keyspace("KunderaExamples");
        CfDef cf_def = new CfDef();
        cf_def.keyspace = "KunderaExamples";
        cf_def.name = "users";
        client.system_add_column_family(cf_def);
    }

    @Test
    public void test()
    {
        EntityManagerFactoryImpl emf = getEntityManagerFactory();
        EntityManager em = emf.createEntityManager();
        CassandraEntitySample entity = new CassandraEntitySample();
        entity.setKey("123");
        entity.setFull_name("kuldeep mishra");
        entity.setState("delhi");
        // birth_date is null

        em.persist(entity);

        CassandraEntitySample findEntity = em.find(CassandraEntitySample.class, 123);
        Assert.assertNotNull(findEntity);
        Assert.assertEquals("123", findEntity.getKey());
        Assert.assertEquals("kuldeep mishra", findEntity.getFull_name());
        Assert.assertEquals("delhi", findEntity.getState());
        Assert.assertNull(findEntity.getBirth_date());

    }

    /**
     * Gets the entity manager factory.
     * 
     * @return the entity manager factory
     */
    private EntityManagerFactoryImpl getEntityManagerFactory()
    {
        Map<String, Object> props = new HashMap<String, Object>();
        String persistenceUnit = "cassandra";
        props.put(Constants.PERSISTENCE_UNIT_NAME, persistenceUnit);
        props.put(PersistenceProperties.KUNDERA_CLIENT_FACTORY,
                "com.impetus.client.cassandra.pelops.PelopsClientFactory");
        props.put(PersistenceProperties.KUNDERA_NODES, "localhost");
        props.put(PersistenceProperties.KUNDERA_PORT, "9160");
        props.put(PersistenceProperties.KUNDERA_KEYSPACE, "KunderaExamples");
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
        PersistenceUnitMetadata puMetadata = new PersistenceUnitMetadata();
        puMetadata.setPersistenceUnitName(persistenceUnit);
        Properties p = new Properties();
        p.putAll(props);
        puMetadata.setProperties(p);
        Map<String, PersistenceUnitMetadata> metadata = new HashMap<String, PersistenceUnitMetadata>();
        metadata.put("cassandra", puMetadata);
        appMetadata.addPersistenceUnitMetadata(metadata);

        Map<String, List<String>> clazzToPu = new HashMap<String, List<String>>();

        List<String> pus = new ArrayList<String>();
        pus.add(persistenceUnit);
        clazzToPu.put(CassandraEntitySample.class.getName(), pus);

        appMetadata.setClazzToPuMap(clazzToPu);

        EntityMetadata m = new EntityMetadata(CassandraEntitySample.class);
        TableProcessor processor = new TableProcessor();
        processor.process(CassandraEntitySample.class, m);
        m.setPersistenceUnit(persistenceUnit);
        MetamodelImpl metaModel = new MetamodelImpl();
        metaModel.addEntityMetadata(CassandraEntitySample.class, m);
        appMetadata.getMetamodelMap().put(persistenceUnit, metaModel);
        EntityManagerFactoryImpl emf = new EntityManagerFactoryImpl(persistenceUnit, props);
        return emf;
    }

    @After
    public void tearDown() throws Exception
    {
        CassandraCli.dropKeySpace("KunderaExamples");
    }
}
