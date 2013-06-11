/**
 * 
 */
package com.impetus.client.persistence;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.metadata.model.KunderaMetadata;

/**
 * @author Kuldeep Mishra
 * 
 */
public class EntityManagerFactoryImplTest
{

    /**
     * 
     */
    private static final String _KEYSPACE2 = "KunderaExamples";

    /**
     * 
     */
    private static final String _KEYSPACE1 = "UUIDCassandra";

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
        CassandraCli.cassandraSetUp();
        CassandraCli.createKeySpace(_KEYSPACE1);
        CassandraCli.createKeySpace(_KEYSPACE2);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        CassandraCli.dropKeySpace(_KEYSPACE1);
        CassandraCli.dropKeySpace(_KEYSPACE2);
    }

    @Test
    public void test()
    {
        String _PU1 = "cass_pu";
        String _PU2 = "secIdxCassandraTest";
        EntityManagerFactory emf1 = Persistence.createEntityManagerFactory(_PU1);
        checkEMFPropertiesAfterCreating(_PU1, emf1);
        emf1.close();
        checkEMFPropertyAfterClosing(_PU1, emf1);

        EntityManagerFactory emf2 = Persistence.createEntityManagerFactory(_PU2);
        checkEMFPropertiesAfterCreating(_PU2, emf2);
        emf2.close();
        checkEMFPropertyAfterClosing(_PU2, emf2);

        emf1 = Persistence.createEntityManagerFactory(_PU1);
        checkEMFPropertiesAfterCreating(_PU1, emf1);

        emf2 = Persistence.createEntityManagerFactory(_PU2);
        checkEMFPropertiesAfterCreating(_PU2, emf2);

        emf1.close();
        checkEMFPropertyAfterClosing(_PU1, emf1);

        checkEMFPropertiesAfterCreating(_PU2, emf2);

        emf2.close();
        checkEMFPropertyAfterClosing(_PU2, emf2);

        // On concurrent testing

        // create first emf
        OnConcurrentEMF c1 = new OnConcurrentEMF(_PU1, "emf1");
        try
        {
            c1.t.join();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        Assert.assertEquals("emf1", c1.t.getName());
        checkEMFPropertiesAfterCreating(_PU1, c1.emf);

        // create second emf
        OnConcurrentEMF c2 = new OnConcurrentEMF(_PU2, "emf2");
        try
        {
            c2.t.join();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        Assert.assertEquals("emf2", c2.t.getName());
        checkEMFPropertiesAfterCreating(_PU2, c2.emf);

        // close first emf
        c1.close();
        checkEMFPropertyAfterClosing(_PU1, c1.emf);

        // close second emf
        c2.close();
        checkEMFPropertyAfterClosing(_PU2, c2.emf);

    }

    /**
     * @param _PU1
     */
    private void checkEMFPropertyAfterClosing(String _PU1, EntityManagerFactory emf)
    {
        Assert.assertFalse(emf.isOpen());
        Assert.assertNotNull(KunderaMetadata.INSTANCE.getApplicationMetadata().getPersistenceUnitMetadata(_PU1));
        Assert.assertNotNull(KunderaMetadata.INSTANCE.getApplicationMetadata().getPersistenceUnitMetadataMap().get(_PU1));
        Assert.assertNull(KunderaMetadata.INSTANCE.getClientMetadata(_PU1));
        Assert.assertNotNull(KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodelMap().get(_PU1));
        Assert.assertNotNull(KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(_PU1));
        Assert.assertNotNull(KunderaMetadata.INSTANCE.getApplicationMetadata().getMetaModelBuilder(_PU1));
        Assert.assertNotNull(KunderaMetadata.INSTANCE.getApplicationMetadata().getSchemaMetadata().getPuToSchemaMetadata()
                .get(_PU1));
    }

    /**
     * @param pu
     */
    private void checkEMFPropertiesAfterCreating(String pu, EntityManagerFactory emf)
    {
        Assert.assertTrue(emf.isOpen());
        EntityManager em = emf.createEntityManager();
        Assert.assertNotNull(em);
        Assert.assertNotNull(KunderaMetadata.INSTANCE.getApplicationMetadata().getPersistenceUnitMetadata(pu));
        Assert.assertFalse(KunderaMetadata.INSTANCE.getApplicationMetadata().getPersistenceUnitMetadataMap().isEmpty());
        Assert.assertNotNull(KunderaMetadata.INSTANCE.getApplicationMetadata().getPersistenceUnitMetadataMap().get(pu));
        Assert.assertNotNull(KunderaMetadata.INSTANCE.getClientMetadata(pu));
        Assert.assertFalse(KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodelMap().isEmpty());
        Assert.assertNotNull(KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodelMap().get(pu));
        Assert.assertNotNull(KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(pu));
        Assert.assertNotNull(KunderaMetadata.INSTANCE.getApplicationMetadata().getMetaModelBuilder(pu));
        Assert.assertNotNull(KunderaMetadata.INSTANCE.getApplicationMetadata().getSchemaMetadata()
                .getPuToSchemaMetadata().get(pu));
        Assert.assertNull(KunderaMetadata.INSTANCE.getClientMetadata(pu).getLuceneIndexDir());
    }

    private class OnConcurrentEMF implements Runnable
    {
        public Thread t;

        private String _PU;

        public EntityManagerFactory emf;

        /**
         * @param pu
         * @param name
         * 
         */

        public OnConcurrentEMF(String pu, String name)
        {
            this._PU = pu;
            t = new Thread(this);
            t.setName(name);
            t.start();
        }

        @Override
        public void run()
        {
            emf = Persistence.createEntityManagerFactory(_PU);
            checkEMFPropertiesAfterCreating(_PU, emf);

        }

        public void close()
        {
            emf.close();
            checkEMFPropertyAfterClosing(_PU, emf);
        }
    }
}
