/**
 * 
 */
package com.impetus.client.cassandra.thrift.cql;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

/**
 * @author impadmin
 * 
 */
public class LoggingConfigurationTest
{
    private EntityManagerFactory emf;

    private EntityManager em;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        Map propertyMap = new HashMap();
        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        emf = Persistence.createEntityManagerFactory("twissandraTest", propertyMap);
        em = emf.createEntityManager();

    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        em.close();
        emf.close();
    }

    @Test
    public void test()
    {
        LoggingConfiguration lc = new LoggingConfiguration();
        lc.setId("lc1");
        lc.setLable("one");
        lc.setLongName("first lc");

        LoggingConfiguration lc1 = new LoggingConfiguration();
        lc1.setId("lc2");
        lc1.setLable("two");
        lc1.setLongName("second lc");

        em.persist(lc);
        em.persist(lc1);

        em.clear();

        em.createNativeQuery(
                "insert into \"LoggingConfiguration\" (key,label,logname,nnow) values ('1','one','first lc','now')",
                LoggingConfiguration.class).executeUpdate();
        em.createNativeQuery(
                "insert into \"LoggingConfiguration\" (key,label,logname,nnow) values ('2','two','second log','now')",
                LoggingConfiguration.class).executeUpdate();

        Query query = em.createQuery("SELECT lc FROM LoggingConfiguration lc WHERE lc.logname = :logname",
                LoggingConfiguration.class);
        query.setParameter("logname", "first lc");
        List<LoggingConfiguration> matchingConfigurations = query.getResultList();

        int count = 0;
        Assert.assertNotNull(matchingConfigurations);
        for (LoggingConfiguration configuration : matchingConfigurations)
        {
            if (configuration.getId().equals("1"))
            {
                Assert.assertNotNull(configuration.getNnow());
                Assert.assertEquals("one", configuration.getLable());
                Assert.assertEquals("first lc", configuration.getLongName());
                count++;
            }
            else
            {
                Assert.assertNull(configuration.getNnow());
                Assert.assertEquals("one", configuration.getLable());
                Assert.assertEquals("lc1", configuration.getId());
                Assert.assertEquals("first lc", configuration.getLongName());
                count++;
            }
        }
        Assert.assertEquals(2, count);
        matchingConfigurations = em.createNativeQuery("SELECT * FROM \"LoggingConfiguration\"",
                LoggingConfiguration.class).getResultList();
        Assert.assertNotNull(matchingConfigurations);
        for (LoggingConfiguration configuration : matchingConfigurations)
        {
            if (configuration.getId().equals("1"))
            {
                Assert.assertNotNull(configuration.getNnow());
                Assert.assertEquals("one", configuration.getLable());
                Assert.assertEquals("first lc", configuration.getLongName());
                count++;
            }
            else if (configuration.getId().equals("2"))
            {
                Assert.assertNotNull(configuration.getNnow());
                Assert.assertEquals("two", configuration.getLable());
                Assert.assertEquals("second log", configuration.getLongName());
                count++;
            }
            else if (configuration.getId().equals("lc1"))
            {
                Assert.assertNull(configuration.getNnow());
                Assert.assertEquals("one", configuration.getLable());
                Assert.assertEquals("lc1", configuration.getId());
                Assert.assertEquals("first lc", configuration.getLongName());
                count++;
            }
            else
            {
                Assert.assertNull(configuration.getNnow());
                Assert.assertEquals("two", configuration.getLable());
                Assert.assertEquals("lc2", configuration.getId());
                Assert.assertEquals("second lc", configuration.getLongName());
                count++;
            }
        }
        Assert.assertEquals(6, count);
    }
}
