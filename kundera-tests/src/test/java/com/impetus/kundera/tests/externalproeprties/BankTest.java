/**
 * 
 */
package com.impetus.kundera.tests.externalproeprties;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.tests.cli.HBaseCli;

/**
 * @author Kuldeep Mishra
 * 
 */
public class BankTest
{
    /**
     * 
     */
    private static final String _PU = "addHbase,addMongo";

    private EntityManagerFactory emf;

    private EntityManager em;

    private Map<String, Object> mongoProperties = new HashMap<String, Object>();

    private Map<String, Object> hbaseProperties = new HashMap<String, Object>();

    private Map<String, Map<String, Object>> puPropertiesMap = new HashMap<String, Map<String, Object>>();

    private HBaseCli cli;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
        if (!HBaseCli.isStarted())
        {
            HBaseCli.startCluster();
        }

        mongoProperties.put("kundera.ddl.auto.prepare", "create-drop");
        mongoProperties.put("kundera.keyspace", "KunderaKeyspace");

        hbaseProperties.put("kundera.ddl.auto.prepare", "create-drop");
        hbaseProperties.put("kundera.keyspace", "KunderaKeyspace");

        puPropertiesMap.put("addMongo", mongoProperties);
        puPropertiesMap.put("addHbase", hbaseProperties);

        emf = Persistence.createEntityManagerFactory(_PU, puPropertiesMap);
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
        mongoProperties = null;
        hbaseProperties = null;
        puPropertiesMap = null;
        // cli.startCluster();
    }

    @Test
    public void test()
    {
        Bank b = new Bank();
        b.setBankId("SBI_1");
        b.setBankName("SBI");
        Set<AccountHolder> accountHolders = new HashSet<AccountHolder>();

        AccountHolder accountHolder1 = new AccountHolder();
        AccountHolder accountHolder2 = new AccountHolder();
        accountHolder1.setAccountHolderId("1");
        accountHolder1.setAccountHoldername("kuldeep");
        accountHolder1.setTotalBalance("10000");
        accountHolder2.setAccountHolderId("2");
        accountHolder2.setAccountHoldername("amresh");
        accountHolder2.setTotalBalance("100000");
        accountHolders.add(accountHolder1);
        accountHolders.add(accountHolder2);

        b.setAccountHolders(accountHolders);

        em.persist(b);

        em.clear();

        Bank found = em.find(Bank.class, "SBI_1");
        Assert.assertNotNull(found);
        Assert.assertEquals("SBI", found.getBankName());
        Assert.assertNotNull(found.getAccountHolders());
        Assert.assertEquals(2, found.getAccountHolders().size());
    }
}
