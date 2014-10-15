
package com.impetus.client.hbase.crud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.utils.LuceneCleanupUtilities;

/**
 * @author Shaheed.Hussain
 * 
 */
public class HBaseLuceneTest extends BaseTest
{
    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    protected Map<String,String> propertyMap = new HashMap<String, String>();


    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        propertyMap.put("index.home.dir", "lucene");    
        emf = Persistence.createEntityManagerFactory("hbaseTest",propertyMap);
        em = emf.createEntityManager();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        LuceneCleanupUtilities.cleanLuceneDirectory(((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance()
                .getApplicationMetadata().getPersistenceUnitMetadata("hbaseTest"));
        emf.close();
    }

    @Test
    public void test()
    {
        init();
        em.clear();

        String qry = "Select p.personName, p.age from PersonHBase p where p.personId = 1 and p.age = 10";
        Query q = em.createQuery(qry);
        List<PersonHBase> persons = q.getResultList();
        assertNotNull(persons);
        assertEquals(1, persons.size());
        assertNull(persons.get(0).getMonth());
        assertNull(persons.get(0).getDay());
        assertEquals(10, persons.get(0).getAge().intValue());
        assertEquals("vivek", persons.get(0).getPersonName());

        qry = "Select p.personName from PersonHBase p where p.age=20";
        q = em.createQuery(qry);
        persons = q.getResultList();
        assertNotNull(persons);
        assertEquals(1, persons.size());
        assertNull(persons.get(0).getMonth());
        assertNull(persons.get(0).getDay());
        assertNull(persons.get(0).getAge());
        assertEquals("vivek", persons.get(0).getPersonName());

        qry = "Select p.age from PersonHBase p where p.personId = 2";
        q = em.createQuery(qry);
        persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(1, persons.size());
        assertNull(persons.get(0).getMonth());
        assertNull(persons.get(0).getDay());
        assertNull(persons.get(0).getPersonName());
        assertEquals(20, persons.get(0).getAge().intValue());

        qry = "Select p from PersonHBase p";
        q = em.createQuery(qry);
        persons = q.getResultList();
        Assert.assertNotNull(persons);
        Assert.assertEquals(3, persons.size());
    }

    private void init()
    {
        Object p1 = prepareHbaseInstance("1", 10);
        Object p2 = prepareHbaseInstance("2", 20);
        Object p3 = prepareHbaseInstance("3", 15);
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);
    }
}
