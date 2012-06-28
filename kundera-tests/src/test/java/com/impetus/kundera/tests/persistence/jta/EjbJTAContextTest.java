package com.impetus.kundera.tests.persistence.jta;

import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FlushModeType;
import javax.persistence.Persistence;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.persistence.jta.KunderaJTAUserTransaction;
import com.impetus.kundera.tests.crossdatastore.useraddress.entities.HabitatOToOFKEntity;
import com.impetus.kundera.tests.crossdatastore.useraddress.entities.PersonnelOToOFKEntity;

/**
 * @author vivek.mishra
 * 
 */
public class EjbJTAContextTest
{
    private InitialContext initialContext;

    private UserTransaction userTransaction;

    private EntityManagerFactory emf;

    private EntityManager em;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {

//        Properties properties = new Properties();
//        properties.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.openejb.client.LocalInitialContextFactory");
//        // DO NOT DELETE IT !!!
//        // properties.put("openejb.deployments.classpath.ear", "true");
//        // properties.put("openejb.jndiname.format","{deploymentId}/{interfaceType.annotationName}");
//        // properties.put("openejb.altdd.prefix", "test");
//        // properties.put("openejb.validation.output.level", "VERBOSE");
//        // initialContext = new InitialContext(properties);
//        initialContext = new InitialContext(properties);
//        initialContext.bind("inject", this);
//        initialContext.bind("java:comp/UserTransaction", new KunderaJTAUserTransaction());

        System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.naming.java.javaURLContextFactory");
        System.setProperty(Context.URL_PKG_PREFIXES, "org.apache.naming");

        initialContext = new InitialContext();
        initialContext.createSubcontext("java:comp");
        // This is what we need to bind to get handle of JTA's
        // UserTransaction.

        initialContext.bind("java:comp/UserTransaction", new KunderaJTAUserTransaction());

        emf = Persistence.createEntityManagerFactory("secIdxAddCassandra,addMongo");
        em = emf.createEntityManager();
    }

    @Test
    public void testPersist() throws NotSupportedException, SystemException, NamingException, SecurityException,
            IllegalStateException, RollbackException, HeuristicMixedException, HeuristicRollbackException
    {

        userTransaction = (UserTransaction) initialContext.lookup("java:comp/UserTransaction");
        em.setFlushMode(FlushModeType.COMMIT);

        userTransaction.begin();
        PersonnelOToOFKEntity person = new PersonnelOToOFKEntity();
        person.setPersonId("1_p");
        person.setPersonName("crossdata-store");
        HabitatOToOFKEntity address = new HabitatOToOFKEntity();
        address.setAddressId("1_a");
        address.setStreet("my street");
        person.setAddress(address);
        try
        {
            em.persist(person);
        }
        catch (Exception ex)
        {
            HabitatOToOFKEntity found = em.find(HabitatOToOFKEntity.class, "1_a");
            Assert.assertNull(found);
        }
        userTransaction.commit();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

}
