package com.impetus.client;



import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PlayerTest
{
    
    
    @Test
    public void testPersist()
    {
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("testHibernate");
        
        EntityManager em = emf.createEntityManager();
        em.persist(prepareObject());
/*        HibernateClient client = new HibernateClient("testHibernate");
        
        EnhancedEntity enhanceEntity = new CglibEnhancedEntity(prepareObject(), null, null);
        try
        {
            client.persist(enhanceEntity);
        }
        catch (Exception e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
*/        
    }

    @Before
    public void setUp() throws Exception
    {
    }

    @After
    public void tearDown() throws Exception
    {
    }

    
    private Player prepareObject()
    {
        Player player = new Player();
        player.setFirstName("vivek");
        player.setJerseyNumber(10);
        player.setLastName("mishra");
        player.setId("1");
        player.setLastSpokenWords("i will finish it to win!");
        return player;
    }
}
