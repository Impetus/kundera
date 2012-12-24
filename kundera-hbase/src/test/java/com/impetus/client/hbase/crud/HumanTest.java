package com.impetus.client.hbase.crud;

import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.hbase.junits.HBaseCli;

public class HumanTest
{

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    private Map<Object, Object> col;

    private HBaseCli cli;

    @Before
    public void setUp()
    {
        cli = new HBaseCli();
        cli.startCluster();
        emf = Persistence.createEntityManagerFactory("ilpMainSchema");
        em = emf.createEntityManager();

    }

    @Test
    public void testOps()
    {
        String humanId = "human1";
        Human human = new Human(humanId);
        human.setHumanAlive(true);
        HumansPrivatePhoto photo = new HumansPrivatePhoto(humanId);
        photo.setPhotoName("myPhoto");
        human.setHumansPrivatePhoto(photo);
        photo.setHuman(human);
        em.persist(human);

        em.clear(); // just to clear pc cache

        Human result = em.find(Human.class, humanId);
        System.out.println(result);

    }

    @After
    public void tearDown()
    {
        em.close();
        emf.close();
        if (cli != null && cli.isStarted())
        {
            cli.dropTable("Humans");
            cli.dropTable("HumansPrivatePhoto");
            cli.stopCluster("Humans");
        }

    }
}
