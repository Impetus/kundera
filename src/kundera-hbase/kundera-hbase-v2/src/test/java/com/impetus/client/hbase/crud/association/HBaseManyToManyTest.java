package com.impetus.client.hbase.crud.association;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.Before;
import org.junit.Test;


public class HBaseManyToManyTest
{

    private EntityManagerFactory emf;

    private EntityManager em;

    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("mtmTest");
        em = emf.createEntityManager();
    }

    @Test
    public void testManyToMany()
    {
        GroupMToM g1 = new GroupMToM();
        g1.setId(11);
        g1.setName("g1");
        
        GroupMToM g2 = new GroupMToM();
        g2.setId(22);
        g2.setName("g2");
        
        List<GroupMToM> groups = new ArrayList<GroupMToM>();
        groups.add(g2);
        groups.add(g1);
        UserMToM user = new UserMToM();
        user.setId(1);
        user.setName("user");
        user.setGroups(groups);
        em.persist(user);
        em.clear();
        UserMToM uu = em.find(UserMToM.class, 1);
        
    }
    
}
