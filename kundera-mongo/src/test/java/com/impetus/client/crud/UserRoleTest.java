package com.impetus.client.crud;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.twitter.entities.Role;
import com.impetus.client.twitter.entities.User;

/**
 * @author vivek.mishra
 *
 */
public class UserRoleTest
{
    private EntityManager em;

    @Before
    public void setUp()
    {
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("mongoTest");
        em = emf.createEntityManager();
    }
    
//    @Test
    public void testAssociation()
    {
        Role rol = new Role();
        rol.setRolId(1);
        rol.setName("Administrador");
        User u =  new User();
        u.setAge(15);
        u.setEmail("usuario1@infos.com");
        u.setName("usuario1");
        u.setUserId(1);
        u.setLastName("apellido1");
        User u2 =  new User();
        u2.setAge(17);
        u2.setEmail("usuario2@infos.com");
        u2.setName("usuario2");
        u2.setUserId(2);
        u2.setLastName("apellido2");
        u.setUserRol(rol);
        u2.setUserRol(rol);
        List<User> users = new ArrayList<User>();
        users.add(u);
        users.add(u2);
        rol.setSegUsuarioList(users);
        em.persist(rol);
        
    }
    
    @Test
    public void testFindbyRole()
    {
        String query = "Select r from Role r";
        Query q = em.createQuery(query);
        List<Role> roles = q.getResultList();
        Assert.assertNotNull(roles);
        Assert.assertEquals(1, roles.size());
    }
    
    @Test
    public void testFindbyUser()
    {
        String query = "Select u from User u";
        Query q = em.createQuery(query);
        List<User> users = q.getResultList();
        Assert.assertNotNull(users);
        Assert.assertEquals(2, users.size());
    }
    
    @After
    public void tearDown()
    {
    }
}
