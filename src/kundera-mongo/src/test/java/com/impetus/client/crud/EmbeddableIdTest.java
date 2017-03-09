package com.impetus.client.crud;

import com.impetus.client.crud.entities.CompositeId;
import com.impetus.client.crud.entities.CompositeUser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import java.util.List;

/**
 * This test verifies embedded IDs work in queries
 * when the field name is different from the column name.
 */
public class EmbeddableIdTest {

   private EntityManagerFactory emf;

   /** The em. */
   private EntityManager em;

   @Before
   public void setUp()
   {
      emf = Persistence.createEntityManagerFactory("mongoTest");
      em = emf.createEntityManager();

      prepareData();
   }

   @Test
   public void testSelect()
   {
      Query query = null;
      List<?> results = null;

      query = em.createQuery("select u from CompositeUser u");
      results = query.getResultList();

      Assert.assertNotNull(results);
      Assert.assertEquals(results.size(), 3);

      query = em.createQuery("select u from CompositeUser u where u.id.birthDate = :year");
      query.setParameter("year", "1986");
      results = query.getResultList();

      Assert.assertNotNull(results);
      Assert.assertEquals(results.size(), 1);

      query = em.createQuery("select u from CompositeUser u where u.id.birthDate <= :year");
      query.setParameter("year", "1986");
      results = query.getResultList();

      Assert.assertNotNull(results);
      Assert.assertEquals(results.size(), 2);
   }

   private void prepareData()
   {
      CompositeId id1 = new CompositeId();
      id1.setFirstName("John");
      id1.setBirthDate("1981");

      CompositeUser user1 = new CompositeUser();
      user1.setId(id1);
      user1.setPhone("90001");

      em.persist(user1);

      CompositeId id2 = new CompositeId();
      id2.setFirstName("Carl");
      id2.setBirthDate("1988");

      CompositeUser user2 = new CompositeUser();
      user2.setId(id2);
      user2.setPhone("90002");

      em.persist(user2);

      CompositeId id3 = new CompositeId();
      id3.setFirstName("Viktor");
      id3.setBirthDate("1986");

      CompositeUser user3 = new CompositeUser();
      user3.setId(id3);
      user3.setPhone("90003");

      em.persist(user3);
   }

   @After
   public void tearDown()
   {
      em.close();
      emf.close();
   }

}
