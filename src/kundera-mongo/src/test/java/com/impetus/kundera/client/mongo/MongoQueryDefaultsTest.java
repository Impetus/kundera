package com.impetus.kundera.client.mongo;

import com.impetus.kundera.PersistenceProperties;
import junit.framework.Assert;
import org.junit.Test;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import java.util.HashMap;
import java.util.Map;

/**
 * Testing the options for defining the default maxResults setting for queries.
 */
public class MongoQueryDefaultsTest
{

   /** The emf. */
   private static EntityManagerFactory emf;

   /** The em. */
   private static EntityManager em;

   @Test
   public void testDefaultMaxResultsIsApplied()
   {
      emf = Persistence.createEntityManagerFactory("mongoTest");
      em = emf.createEntityManager();

      try
      {
         Query query = em.createQuery("select p from PersonMongo p");
         Assert.assertEquals(0, query.getMaxResults());
      }
      finally
      {
         em.close();
         emf.close();
      }
   }

   @Test
   public void testMaxResultsOverrideFromExternalProperties()
   {
      Map<String, Object> properties = new HashMap<String, Object>();
      properties.put(PersistenceProperties.KUNDERA_QUERY_DEFAULT_MAX_RESULTS, "32");

      emf = Persistence.createEntityManagerFactory("mongoTest", properties);
      em = emf.createEntityManager();

      try
      {
         Query query = em.createQuery("select p from PersonMongo p");
         Assert.assertEquals(32, query.getMaxResults());
      }
      finally
      {
         em.close();
         emf.close();
      }
   }

   @Test
   public void testMaxResultsOverrideFromPersistenceProperties()
   {
      emf = Persistence.createEntityManagerFactory("MongoDefaultsTest");
      em = emf.createEntityManager();

      try
      {
         Query query = em.createQuery("select u from UserInformation u");
         Assert.assertEquals(47, query.getMaxResults());
      }
      finally
      {
         em.close();
         emf.close();
      }
   }

   @Test
   public void testExternalPropertiesOverrideTakesPrecedence()
   {
      Map<String, Object> properties = new HashMap<String, Object>();
      properties.put(PersistenceProperties.KUNDERA_QUERY_DEFAULT_MAX_RESULTS, "91");

      emf = Persistence.createEntityManagerFactory("MongoDefaultsTest", properties);
      em = emf.createEntityManager();

      try
      {
         Query query = em.createQuery("select u from UserInformation u");
         Assert.assertEquals(91, query.getMaxResults());
      }
      finally
      {
         em.close();
         emf.close();
      }
   }

   @Test
   public void testNativeQuery()
   {
      Map<String, Object> properties = new HashMap<String, Object>();
      properties.put(PersistenceProperties.KUNDERA_QUERY_DEFAULT_MAX_RESULTS, "52");

      emf = Persistence.createEntityManagerFactory("mongoTest", properties);
      em = emf.createEntityManager();

      try
      {
         Query query = em.createNativeQuery("db.PERSON.find({})");
         Assert.assertEquals(52, query.getMaxResults());
      }
      finally
      {
         em.close();
         emf.close();
      }
   }

   @Test
   public void testNamedQuery()
   {
      Map<String, Object> properties = new HashMap<String, Object>();
      properties.put(PersistenceProperties.KUNDERA_QUERY_DEFAULT_MAX_RESULTS, "77");

      emf = Persistence.createEntityManagerFactory("mongoTest", properties);
      em = emf.createEntityManager();

      try
      {
         Query query = em.createNamedQuery("mongo.named.query");
         Assert.assertEquals(77, query.getMaxResults());
      }
      finally
      {
         em.close();
         emf.close();
      }
   }

}
