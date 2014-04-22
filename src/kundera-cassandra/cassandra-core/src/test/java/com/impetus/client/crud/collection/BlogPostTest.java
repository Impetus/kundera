/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.client.crud.collection;

import java.nio.ByteBuffer;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

/**
 * Test case for Storing and retrieving Blog posts. 1. Validates correct
 * functioning of Element collection for basic types 2. Checks all types of
 * collection (Set, Map, List)
 * 
 * @author amresh.singh
 */
public class BlogPostTest
{
    EntityManagerFactory emf;

    EntityManager em;

    String persistenceUnit = "secIdxCassandraTest";

    private boolean RUN_IN_EMBEDDED_MOODE = true;

    private boolean AUTO_MANAGE_SCHEMA = true;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {

        if (RUN_IN_EMBEDDED_MOODE)
        {
            CassandraCli.cassandraSetUp();
        }

        if (AUTO_MANAGE_SCHEMA)
        {

            createKeyspace();
            createColumnFamily();
        }

        emf = Persistence.createEntityManagerFactory(persistenceUnit);
        em = emf.createEntityManager();
        em.setProperty(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
    }

    @Test
    public void testCRUD()
    {
        // Insert records
        BlogPost p1 = prepareBlogPost1();
        BlogPost p2 = prepareBlogPost2();

        em.persist(p1);
        em.persist(p2);

        // Find records by ID
        em.clear();
        BlogPost pp1 = em.find(BlogPost.class, 1);
        BlogPost pp2 = em.find(BlogPost.class, 2);

        assertPost1(pp1);
        assertPost2(pp2);

        // Update records
        modifyBlogPost1(pp1);
        modifyBlogPost2(pp2);

        em.merge(pp1);
        em.merge(pp2);

        em.clear();
        pp1 = em.find(BlogPost.class, 1);
        pp2 = em.find(BlogPost.class, 2);

        assertUpdatedPost1(pp1);
        assertUpdatedPost2(pp2);

        // Remove records
        em.remove(pp1);
        em.remove(pp2);

        em.clear();
        pp1 = em.find(BlogPost.class, 1);
        pp2 = em.find(BlogPost.class, 2);

        Assert.assertNull(pp1);
        Assert.assertNull(pp2);
    }

    @Test
    public void testJPAQuery()
    {
        // Insert records
        BlogPost p1 = prepareBlogPost1();
        BlogPost p2 = prepareBlogPost2();

        em.persist(p1);
        em.persist(p2);

        // Select All query
        Query q = em.createQuery("Select p from BlogPost p");
        List<BlogPost> allPosts = q.getResultList();
        Assert.assertNotNull(allPosts);
        Assert.assertFalse(allPosts.isEmpty());
        Assert.assertEquals(2, allPosts.size());
        assertPost1(allPosts.get(0));
        assertPost2(allPosts.get(1));

        // Search over Row ID
        q = em.createQuery("Select p from BlogPost p where p.postId=:postId");
        q.setParameter("postId", 1);
        allPosts = q.getResultList();
        Assert.assertNotNull(allPosts);
        Assert.assertFalse(allPosts.isEmpty());
        Assert.assertEquals(1, allPosts.size());
        assertPost1(allPosts.get(0));

        // Search over Body column
        q = em.createQuery("Select p from BlogPost p where p.body=:body");
        q.setParameter("body", "Kundera - Knight in the shining armor!");
        allPosts = q.getResultList();
        Assert.assertNotNull(allPosts);
        Assert.assertFalse(allPosts.isEmpty());
        Assert.assertEquals(1, allPosts.size());
        assertPost2(allPosts.get(0));

        // Update Query
        q = em.createQuery("update BlogPost p set p.body=:body,p.tags=:tags,p.likedBy=:likedBy,p.comments=:comments where p.postId=1");
        modifyBlogPost1(p1);
        q.setParameter("body", p1.getBody());
        q.setParameter("tags", p1.getTags());
        q.setParameter("likedBy", p1.getLikedBy());
        q.setParameter("comments", p1.getComments());
        int updatedRecords = q.executeUpdate();

        em.clear();
        q = em.createQuery("Select p from BlogPost p where p.postId=:postId");
        q.setParameter("postId", 1);
        allPosts = q.getResultList();
        Assert.assertNotNull(allPosts);
        Assert.assertFalse(allPosts.isEmpty());
        Assert.assertEquals(1, allPosts.size());
        assertUpdatedPost1(allPosts.get(0));

        // Named Query
        em.clear();
        q = em.createNamedQuery("select.post.2");
        allPosts = q.getResultList();
        Assert.assertNotNull(allPosts);
        Assert.assertFalse(allPosts.isEmpty());
        Assert.assertEquals(1, allPosts.size());
        assertPost2(allPosts.get(0));

        // Delete Query
        q = em.createQuery("DELETE from BlogPost");
        int deleteCount = q.executeUpdate();
        Assert.assertEquals(2, deleteCount);

        em.clear();
        q = em.createQuery("Select p from BlogPost p");
        allPosts = q.getResultList();
        Assert.assertTrue(allPosts == null || allPosts.isEmpty());
    }

    @Test
    public void testNativeQuery()
    {
        // Insert records
        BlogPost p1 = prepareBlogPost1();
        BlogPost p2 = prepareBlogPost2();

        em.persist(p1);
        em.persist(p2);

        // Select All query
        Query q = em.createNativeQuery("select * from blog_posts", BlogPost.class);
        List<BlogPost> allPosts = q.getResultList();
        Assert.assertNotNull(allPosts);
        Assert.assertFalse(allPosts.isEmpty());
        Assert.assertEquals(2, allPosts.size());
        assertPost1(allPosts.get(0));
        assertPost2(allPosts.get(1));

        // Search over a column
        q = em.createNativeQuery("select * from blog_posts where body='Working with MongoDB using Kundera'",
                BlogPost.class);
        allPosts = q.getResultList();
        Assert.assertNotNull(allPosts);
        Assert.assertFalse(allPosts.isEmpty());
        Assert.assertEquals(1, allPosts.size());
        assertPost1(allPosts.get(0));

        // Updating set, list and map for Blog Post 1
        q = em.createNativeQuery("update blog_posts set body = 'Updated body 1' where post_id = 1", BlogPost.class);
        q.executeUpdate();
        q = em.createNativeQuery("update blog_posts set tags = tags + {'new tag 1'} where post_id = 1", BlogPost.class);
        q.executeUpdate();
        q = em.createNativeQuery("update blog_posts set liked_by = liked_by - [111] where post_id = 1", BlogPost.class);
        q.executeUpdate();
        q = em.createNativeQuery("update blog_posts set comments = comments + {888:'New comment 1'} where post_id = 1",
                BlogPost.class);
        q.executeUpdate();

        q = em.createNativeQuery("select * from blog_posts where post_id = 1", BlogPost.class);
        allPosts = q.getResultList();
        Assert.assertNotNull(allPosts);
        Assert.assertFalse(allPosts.isEmpty());
        Assert.assertEquals(1, allPosts.size());
        assertUpdatedPost1(allPosts.get(0));

        // Updating set, list and map for Blog Post 2
        q = em.createNativeQuery("update blog_posts set body = 'Updated body 2' where post_id = 2", BlogPost.class);
        q.executeUpdate();
        q = em.createNativeQuery("update blog_posts set tags = tags + {'new tag 2'} where post_id = 2", BlogPost.class);
        q.executeUpdate();
        q = em.createNativeQuery("update blog_posts set liked_by = liked_by - [444] where post_id = 2", BlogPost.class);
        q.executeUpdate();
        q = em.createNativeQuery("update blog_posts set comments = comments + {999:'New comment 2'} where post_id = 2",
                BlogPost.class);
        q.executeUpdate();

        // Native select query
        q = em.createNativeQuery("select * from blog_posts where post_id = 2", BlogPost.class);
        allPosts = q.getResultList();
        Assert.assertNotNull(allPosts);
        Assert.assertFalse(allPosts.isEmpty());
        Assert.assertEquals(1, allPosts.size());
        assertUpdatedPost2(allPosts.get(0));

        // Named native query
        q = em.createNativeQuery("select.post.1", BlogPost.class);
        allPosts = q.getResultList();
        Assert.assertNotNull(allPosts);
        Assert.assertFalse(allPosts.isEmpty());
        Assert.assertEquals(1, allPosts.size());
        assertUpdatedPost1(allPosts.get(0));

        // Delete all posts
        q = em.createNativeQuery("delete from blog_posts where post_id = 1", BlogPost.class);
        q.executeUpdate();
        q = em.createNativeQuery("delete from blog_posts where post_id = 2", BlogPost.class);
        q.executeUpdate();

        q = em.createNativeQuery("select * from blog_posts", BlogPost.class);
        allPosts = q.getResultList();
        Assert.assertTrue(allPosts == null || allPosts.isEmpty());
    }

    @Test
    public void testCollectionWithNullValues()
    {
        // Insert records
        BlogPost p1 = prepareBlogPost1();
        BlogPost p2 = prepareBlogPost2();

        try
        {
            // Testing map containing null as value.
            p1.addComment(444, null);
            em.persist(p1);
            Assert.fail("Should have gone into catch block.");
        }
        catch (Exception e)
        {
            Assert.assertEquals("com.impetus.kundera.KunderaException: InvalidRequestException(why:null is not supported inside collections)", e.getMessage());
        }
        try
        {
            // Testing map containing null as key and value.
            p2.addComment(null, null);
            em.persist(p2);
            Assert.fail("Should have gone into catch block.");
        }
        catch (Exception e)
        {
            Assert.assertEquals("com.impetus.kundera.KunderaException: InvalidRequestException(why:null is not supported inside collections)", e.getMessage());
        }

        p1 = prepareBlogPost1();
        p2 = prepareBlogPost2();

        try
        {
            // Testing list containing null as value.
            p1.addLikedBy(null);
            em.persist(p1);
            Assert.fail("Should have gone into catch block.");
        }
        catch (Exception e)
        {
            Assert.assertEquals("com.impetus.kundera.KunderaException: InvalidRequestException(why:null is not supported inside collections)", e.getMessage());
        }

        try
        {
            // Testing set containing null as value.
            p2.addTag(null);
            em.persist(p2);
            Assert.fail("Should have gone into catch block.");
        }
        catch (Exception e)
        {
            Assert.assertEquals("com.impetus.kundera.KunderaException: InvalidRequestException(why:null is not supported inside collections)", e.getMessage());
        }

    }

    private BlogPost prepareBlogPost1()
    {
        BlogPost p1 = new BlogPost();
        p1.setPostId(1);
        p1.setBody("Working with MongoDB using Kundera");

        p1.addTag("nosql");
        p1.addTag("kundera");
        p1.addTag("mongo");

        p1.addLikedBy(111);
        p1.addLikedBy(222);

        p1.addComment(111, "What a post!");
        p1.addComment(222, "I am getting NPE on line no. 145");
        p1.addComment(333, "My hobby is to spam blogs");
        return p1;
    }

    private BlogPost prepareBlogPost2()
    {
        BlogPost p2 = new BlogPost();
        p2.setPostId(2);
        p2.setBody("Kundera - Knight in the shining armor!");

        p2.addTag("nosql");
        p2.addTag("cassandra");
        p2.addTag("kundera");
        p2.addTag("jpa");

        p2.addLikedBy(333);
        p2.addLikedBy(444);
        p2.addLikedBy(555);

        p2.addComment(333, "Great work");
        p2.addComment(555, "Doesn't work on my machine");
        p2.addComment(777, "Wanna buy medicines from my store?");
        return p2;
    }

    private void modifyBlogPost1(BlogPost p)
    {
        p.setBody("Updated body 1");
        p.getTags().add("new tag 1");
        p.getLikedBy().remove(0);
        p.addComment(888, "New comment 1");
    }

    private void modifyBlogPost2(BlogPost p)
    {
        p.setBody("Updated body 2");
        p.getTags().add("new tag 2");
        p.getLikedBy().remove(1);
        p.addComment(999, "New comment 2");
    }

    private void assertPost1(BlogPost p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals(1, p.getPostId());
        Assert.assertEquals("Working with MongoDB using Kundera", p.getBody());

        Assert.assertNotNull(p.getTags());
        Assert.assertFalse(p.getTags().isEmpty());
        Assert.assertEquals(3, p.getTags().size());
        for (String tag : p.getTags())
        {
            Assert.assertTrue(tag.equals("nosql") || tag.equals("kundera") || tag.equals("mongo"));
        }

        Assert.assertNotNull(p.getLikedBy());
        Assert.assertFalse(p.getLikedBy().isEmpty());
        Assert.assertEquals(2, p.getLikedBy().size());
        for (int likedUserId : p.getLikedBy())
        {
            Assert.assertTrue(likedUserId == 111 || likedUserId == 222);
        }

        Assert.assertNotNull(p.getComments());
        Assert.assertFalse(p.getComments().isEmpty());
        Assert.assertEquals(3, p.getComments().size());
        for (int commentedBy : p.getComments().keySet())
        {
            String commentText = p.getComments().get(commentedBy);
            Assert.assertTrue(commentedBy == 111 || commentedBy == 222 || commentedBy == 333);
            Assert.assertTrue(commentText.equals("What a post!")
                    || commentText.equals("I am getting NPE on line no. 145")
                    || commentText.equals("My hobby is to spam blogs"));
        }
    }

    private void assertPost2(BlogPost p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals(2, p.getPostId());
        Assert.assertEquals("Kundera - Knight in the shining armor!", p.getBody());

        Assert.assertNotNull(p.getTags());
        Assert.assertFalse(p.getTags().isEmpty());
        Assert.assertEquals(4, p.getTags().size());
        for (String tag : p.getTags())
        {
            Assert.assertTrue(tag.equals("nosql") || tag.equals("cassandra") || tag.equals("kundera")
                    || tag.equals("jpa"));
        }

        Assert.assertNotNull(p.getLikedBy());
        Assert.assertFalse(p.getLikedBy().isEmpty());
        Assert.assertEquals(3, p.getLikedBy().size());
        for (int likedUserId : p.getLikedBy())
        {
            Assert.assertTrue(likedUserId == 333 || likedUserId == 444 || likedUserId == 555);
        }

        Assert.assertNotNull(p.getComments());
        Assert.assertFalse(p.getComments().isEmpty());
        Assert.assertEquals(3, p.getComments().size());
        for (int commentedBy : p.getComments().keySet())
        {
            String commentText = p.getComments().get(commentedBy);
            Assert.assertTrue(commentedBy == 333 || commentedBy == 555 || commentedBy == 777);
            Assert.assertTrue(commentText.equals("Great work") || commentText.equals("Doesn't work on my machine")
                    || commentText.equals("Wanna buy medicines from my store?"));
        }
    }

    private void assertUpdatedPost1(BlogPost p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals(1, p.getPostId());
        Assert.assertEquals("Updated body 1", p.getBody());

        Assert.assertNotNull(p.getTags());
        Assert.assertFalse(p.getTags().isEmpty());
        Assert.assertEquals(4, p.getTags().size());
        for (String tag : p.getTags())
        {
            Assert.assertTrue(tag.equals("nosql") || tag.equals("kundera") || tag.equals("mongo")
                    || tag.equals("new tag 1"));
        }

        Assert.assertNotNull(p.getLikedBy());
        Assert.assertFalse(p.getLikedBy().isEmpty());
        Assert.assertEquals(1, p.getLikedBy().size());
        for (int likedUserId : p.getLikedBy())
        {
            Assert.assertTrue(likedUserId == 222);
        }

        Assert.assertNotNull(p.getComments());
        Assert.assertFalse(p.getComments().isEmpty());
        Assert.assertEquals(4, p.getComments().size());
        for (int commentedBy : p.getComments().keySet())
        {
            String commentText = p.getComments().get(commentedBy);
            Assert.assertTrue(commentedBy == 111 || commentedBy == 222 || commentedBy == 333 || commentedBy == 888);
            Assert.assertTrue(commentText.equals("What a post!")
                    || commentText.equals("I am getting NPE on line no. 145")
                    || commentText.equals("My hobby is to spam blogs") || commentText.equals("New comment 1"));
        }

    }

    private void assertUpdatedPost2(BlogPost p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals(2, p.getPostId());
        Assert.assertEquals("Updated body 2", p.getBody());

        Assert.assertNotNull(p.getTags());
        Assert.assertFalse(p.getTags().isEmpty());
        Assert.assertEquals(5, p.getTags().size());
        for (String tag : p.getTags())
        {
            Assert.assertTrue(tag.equals("nosql") || tag.equals("cassandra") || tag.equals("kundera")
                    || tag.equals("jpa") || tag.equals("new tag 2"));
        }

        Assert.assertNotNull(p.getLikedBy());
        Assert.assertFalse(p.getLikedBy().isEmpty());
        Assert.assertEquals(2, p.getLikedBy().size());
        for (int likedUserId : p.getLikedBy())
        {
            Assert.assertTrue(likedUserId == 333 || likedUserId == 555);
        }

        Assert.assertNotNull(p.getComments());
        Assert.assertFalse(p.getComments().isEmpty());
        Assert.assertEquals(4, p.getComments().size());
        for (int commentedBy : p.getComments().keySet())
        {
            String commentText = p.getComments().get(commentedBy);
            Assert.assertTrue(commentedBy == 333 || commentedBy == 555 || commentedBy == 777 || commentedBy == 999);
            Assert.assertTrue(commentText.equals("Great work") || commentText.equals("Doesn't work on my machine")
                    || commentText.equals("Wanna buy medicines from my store?") || commentText.equals("New comment 2"));
        }
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        if (em != null && em.isOpen())
        {
            em.close();
        }
        if (emf != null && emf.isOpen())
        {
            emf.close();
        }

        if (AUTO_MANAGE_SCHEMA)
        {
            CassandraCli.executeCqlQuery("TRUNCATE blog_posts", "KunderaExamples");
            CassandraCli.executeCqlQuery("DROP TABLE blog_posts", "KunderaExamples");
            CassandraCli.executeCqlQuery("DROP KEYSPACE \"KunderaExamples\"", "KunderaExamples");
        }

    }

    private void createColumnFamily()
    {
        try
        {
            CassandraCli.executeCqlQuery("USE \"KunderaExamples\"", "KunderaExamples");
            CassandraCli
                    .executeCqlQuery(
                            "CREATE TABLE blog_posts (post_id int PRIMARY KEY, body text, tags set<text>, liked_by list<int>, comments map<int, text>)",
                            "KunderaExamples");
            CassandraCli.executeCqlQuery("CREATE INDEX ON blog_posts(body)", "KunderaExamples");
        }
        catch (Exception e)
        {
        }
    }

    private void createKeyspace()
    {
        try
        {
            CassandraCli
                    .getClient()
                    .execute_cql3_query(
                            ByteBuffer.wrap("CREATE KEYSPACE \"KunderaExamples\" WITH replication = {'class':'SimpleStrategy','replication_factor':3}"
                                    .getBytes("UTF-8")), Compression.NONE, ConsistencyLevel.ONE);
        }
        catch (Exception e)
        {

        }
    }

}
