/*
 * Copyright 2010 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.junit;

import java.net.URL;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceException;

import junit.framework.TestCase;

import org.apache.cassandra.service.EmbeddedCassandraService;

import com.impetus.kundera.ejb.EntityManagerFactoryImpl;
import com.impetus.kundera.entity.Author;
import com.impetus.kundera.entity.Post;

/**
 * The Class TestKundera.
 * 
 * @author animesh.kumar
 */
public class TestKundera extends TestCase {

    /** The manager. */
    private EntityManager manager;

    /** The factory. */
    EntityManagerFactory factory;

    /** The embedded server cassandra. */
    private static EmbeddedCassandraService cassandra;

    /**
     * Sets the up.
     * 
     * @throws java.lang.Exception * @throws Exception the exception
     * @throws Exception the exception
     */
    public void setUp() throws Exception {
        URL configURL = TestKundera.class.getClassLoader().getResource("storage-conf.xml");
        try {
            String storageConfigPath = configURL.getFile().substring(1).substring(0, configURL.getFile().lastIndexOf("/"));
            System.setProperty("storage-config", storageConfigPath);
        } catch (Exception e) {
            fail("Could not find storage-config.xml sfile");
        }
        cassandra = new EmbeddedCassandraService();
        cassandra.init();
        Thread t = new Thread(cassandra);
        t.setDaemon(true);
        t.start();

        Map map = new HashMap();
        map.put("kundera.nodes", "localhost");
        // note : change it to 9160 if running in cassandra server mode the
        // embedded one runs on 9165 port
        map.put("kundera.port", "9165");
        map.put("kundera.keyspace", "Blog");
        map.put("sessionless", "false");
        map.put("kundera.client", "com.impetus.kundera.client.PelopsClient");

        factory = new EntityManagerFactoryImpl("test", map);
        manager = factory.createEntityManager();

    }

    /**
     * Tear down.
     * 
     * @throws java.lang.Exception * @throws Exception the exception
     * @throws Exception the exception
     */
    public void tearDown() throws Exception {
        manager.close();
        factory.close();
    }

    /**
     * Test save authors.
     * 
     * @throws Exception the exception
     */
    public void testSaveAuthors() throws Exception {
        String key = System.currentTimeMillis() + "-author";
        Author animesh = createAuthor(key, "animesh@animesh.org", "India", new Date());
        manager.persist(animesh);

        // check if saved?
        Author animesh_db = manager.find(Author.class, key);
        assertEquals(animesh, animesh_db);
    }

    /**
     * Test save posts.
     * 
     * @throws Exception the exception
     */
    public void testSavePosts() throws Exception {
        String key = System.currentTimeMillis() + "-post";
        Post post = createPost(key, "I hate love stories", "I hate - Imran Khan, Sonal Kapoor", "Animesh", new Date(), "movies", "hindi");
        manager.persist(post);

        // check if saved?
        Post post_db = manager.find(Post.class, key);
        System.out.println(post_db);
        assertEquals(post, post_db);
    }

    /**
     * _test delete authors.
     * 
     * @throws Exception the exception
     */
    public void testDeleteAuthors() throws Exception {
        String key = System.currentTimeMillis() + "-animesh";

        // save new author
        Author animesh = createAuthor(key, "animesh@animesh.org", "India", new Date());
        manager.persist(animesh);

        // delete this author
        manager.remove(animesh);

        // check if deleted?
        try {

            manager.find(Author.class, key);
            fail("Gosh! Author was not deleted");
        } catch (PersistenceException e) {
            System.err.println(e.getMessage());
        }
    }

    /**
     * Creates the author.
     * 
     * @param username the username
     * @param email the email
     * @param country the country
     * @param registeredSince the registered since
     * 
     * @return the author
     */
    private static Author createAuthor(String username, String email, String country, Date registeredSince) {
        Author author = new Author();
        author.setUsername(username);
        author.setCountry(country);
        author.setEmailAddress(email);
        author.setRegistered(registeredSince);
        return author;
    }

    /**
     * Creates the post.
     * 
     * @param permalink the permalink
     * @param title the title
     * @param body the body
     * @param author the author
     * @param created the created
     * @param tags the tags
     * 
     * @return the post
     */
    private static Post createPost(String permalink, String title, String body, String author, Date created, String... tags) {
        Post post = new Post();
        post.setTitle(title);
        post.setPermalink(permalink);
        post.setBody(body);
        post.setAuthor(author);
        post.setCreated(created);
        post.setTags(Arrays.asList(tags));
        return post;
    }

}
