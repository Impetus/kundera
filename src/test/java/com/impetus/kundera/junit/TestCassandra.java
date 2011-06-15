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

import java.util.Date;

import javax.persistence.EntityManager;

import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.log4j.Logger;

import com.impetus.kundera.entity.Author;
import com.impetus.kundera.entity.PersonalDetail;
import com.impetus.kundera.entity.Post;
import com.impetus.kundera.entity.Tweet;
import com.impetus.kundera.entity.User;
import com.impetus.kundera.loader.Configuration;

/**
 * Test case for CRUD operations on Cassandra database using Kundera.
 * 
 * @author animesh.kumar
 */
public class TestCassandra extends BaseTest
{

    /** The manager. */
    private EntityManager manager;

    Configuration conf;
    
    private static Logger logger =  Logger.getLogger(TestCassandra.class);
     

    /** The embedded server cassandra. */
    private static EmbeddedCassandraService cassandra;

    public void startCassandraServer() throws Exception
    {
        super.startCassandraServer();
    }

    /**
     * Sets the up.
     * 
     * @throws java.lang.Exception
     *             * @throws Exception the exception
     * @throws Exception
     *             the exception
     */
    public void setUp() throws Exception
    {
        logger.info("starting server");
        if (cassandra == null)
        {
            //startCassandraServer();
        }
        if (conf == null)
        {
            conf = new Configuration();
            manager = conf.getEntityManager("cassandra");
        }

    }

    /**
     * Tear down.
     * 
     * @throws java.lang.Exception
     *             * @throws Exception the exception
     * @throws Exception
     *             the exception
     */
    public void tearDown() throws Exception
    {
        logger.info("destroying conf");
        conf.destroy();
    }
    
    public void testSaveUser() {
    	User user = new User();
    	user.setUserId("IIIPL-0001");
    	user.setPassword("password");
    	
    	PersonalDetail pd = new PersonalDetail();
    	pd.setPersonalDetailId("1");
    	pd.setName("Amresh");
    	pd.setRelationshipStatus("single");
    	user.setPersonalDetail(pd);
    	
    	user.addTweet(new Tweet("a", "Here it goes, my first tweet", "web"));
    	user.addTweet(new Tweet("b", "Another one from me", "mobile"));
    	
    	manager.persist(user);
    }

    /**
     * Test save authors.
     * 
     * @throws Exception
     *             the exception
     */
   /* public void testSaveAuthors() throws Exception
    {
        logger.info("onTestSaveAuthors");
        String key = System.currentTimeMillis() + "-author";
        Author animesh = createAuthor(key, "animesh@animesh.org", "India", new Date());
        manager.persist(animesh);

        // check if saved?
        Author animesh_db = manager.find(Author.class, key);
        assertEquals(animesh, animesh_db);
    }*/

    /**
     * Test save posts.
     * 
     * @throws Exception
     *             the exception
     */
   /* public void testSavePosts() throws Exception
    {
        logger.info("onTestSavePosts");
        String key = System.currentTimeMillis() + "-post";
        Post post = createPost(key, "I hate love stories", "I hate - Imran Khan, Sonal Kapoor", "Animesh", new Date(),
                "movies", "hindi");
        manager.persist(post);

        // check if saved?
        Post post_db = manager.find(Post.class, key);
        assertEquals(post, post_db);
    }*/

    /**
     * _test delete authors.
     * 
     * @throws Exception
     *             the exception
     */
    /*public void testDeleteAuthors() throws Exception
    {
        logger.info("ontestDeleteAuthors");

        String key = System.currentTimeMillis() + "-animesh";

        // save new author
        Author animesh = createAuthor(key, "animesh@animesh.org", "India", new Date());
        manager.persist(animesh);

        // delete this author
        manager.remove(animesh);

        // check if deleted?
        Author animesh_db = manager.find(Author.class, key);
        assertEquals(null, animesh_db);
    }*/

    /**
     * Creates the author.
     * 
     * @param username
     *            the user name
     * @param email
     *            the email
     * @param country
     *            the country
     * @param registeredSince
     *            the registered since
     * 
     * @return the author
     */
   /* private static Author createAuthor(String username, String email, String country, Date registeredSince)
    {
        Author author = new Author();
        author.setUsername(username);
        author.setCountry(country);
        author.setEmailAddress(email);
        author.setRegistered(registeredSince);
        return author;
    }*/

    /**
     * Creates the post.
     * 
     * @param permalink
     *            the permalink
     * @param title
     *            the title
     * @param body
     *            the body
     * @param author
     *            the author
     * @param created
     *            the created
     * @param tags
     *            the tags
     * 
     * @return the post
     */
    /*private static Post createPost(String permalink, String title, String body, String author, Date created,
            String... tags)
    {
        Post post = new Post();
        post.setTitle(title);
        post.setPermalink(permalink);
        post.setBody(body);
        post.setAuthor(author);
        post.setCreated(created);
//        post.setTags(Arrays.asList(tags));
        return post;
    }*/

}
