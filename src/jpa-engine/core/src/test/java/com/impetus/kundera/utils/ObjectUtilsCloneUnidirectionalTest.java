/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.utils;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.entity.PersonalDetail;
import com.impetus.kundera.entity.Tweet;
import com.impetus.kundera.entity.album.AlbumUni_1_M_1_M;
import com.impetus.kundera.entity.photo.PhotoUni_1_M_1_M;
import com.impetus.kundera.entity.photographer.PhotographerUni_1_M_1_M;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

/**
 * Test case for {@link ObjectUtils} for cloning for unidirectional object
 * 
 * @author amresh.singh
 */
public class ObjectUtilsCloneUnidirectionalTest
{
    private final String _persistenceUnit = "kunderatest";

    private String _keyspace = "KunderaTest";

    /** The log. */
    private static Logger log = LoggerFactory.getLogger(ObjectUtils.class);

    private EntityManagerFactoryImpl emf;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = getEntityManagerFactory(null);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    @Test
    public void testPhotographer()
    {
        // Construct photographer object
        PhotographerUni_1_M_1_M a1 = constructPhotographer(1);

        // Create a deep copy using Kundera
        long t1 = System.currentTimeMillis();
        PhotographerUni_1_M_1_M a2 = (PhotographerUni_1_M_1_M) ObjectUtils.deepCopy(a1,
                emf.getKunderaMetadataInstance());
        long t2 = System.currentTimeMillis();
        log.info("Time taken by Kundera:" + (t2 - t1));

        // Check for reference inequality
        assertObjectReferenceInequality(a1, a2);

        // Check for deep clone object equality
        Assert.assertTrue(DeepEquals.deepEquals(a1, a2));

        // Change original object
        modifyPhotographer(a1);

        // Check whether clones are unaffected from change in original object
        assertOriginalObjectValues(a2);
    }

    // @Test
    public void testBulkCopyUsingKunderaCloner()
    {
        int n = 100000;
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < n; i++)
        {
            PhotographerUni_1_M_1_M a1 = constructPhotographer(i + 1);
            PhotographerUni_1_M_1_M a2 = (PhotographerUni_1_M_1_M) ObjectUtils.deepCopy(a1, emf.getKunderaMetadataInstance());
        }
        long t2 = System.currentTimeMillis();
        log.info("Time taken by Kundera Cloner for " + n + " records:" + (t2 - t1));
    }

    /**
     * @return
     */
    private PhotographerUni_1_M_1_M constructPhotographer(int photographerId)
    {
        PhotographerUni_1_M_1_M a1 = new PhotographerUni_1_M_1_M();
        a1.setPhotographerId(photographerId);
        a1.setPhotographerName("Amresh");

        a1.setPersonalDetail(new PersonalDetail("xamry", "password1", "Single"));

        a1.addTweet(new Tweet("My First Tweet", "Web"));
        a1.addTweet(new Tweet("My Second Tweet", "Android"));
        a1.addTweet(new Tweet("My Third Tweet", "iPad"));

        a1.addTag("nosql");
        a1.addTag("kundera");
        a1.addTag("mongo");

        a1.addLikedBy(111);
        a1.addLikedBy(222);

        a1.addComment(111, "What a post!");
        a1.addComment(222, "I am getting NPE on line no. 145");
        a1.addComment(333, "My hobby is to spam blogs");

        AlbumUni_1_M_1_M b11 = new AlbumUni_1_M_1_M("b1", "Album 1", "This is album 1");
        AlbumUni_1_M_1_M b12 = new AlbumUni_1_M_1_M("b2", "Album 2", "This is album 2");

        PhotoUni_1_M_1_M c11 = new PhotoUni_1_M_1_M("c1", "Photo 1", "This is Photo 1");
        PhotoUni_1_M_1_M c12 = new PhotoUni_1_M_1_M("c2", "Photo 2", "This is Photo 2");
        PhotoUni_1_M_1_M c13 = new PhotoUni_1_M_1_M("c3", "Photo 3", "This is Photo 3");
        PhotoUni_1_M_1_M c14 = new PhotoUni_1_M_1_M("c4", "Photo 4", "This is Photo 4");

        b11.addPhoto(c11);
        b11.addPhoto(c12);
        b12.addPhoto(c13);
        b12.addPhoto(c14);
        a1.addAlbum(b11);
        a1.addAlbum(b12);
        return a1;
    }

    private void modifyPhotographer(PhotographerUni_1_M_1_M p)
    {
        p.setPhotographerId(2);
        p.setPhotographerName("Vivek");

        p.getPersonalDetail().setName("mevivs");
        p.getPersonalDetail().setPersonalDetailId("11111111111111");
        p.getPersonalDetail().setPassword("password2");
        p.getPersonalDetail().setRelationshipStatus("unknown");

        Tweet tweet1 = new Tweet("My First Tweet2", "Web2");
        tweet1.setTweetId("t1");
        Tweet tweet2 = new Tweet("My Second Tweet2", "iPad2");
        tweet2.setTweetId("t2");
        Tweet tweet3 = new Tweet("My Third Tweet2", "Text2");
        tweet3.setTweetId("t3");

        List<Tweet> tweets = new ArrayList<Tweet>();
        tweets.add(tweet1);
        tweets.add(tweet2);
        tweets.add(tweet3);
        p.setTweets(tweets);

        AlbumUni_1_M_1_M b11 = new AlbumUni_1_M_1_M("Xb1", "XAlbum 1", "XThis is album 1");
        AlbumUni_1_M_1_M b12 = new AlbumUni_1_M_1_M("Xb2", "XAlbum 2", "XThis is album 2");

        PhotoUni_1_M_1_M c11 = new PhotoUni_1_M_1_M("Xc1", "XPhoto 1", "XThis is Photo 1");
        PhotoUni_1_M_1_M c12 = new PhotoUni_1_M_1_M("Xc2", "Photo 2", "XThis is Photo 2");
        PhotoUni_1_M_1_M c13 = new PhotoUni_1_M_1_M("Xc3", "XPhoto 3", "XThis is Photo 3");
        PhotoUni_1_M_1_M c14 = new PhotoUni_1_M_1_M("Xc4", "XPhoto 4", "XThis is Photo 4");

        b11.addPhoto(c11);
        b11.addPhoto(c12);
        b12.addPhoto(c13);
        b12.addPhoto(c14);

        List<AlbumUni_1_M_1_M> albums = new ArrayList<AlbumUni_1_M_1_M>();
        albums.add(b11);
        albums.add(b12);
        p.setAlbums(albums);
    }

    private void assertOriginalObjectValues(PhotographerUni_1_M_1_M p)
    {
        Assert.assertTrue(p.getPhotographerId() == 1);
        Assert.assertTrue(p.getPhotographerName().equals("Amresh"));

        PersonalDetail pd = p.getPersonalDetail();
        Assert.assertFalse(pd.getPersonalDetailId().equals("11111111111111"));
        Assert.assertTrue(pd.getName().equals("xamry"));
        Assert.assertTrue(pd.getPassword().equals("password1"));
        Assert.assertTrue(pd.getRelationshipStatus().equals("Single"));

        List<Tweet> tweets = p.getTweets();
        Tweet t1 = tweets.get(0);
        Tweet t2 = tweets.get(1);
        Tweet t3 = tweets.get(2);

        Assert.assertFalse(t1.getTweetId().equals("t1"));
        Assert.assertTrue(t1.getBody().equals("My First Tweet"));
        Assert.assertTrue(t1.getDevice().equals("Web"));

        Assert.assertFalse(t2.getTweetId().equals("t2"));
        Assert.assertTrue(t2.getBody().equals("My Second Tweet"));
        Assert.assertTrue(t2.getDevice().equals("Android"));

        Assert.assertFalse(t3.getTweetId().equals("t3"));
        Assert.assertTrue(t3.getBody().equals("My Third Tweet"));
        Assert.assertTrue(t3.getDevice().equals("iPad"));

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

        for (AlbumUni_1_M_1_M album : p.getAlbums())
        {
            Assert.assertFalse(album.getAlbumId().startsWith("X"));
            Assert.assertFalse(album.getAlbumName().startsWith("X"));
            Assert.assertFalse(album.getAlbumDescription().startsWith("X"));

            for (PhotoUni_1_M_1_M photo : album.getPhotos())
            {
                Assert.assertFalse(photo.getPhotoId().startsWith("X"));
                Assert.assertFalse(photo.getPhotoCaption().startsWith("X"));
                Assert.assertFalse(photo.getPhotoDescription().startsWith("X"));
            }

        }

    }

    private void assertObjectReferenceInequality(PhotographerUni_1_M_1_M p1, PhotographerUni_1_M_1_M p2)
    {

        Assert.assertFalse(p1 == p2);
        Assert.assertFalse(p1.getPhotographerName() == p2.getPhotographerName());
        Assert.assertFalse(p1.getPersonalDetail() == p2.getPersonalDetail());
        Assert.assertFalse(p1.getPersonalDetail().getPersonalDetailId() == p2.getPersonalDetail().getPersonalDetailId());
        Assert.assertFalse(p1.getPersonalDetail().getName() == p2.getPersonalDetail().getName());
        Assert.assertFalse(p1.getPersonalDetail().getPassword() == p2.getPersonalDetail().getPassword());
        Assert.assertFalse(p1.getPersonalDetail().getRelationshipStatus() == p2.getPersonalDetail()
                .getRelationshipStatus());
        Assert.assertFalse(p1.getTweets() == p2.getTweets());

        for (int i = 0; i < p1.getTweets().size(); i++)
        {
            Tweet p1Tweet = p1.getTweets().get(i);
            Tweet p2Tweet = p2.getTweets().get(i);
            Assert.assertFalse(p1Tweet == p2Tweet);
            Assert.assertFalse(p1Tweet.getTweetId() == p2Tweet.getTweetId());
            Assert.assertFalse(p1Tweet.getBody() == p2Tweet.getBody());
            Assert.assertFalse(p1Tweet.getDevice() == p2Tweet.getDevice());
        }

        Assert.assertFalse(p1.getAlbums() == p2.getAlbums());
        for (int i = 0; i < p1.getAlbums().size(); i++)
        {
            AlbumUni_1_M_1_M p1Album = p1.getAlbums().get(i);
            AlbumUni_1_M_1_M p2Album = p2.getAlbums().get(i);
            Assert.assertFalse(p1Album == p2Album);
            Assert.assertFalse(p1Album.getAlbumId() == p2Album.getAlbumId());
            Assert.assertFalse(p1Album.getAlbumName() == p2Album.getAlbumName());
            Assert.assertFalse(p1Album.getAlbumDescription() == p2Album.getAlbumDescription());

            Assert.assertFalse(p1.getAlbums().get(i).getPhotos() == p2.getAlbums().get(i).getPhotos());

            for (int j = 0; j < p1.getAlbums().get(i).getPhotos().size(); j++)
            {
                PhotoUni_1_M_1_M photo1 = p1.getAlbums().get(i).getPhotos().get(j);
                PhotoUni_1_M_1_M photo2 = p2.getAlbums().get(i).getPhotos().get(j);

                Assert.assertFalse(photo1 == photo2);
                Assert.assertFalse(photo1.getPhotoId() == photo2.getPhotoId());
                Assert.assertFalse(photo1.getPhotoCaption() == photo2.getPhotoCaption());
                Assert.assertFalse(photo1.getPhotoDescription() == photo2.getPhotoDescription());
            }
        }
    }

    /**
     * Gets the entity manager factory.
     * 
     * @param useLucene
     * @param property
     * 
     * @return the entity manager factory
     */
    private EntityManagerFactoryImpl getEntityManagerFactory(String property)
    {
        return (EntityManagerFactoryImpl) Persistence.createEntityManagerFactory(_persistenceUnit);
    }

}
