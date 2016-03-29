/*******************************************************************************
 *  * Copyright 2016 Impetus Infotech.
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
package com.impetus.kundera.dataasobject.polyglot;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.dataasobject.entities.Tweets;
import com.impetus.kundera.dataasobject.entities.User;
import com.impetus.kundera.dataasobject.entities.Video;

import junit.framework.Assert;

/**
 * The Class PolyglotTest.
 */
public class PolyglotTest
{

    /**
     * Sets the up before class.
     */
    @BeforeClass
    public static void SetUpBeforeClass()
    {
        User.bind("client-polyglot-properties.json", User.class);
    }

    /**
     * Test polyglot.
     */
    @Test
    public void testPolyglot()
    {
        testInsert();
        testQuery();
        testDelete();
    }

    /**
     * Test insert.
     */
    private void testInsert()
    {
        User user = new User();
        user.setFirstName("Devender");
        user.setLastName("Yadav");
        user.setUserId("u-101");
        user.setEmailId("devender.yadav@impetus.co.in");

        Set<Tweets> tweetSet = new HashSet<>();

        Tweets tweet1 = new Tweets();
        tweet1.setTweetId("t-101");
        tweet1.setBody("this is tweet 1");
        tweet1.setTweetDate(new Date());

        Set<Video> videoSet = new HashSet<>();

        Video video1 = new Video();
        video1.setVideoId("v-101");
        video1.setVideoName("movie");
        video1.setVideoProvider("netflix");

        videoSet.add(video1);
        tweet1.setVideos(videoSet);
        tweetSet.add(tweet1);
        user.setTweets(tweetSet);

        user.save();

        User u = new User().find("u-101");
        validateUser(u);
    }

    /**
     * Test query.
     */
    private void testQuery()
    {
        List<User> users = new User().query("select u from User u");
        Assert.assertEquals(1, users.size());
        User u = users.get(0);
        validateUser(u);

        users = null;

        users = new User().query("select u from User u where u.emailId = 'devender.yadav@impetus.co.in'");
        validateUser(u);
    }

    /**
     * Test delete.
     */
    private void testDelete()
    {
        User u = new User().find("u-101");
        u.delete();

        User u1 = new User().find("u-101");
        Assert.assertNull(u1);
    }

    /**
     * Tear down after class.
     *
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        User.unbind();
    }

    /**
     * Validate user.
     *
     * @param u
     *            the u
     */
    private void validateUser(User u)
    {
        Assert.assertEquals("Devender", u.getFirstName());
        Assert.assertEquals("Yadav", u.getLastName());
        Assert.assertEquals("u-101", u.getUserId());
        Assert.assertEquals("devender.yadav@impetus.co.in", u.getEmailId());

        Set<Tweets> tweets = u.getTweets();
        Assert.assertEquals(1, tweets.size());

        Tweets t = tweets.iterator().next();
        Assert.assertEquals("t-101", t.getTweetId());
        Assert.assertEquals("this is tweet 1", t.getBody());

        Set<Video> videos = t.getVideos();
        Assert.assertEquals(1, videos.size());

        Video v = videos.iterator().next();
        Assert.assertEquals("v-101", v.getVideoId());
        Assert.assertEquals("movie", v.getVideoName());
        Assert.assertEquals("netflix", v.getVideoProvider());
    }
}
