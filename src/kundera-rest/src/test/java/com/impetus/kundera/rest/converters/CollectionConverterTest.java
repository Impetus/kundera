/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.rest.converters;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.ws.rs.core.MediaType;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.rest.common.Book;
import com.impetus.kundera.rest.common.ExternalLink;
import com.impetus.kundera.rest.common.JAXBUtils;
import com.impetus.kundera.rest.common.PreferenceCassandra;
import com.impetus.kundera.rest.common.ProfessionalDetailCassandra;
import com.impetus.kundera.rest.common.StreamUtils;
import com.impetus.kundera.rest.common.TweetCassandra;
import com.impetus.kundera.rest.common.UserCassandra;

/**
 * @author amresh
 * 
 */
public class CollectionConverterTest {

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for
	 * {@link com.impetus.kundera.rest.converters.CollectionConverter#toString(java.util.Collection, java.lang.Class, java.lang.String)}
	 * .
	 */
	@Test
	public void testToStringCollectionOfQClassOfQString() {
		List books = new ArrayList();

		Book book1 = new Book();
		book1.setIsbn("11111111111");
		book1.setAuthor("Amresh");
		book1.setPublication("AAA");

		Book book2 = new Book();
		book2.setIsbn("22222222222");
		book2.setAuthor("Vivek");
		book2.setPublication("BBB");

		books.add(book1);
		books.add(book2);

		String s = CollectionConverter.toString(books, Book.class,
				MediaType.APPLICATION_JSON);

		Assert.assertNotNull(s);
	}

	@Test
	public void testToCollection() {

		String s = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><books><book><author>Saurabh</author><isbn>1111111111111</isbn><publication>Willey</publication></book><book><author>Vivek</author><isbn>2222222222222</isbn><publication>OReilly</publication></book><books>";
		Collection c = CollectionConverter.toCollection(s, ArrayList.class,
				Book.class, MediaType.APPLICATION_XML);
		Assert.assertNotNull(c);
		Assert.assertFalse(c.isEmpty());
		Assert.assertEquals(2, c.size());
		
		s = "[{\"isbn\":\"1111111111111\",\"author\":\"Amresh\", \"publication\":\"Willey\"},{\"isbn\":\"2222222222222\",\"author\":\"Vivek\", \"publication\":\"Oreilly\"}]";
		
		c = CollectionConverter.toCollection(s, ArrayList.class,
            Book.class, MediaType.APPLICATION_JSON);
        Assert.assertNotNull(c);
        Assert.assertFalse(c.isEmpty());
        Assert.assertEquals(2, c.size());

	}

	@Test
    public void testComplexCollection(){
		
		List users = new ArrayList();
		
    	List<UserCassandra> friendList = new ArrayList<UserCassandra>();
    	
    	List<UserCassandra> followers = new ArrayList<UserCassandra>();
    	
    	 UserCassandra user1 = new UserCassandra("001", "Amresh", "password1", "married");
    	 
         UserCassandra user2 = new UserCassandra("002", "Vivek", "password1", "married");
         
         UserCassandra user3 = new UserCassandra("0003", "Kuldeep", "password1", "single");

         friendList.add(user2);
         Calendar cal = Calendar.getInstance();
         cal.setTime(new Date(1344079067777l));

         
         user1.setProfessionalDetail(new ProfessionalDetailCassandra(1234567, true, 31, 'C', (byte) 8, (short) 5, (float) 10.0, 163.12, new Date(
                 Long.parseLong("1351667541111")), new Date(Long.parseLong("1351667542222")), new Date(
                 Long.parseLong("1351667543333")), 2, new Long(3634521523423L), new Double(7.23452342343),
     
         new BigInteger("123456789"), new BigDecimal(123456789), cal));
         
         user2.setProfessionalDetail(new ProfessionalDetailCassandra(1234568, true, 21, 'C', (byte) 8, (short) 5, (float) 10.0, 163.12, new Date(
                 Long.parseLong("1351667541111")), new Date(Long.parseLong("1351667542222")), new Date(
                 Long.parseLong("1351667543333")), 2, new Long(3634521523423L), new Double(7.23452342343),
     
         new BigInteger("123456789"), new BigDecimal(123456789), cal));

         user1.setPreference(new PreferenceCassandra("P1", "Motif", "2"));
         
         followers.add(user3);
         
         friendList.add(user2);
         
         user1.setFriends(friendList);
         
         user1.setFollowers(followers);
         
         Set<ExternalLink> externalLinks = new HashSet<ExternalLink>();
         List<TweetCassandra> tweetList = new ArrayList<TweetCassandra>();
         
         externalLinks.add(new ExternalLink("L1", "Facebook", "http://facebook.com/coolnerd"));
         externalLinks.add(new ExternalLink("L2", "LinkedIn", "http://linkedin.com/in/devilmate"));
         user1.setExternalLinks(externalLinks);
         
         
         tweetList.add(new TweetCassandra("Here is my first tweet", "Web"));
         tweetList.add(new TweetCassandra("Second Tweet from me", "Mobile"));
         user1.setTweets(tweetList);
         
        
         String userString1 = JAXBUtils.toString(user1, MediaType.APPLICATION_JSON);
         String userString2= JAXBUtils.toString(user2, MediaType.APPLICATION_JSON);
         String userString3 = JAXBUtils.toString(user3, MediaType.APPLICATION_JSON);
         
         
         
         users.add(user1);
         users.add(user2);
         users.add(user3);
         
         String userList = CollectionConverter.toString(users, UserCassandra.class, MediaType.APPLICATION_JSON);
         Assert.assertNotNull( userList);
         
         
         user1 = (UserCassandra) JAXBUtils.toObject(StreamUtils.toInputStream(userString1), UserCassandra.class, MediaType.APPLICATION_JSON);
         Assert.assertNotNull(user1);
         
         
         user2 = (UserCassandra) JAXBUtils.toObject(StreamUtils.toInputStream(userString2), UserCassandra.class, MediaType.APPLICATION_JSON);
         Assert.assertNotNull(user2);
         
         
         user3 = (UserCassandra) JAXBUtils.toObject(StreamUtils.toInputStream(userString3), UserCassandra.class, MediaType.APPLICATION_JSON);
         Assert.assertNotNull(user3);
         
    }
}
