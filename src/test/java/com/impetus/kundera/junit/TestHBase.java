/**
 * 
 */
package com.impetus.kundera.junit;

import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import junit.framework.TestCase;

import com.impetus.hbase.entity.HAuthor;
import com.impetus.kundera.loader.Configuration;

/**
 * @author impetus
 *
 */
public class TestHBase extends TestCase {

	/** The manager. */
    private EntityManager manager;
//
//    /**
//     * Sets the up.
//     * 
//     * @throws java.lang.Exception * @throws Exception the exception
//     * @throws Exception the exception
//     */
//    public void setUp() throws Exception {
//   	   Configuration conf = new Configuration();
//        manager = conf.getEntityManager("hbase");
//
//    }

    @SuppressWarnings("unchecked")
	public void testOnPersist() {/*
		HAuthor animesh = createAuthor("vivek", "vivek@vivek.com", "India", new Date());
        manager.persist(animesh);		
        HAuthor animesh_db = manager.find(HAuthor.class, "vivek");
        assertEquals(animesh, animesh_db);
        Query query= manager.createQuery("select a from HAuthor a where a.country like :country");
        query.setParameter("country", "India");
		List<HAuthor> list =  query.getResultList();
        assertNotNull(list);
        for(HAuthor auth : list) {
        	System.out.println("called");
        	assertEquals("vivek", auth.getUsername());
        	assertEquals("vivek@vivek.com", auth.getEmailAddress());
        }
        
	*/}

	   /**
     * Creates the author.
     * 
     * @param username the user name
     * @param email the email
     * @param country the country
     * @param registeredSince the registered since
     * 
     * @return the author
     */
    private static HAuthor createAuthor(String username, String email, String country, Date registeredSince) {
        HAuthor author = new HAuthor();
        author.setUsername(username);
        author.setCountry(country);
        author.setEmailAddress(email);
        author.setRegistered(registeredSince);
        return author;
    }
    
}
