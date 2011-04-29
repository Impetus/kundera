/*
 * Copyright 2011 Impetus Infotech.
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

import javax.persistence.EntityManager;

import junit.framework.TestCase;

import com.impetus.kundera.entity.Attachment;
import com.impetus.kundera.entity.Email;
import com.impetus.kundera.loader.Configuration;

/**
 * Test case for CRUD operations on MongoDB using Kundera
 * @author amresh.singh
 */
public class TestMongoDB extends TestCase {		 
    private EntityManager em;
    Configuration conf ;

	
	@Override
	protected void setUp() throws Exception {		
		super.setUp();
		conf = new Configuration();
        em = conf.getEntityManager("mongodb");
	}
	
	public void testSaveEmail() {
		Email email = new Email();
		email.setUniqueId("1");
		email.setFrom("amresh.singh@impetus.co.in");
		email.setTo("admin@impetus.co.in");
		email.setSubject("Please Join Meeting");
		email.setBody("Please Join Meeting");
		
		Attachment atch1 = new Attachment();
		atch1.setAttachmentId("1");
		atch1.setFileName("meeting.doc");
		atch1.setFileType("MS Word");
		atch1.setFileLocation("/usr/local");
		email.addAttachment(atch1);
		
		Attachment atch2 = new Attachment();
		atch2.setAttachmentId("2");
		atch2.setFileName("mom.xls");
		atch2.setFileType("MS Excel");
		atch2.setFileLocation("/usr/local/tmp");
		email.addAttachment(atch2);
		
		em.persist(email);		
	}
	
	/*public void testFindEmail() {
		String uniqueId = "2";
		System.out.println(em.find(Email.class, uniqueId));
	}*/
	
	/*public void testDeleteEmail() {
		Email email = new Email();
		email.setUniqueId("2");
		em.remove(email);
	}*/
	
	/*public void testQuery() {
		Query q = em.createQuery("select e from Email e");		
		List<Email> emails = q.getResultList();
		
		System.out.println("Emails:" + emails);
		
	}	*/
	
	/*public void testPersistEmployee() {
		DBCollection employeeCollection = null;
		BasicDBObject employee = new BasicDBObject();
		
		employee.put("employeeId", "IIIPL-0002");
		employee.put("name", "Ashish");
		employee.put("type", "user");
		
		BasicDBObject address = new BasicDBObject();
		address.put("street", "1/2 High street");
		address.put("city", "Delhi");
		
		employee.put("address", address);
		
		employeeCollection.insert(employee);		
		DBCursor cursor = employeeCollection.find();
		while(cursor.hasNext()) {
			System.out.println("Employee: " + cursor.next());
		}		
	}*/

	
	@Override
	protected void tearDown() throws Exception {		
		super.tearDown();
		conf.destroy();
	}	
	

}
