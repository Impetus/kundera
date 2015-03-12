/**
 * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.hbase.validator;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.Test;

/**
 * The Class ConstructorValidationTest.
 * 
 * @author Devender Yadav
 */
public class ConstructorValidationTest
{

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    /**
     * Valid constructor test.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void validConstructorTest() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("queryTest");
        em = emf.createEntityManager();
        BookEntity book = new BookEntity();
        book.setBookId(1);
        book.setTitle("The Complete Reference");
        book.setAuthor("Herbert Schildt");
        book.setPages(500);

        em.persist(book);

        em.clear();

        BookEntity book1 = em.find(BookEntity.class, 1);

        Assert.assertNotNull(book1);
        Assert.assertEquals(1, book1.getBookId());
        Assert.assertEquals("The Complete Reference", book1.getTitle());
        Assert.assertEquals("Herbert Schildt", book1.getAuthor());
        Assert.assertEquals(500, book1.getPages());

        em.remove(book1);

        BookEntity book2 = em.find(BookEntity.class, 1);
        Assert.assertNull(book2);

        em.close();
        emf.close();
    }

}
