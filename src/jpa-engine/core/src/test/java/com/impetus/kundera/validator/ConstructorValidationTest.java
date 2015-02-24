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
package com.impetus.kundera.validator;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.Test;

/**
 * @author Devender Yadav
 * 
 */
public class ConstructorValidationTest {

    private EntityManagerFactory emf;

    private EntityManager em;

    @Test
    public void validConstructorTest() throws Exception {
        emf = Persistence.createEntityManagerFactory("patest");
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

    @Test
    public void invalidConstructorTest() throws Exception {

        try {
            emf = Persistence.createEntityManagerFactory("invalidJpaEntityPU");
            em = emf.createEntityManager();
            int bookId = 2;
            String title = "Head First Java";
            String author = "Kathy Sierra";
            int pages = 400;
            InvalidBookEntity book = new InvalidBookEntity(bookId, title, author, pages);
            em.persist(book);

        } catch (Exception e) {
            Assert.assertNull(emf);
            Assert
                .assertEquals(
                    "com.impetus.kundera.validator.InvalidBookEntity must have a default public or protected no-argument constructor.",
                    e.getMessage());
        }

    }

}
