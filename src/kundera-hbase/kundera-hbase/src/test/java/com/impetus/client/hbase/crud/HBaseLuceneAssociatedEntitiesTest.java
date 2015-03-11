/*******************************************************************************
 * * Copyright 2014 Impetus Infotech.
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
package com.impetus.client.hbase.crud;

import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.hbase.junits.HBaseCli;
import com.impetus.kundera.utils.LuceneCleanupUtilities;

/**
 * @author Pragalbh Garg
 * 
 */
public class HBaseLuceneAssociatedEntitiesTest extends BaseTest {
    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    protected Map<String, String> propertyMap = new HashMap<String, String>();

    private HBaseCli cli;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        cli = new HBaseCli();
        cli.startCluster();
        propertyMap.put("index.home.dir", "lucene");
        emf = Persistence.createEntityManagerFactory("hbaseTest", propertyMap);
        em = emf.createEntityManager();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        em.close();
        if (cli != null) {
            cli.dropTable("KunderaExamples");
        }
        LuceneCleanupUtilities.cleanDir("./lucene");
        emf.close();
    }

    /*
     * @Test public void test1() { init();
     * 
     * 
     * }
     */

    @Test
    public void test() {
        init();
        em.clear();

        String qry = "select b from Book b";
        Query q = em.createQuery(qry);
        List<Book> books = q.getResultList();

        assertNotNull(books);
        Assert.assertEquals("title", books.get(0).getTitle());
        Assert.assertEquals(2, books.get(0).getCategory().size());
        Iterator<Categories> itr = books.get(0).getCategory().iterator();
        while (itr.hasNext()) {
            Categories cat = (Categories) itr.next();
            if (cat.getCategoryId().equals("c1")) {
                Assert.assertEquals("fiction", cat.getCategoryName());
            } else {
                Assert.assertEquals("thriller", cat.getCategoryName());
            }
        }
        Assert.assertEquals("l1", books.get(0).getLanguage().getLanguageId());
        Assert.assertEquals("isbn", books.get(0).getBookDetails().getIsbn());

        qry = "select b from Book b where b.language = :language";
        q = em.createQuery(qry);
        q.setParameter("language", "l1");
        books = q.getResultList();
        assertNotNull(books);
        Assert.assertEquals("1", books.get(0).getBookId());

        qry = "select b from Book b where b.language = :language";
        q = em.createQuery(qry);
        q.setParameter("language", "l2");
        books = q.getResultList();
        Assert.assertEquals(0, books.size());

        qry = "select b from Book b where b.bookDetails = :bookdetails";
        q = em.createQuery(qry);
        q.setParameter("bookdetails", "1");
        books = q.getResultList();
        assertNotNull(books);
        Assert.assertEquals("1", books.get(0).getBookId());
    }

    private void init() {
        Categories cat1 = new Categories();
        cat1.setCategoryId("c1");
        cat1.setCategoryName("fiction");

        Categories cat2 = new Categories();
        cat2.setCategoryId("c2");
        cat2.setCategoryName("thriller");

        Set<Categories> cat = new HashSet<Categories>();
        cat.add(cat1);
        cat.add(cat2);
        Languages lang1 = new Languages();
        lang1.setLanguage("english");
        lang1.setLanguageId("l1");

        Languages lang2 = new Languages();
        lang2.setLanguage("hindi");
        lang2.setLanguageId("l2");

        BookDetails bookDetails1 = new BookDetails();
        bookDetails1.setCopiesNum(50);
        bookDetails1.setHits(0);
        bookDetails1.setIsbn("isbn");

        Book book1 = new Book();
        book1.setAuthor("author");
        book1.setTitle("title");
        book1.setCategory(cat);
        book1.setImage("image");
        book1.setPublisher("publisher");
        book1.setDescription("description");
        book1.setLanguage(lang1);
        book1.setBookDetails(bookDetails1);

        em.persist(cat1);
        em.persist(cat2);
        em.persist(lang1);
        em.persist(lang2);
        em.persist(book1);

        em.clear();

       

    }
}
