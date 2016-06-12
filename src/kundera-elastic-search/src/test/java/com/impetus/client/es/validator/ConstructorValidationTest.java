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
package com.impetus.client.es.validator;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Devender Yadav
 * 
 */
public class ConstructorValidationTest
{

    private EntityManagerFactory emf;

    private EntityManager em;
    
    private Node node = null;
    
    @Before
    public void setup() throws Exception
    {
        if (!checkIfServerRunning())
        {
            Builder builder = Settings.settingsBuilder();
            builder.put("path.home", "target/data");
            node = new NodeBuilder().settings(builder).node();
        }
    }

    @Test
    public void validConstructorTest() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("es-pu");
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
    
    @After
    public void tearDown() throws Exception
    {
        if (checkIfServerRunning() && node != null)
        {
            node.close();
        }

    }

    /**
     * Check if server running.
     * 
     * @return true, if successful
     */
    private static boolean checkIfServerRunning()
    {
        try
        {
            Socket socket = new Socket("127.0.0.1", 9300);
            return socket.getInetAddress() != null;
        }
        catch (UnknownHostException e)
        {
            return false;
        }
        catch (IOException e)
        {
            return false;
        }
    }

}
