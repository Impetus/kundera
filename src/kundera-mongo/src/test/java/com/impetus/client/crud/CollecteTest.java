/*******************************************************************************
 *  * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.crud;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.crud.datatypes.entities.Collecte;
import com.impetus.client.crud.datatypes.entities.Photoo;

/**
 * Junit for https://github.com/impetus-opensource/Kundera/issues/237
 * 
 * @author vivek.mishra
 * 
 */
public class CollecteTest
{

    private static final String _PU = "mongoTest";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(_PU);
        em = emf.createEntityManager();
    }

    @Test
    public void testInsert()
    {
        // une collecte
        Collecte c = new Collecte();
        c.setId("0001");
        c.setEAN("3251248033108");
        c.setIdProduit(123342124L);
        c.setDateStatut(new Date());
        c.setStatut(0);
        // qq photos
        List<Photoo> photos = new ArrayList<Photoo>();
        Photoo p1 = new Photoo();
        p1.setNomPhoto("Photo de Face");
        p1.setMd5("1235847EA873");
        p1.setNomFichier("photo1.jpg");
        photos.add(p1);
        Photoo p2 = new Photoo();
        p2.setNomPhoto("Photo composition");
        p2.setMd5("234847EA873");
        p2.setNomFichier("photo2.jpg");
        photos.add(p2);
        Photoo p3 = new Photoo();
        p3.setNomPhoto("Photo prix");
        p3.setMd5("5164847EA873");
        p3.setNomFichier("photo3.jpg");
        photos.add(p3);

        c.setPhotos(photos);
        em.persist(c);
        testSelect();

    }
    
    @Test
    public void testNullEmbeddableInsert()
    {
        // une collecte
        Collecte c = new Collecte();
        c.setId("0001");
        c.setEAN("3251248033108");
        c.setIdProduit(123342124L);
        c.setDateStatut(new Date());
        c.setStatut(0);
               
        em.persist(c);
        testNullEmbeddable();

    }

    private void testSelect()
    {
        Query q = em.createQuery("select c from Collecte c where c.id =:id");
        q.setParameter("id", "0001");
        List<Collecte> collectes = q.getResultList();
        Collecte c = collectes.get(0);
        Assert.assertEquals(c.getEAN(), "3251248033108");
        Assert.assertEquals(c.getPhotos().size(), 3);
        Assert.assertEquals(c.getPhotos().iterator().next().getMd5(), "1235847EA873");
    }
    
    private void testNullEmbeddable()
    {
        Query q = em.createQuery("select c from Collecte c where c.id =:id");
        q.setParameter("id", "0001");
        List<Collecte> collectes = q.getResultList();
        Collecte c = collectes.get(0);
        Assert.assertEquals(c.getEAN(), "3251248033108");
        Assert.assertNull(c.getPhotos());
        
    }

    @After
    public void tearDown()
    {
        em.close();
        emf.close();

    }
}
