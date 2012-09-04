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
package com.impetus.kundera.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.configure.PersistenceUnitConfiguration;
import com.impetus.kundera.entity.PersonalDetail;
import com.impetus.kundera.entity.Tweet;
import com.impetus.kundera.entity.album.AlbumBi_1_1_1_1;
import com.impetus.kundera.entity.album.AlbumBi_1_M_1_M;
import com.impetus.kundera.entity.album.AlbumUni_1_1_1_1;
import com.impetus.kundera.entity.album.AlbumUni_1_1_1_M;
import com.impetus.kundera.entity.album.AlbumUni_1_1_M_1;
import com.impetus.kundera.entity.album.AlbumUni_1_M_1_M;
import com.impetus.kundera.entity.album.AlbumUni_1_M_M_M;
import com.impetus.kundera.entity.album.AlbumUni_M_1_1_M;
import com.impetus.kundera.entity.album.AlbumUni_M_M_1_1;
import com.impetus.kundera.entity.album.AlbumUni_M_M_M_M;
import com.impetus.kundera.entity.photo.PhotoBi_1_1_1_1;
import com.impetus.kundera.entity.photo.PhotoBi_1_M_1_M;
import com.impetus.kundera.entity.photo.PhotoUni_1_1_1_1;
import com.impetus.kundera.entity.photo.PhotoUni_1_1_1_M;
import com.impetus.kundera.entity.photo.PhotoUni_1_1_M_1;
import com.impetus.kundera.entity.photo.PhotoUni_1_M_1_M;
import com.impetus.kundera.entity.photo.PhotoUni_1_M_M_M;
import com.impetus.kundera.entity.photo.PhotoUni_M_1_1_M;
import com.impetus.kundera.entity.photo.PhotoUni_M_M_1_1;
import com.impetus.kundera.entity.photo.PhotoUni_M_M_M_M;
import com.impetus.kundera.entity.photographer.PhotographerBi_1_1_1_1;
import com.impetus.kundera.entity.photographer.PhotographerBi_1_M_1_M;
import com.impetus.kundera.entity.photographer.PhotographerUni_1_1_1_1;
import com.impetus.kundera.entity.photographer.PhotographerUni_1_1_1_M;
import com.impetus.kundera.entity.photographer.PhotographerUni_1_1_M_1;
import com.impetus.kundera.entity.photographer.PhotographerUni_1_M_1_M;
import com.impetus.kundera.entity.photographer.PhotographerUni_1_M_M_M;
import com.impetus.kundera.entity.photographer.PhotographerUni_M_1_1_M;
import com.impetus.kundera.entity.photographer.PhotographerUni_M_M_1_1;
import com.impetus.kundera.entity.photographer.PhotographerUni_M_M_M_M;
import com.impetus.kundera.graph.BillingCounter;
import com.impetus.kundera.graph.Store;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.processor.TableProcessor;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

/**
 * Test case for {@link ObjectUtils} for cloning for bidirectional object
 * 
 * @author amresh.singh
 */
public class ObjectUtilsCloneBidirectionalTest
{
    /** The log. */
    private static Logger log = LoggerFactory.getLogger(ObjectUtilsCloneBidirectionalTest.class);

    // Configurator configurator = new Configurator("kunderatest");
    EntityMetadata metadata;

    private String _persistenceUnit = "kunderatest";

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        // configurator.configure();
        getEntityManagerFactory();
        new PersistenceUnitConfiguration("kunderatest").configure();
        // new MetamodelConfiguration("kunderatest").configure();
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
        PhotographerBi_1_M_1_M a1 = new PhotographerBi_1_M_1_M();
        a1.setPhotographerId(1);
        a1.setPhotographerName("Amresh");

        a1.setPersonalDetail(new PersonalDetail("xamry", "password1", "Single"));

        a1.addTweet(new Tweet("My First Tweet", "Web"));
        a1.addTweet(new Tweet("My Second Tweet", "Android"));
        a1.addTweet(new Tweet("My Third Tweet", "iPad"));

        AlbumBi_1_M_1_M b11 = new AlbumBi_1_M_1_M("b1", "Album 1", "This is album 1");
        AlbumBi_1_M_1_M b12 = new AlbumBi_1_M_1_M("b2", "Album 2", "This is album 2");

        PhotoBi_1_M_1_M c11 = new PhotoBi_1_M_1_M("c1", "Photo 1", "This is Photo 1");
        PhotoBi_1_M_1_M c12 = new PhotoBi_1_M_1_M("c2", "Photo 2", "This is Photo 2");
        PhotoBi_1_M_1_M c13 = new PhotoBi_1_M_1_M("c3", "Photo 3", "This is Photo 3");
        PhotoBi_1_M_1_M c14 = new PhotoBi_1_M_1_M("c4", "Photo 4", "This is Photo 4");

        b11.addPhoto(c11);
        b11.addPhoto(c12);
        b12.addPhoto(c13);
        b12.addPhoto(c14);
        a1.addAlbum(b11);
        a1.addAlbum(b12);

        b11.setPhotographer(a1);
        b12.setPhotographer(a1);
        c11.setAlbum(b11);
        c12.setAlbum(b11);
        c13.setAlbum(b12);
        c14.setAlbum(b12);

        // Create a deep copy using cloner
        long t1 = System.currentTimeMillis();
        PhotographerBi_1_M_1_M a3 = (PhotographerBi_1_M_1_M) ObjectUtils.deepCopyUsingCloner(a1);
        long t2 = System.currentTimeMillis();
        log.info("Time taken by Deep Cloner:" + (t2 - t1));

        // Create a deep copy using Kundera
        long t3 = System.currentTimeMillis();
        metadata = KunderaMetadataManager.getEntityMetadata(PhotographerBi_1_M_1_M.class);
        PhotographerBi_1_M_1_M a2 = (PhotographerBi_1_M_1_M) ObjectUtils.deepCopy(a1);
        long t4 = System.currentTimeMillis();
        log.info("Time taken by Kundera:" + (t4 - t3));

        // Check for reference inequality
        assertObjectReferenceInequality(a1, a2);
        assertObjectReferenceInequality(a1, a3);

        // Check for deep clone object equality
        Assert.assertTrue(DeepEquals.deepEquals(a1, a2));
        Assert.assertTrue(DeepEquals.deepEquals(a1, a3));

        // Change original object
        modifyPhotographer(a1);

        // Check whether clones are unaffected from change in original object
        assertOriginalObjectValues(a2);
        assertOriginalObjectValues(a3);

    }

    private void modifyPhotographer(PhotographerBi_1_M_1_M p)
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

        AlbumBi_1_M_1_M b11 = new AlbumBi_1_M_1_M("Xb1", "XAlbum 1", "XThis is album 1");
        AlbumBi_1_M_1_M b12 = new AlbumBi_1_M_1_M("Xb2", "XAlbum 2", "XThis is album 2");

        PhotoBi_1_M_1_M c11 = new PhotoBi_1_M_1_M("Xc1", "XPhoto 1", "XThis is Photo 1");
        PhotoBi_1_M_1_M c12 = new PhotoBi_1_M_1_M("Xc2", "Photo 2", "XThis is Photo 2");
        PhotoBi_1_M_1_M c13 = new PhotoBi_1_M_1_M("Xc3", "XPhoto 3", "XThis is Photo 3");
        PhotoBi_1_M_1_M c14 = new PhotoBi_1_M_1_M("Xc4", "XPhoto 4", "XThis is Photo 4");

        b11.addPhoto(c11);
        b11.addPhoto(c12);
        b12.addPhoto(c13);
        b12.addPhoto(c14);

        b11.setPhotographer(p);
        b12.setPhotographer(p);
        c11.setAlbum(b11);
        c12.setAlbum(b11);
        c13.setAlbum(b12);
        c14.setAlbum(b12);

        List<AlbumBi_1_M_1_M> albums = new ArrayList<AlbumBi_1_M_1_M>();
        albums.add(b11);
        albums.add(b12);
        p.setAlbums(albums);
    }

    private void assertOriginalObjectValues(PhotographerBi_1_M_1_M p)
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

        for (AlbumBi_1_M_1_M album : p.getAlbums())
        {
            Assert.assertFalse(album.getAlbumId().startsWith("X"));
            Assert.assertFalse(album.getAlbumName().startsWith("X"));
            Assert.assertFalse(album.getAlbumDescription().startsWith("X"));

            for (PhotoBi_1_M_1_M photo : album.getPhotos())
            {
                Assert.assertFalse(photo.getPhotoId().startsWith("X"));
                Assert.assertFalse(photo.getPhotoCaption().startsWith("X"));
                Assert.assertFalse(photo.getPhotoDescription().startsWith("X"));
            }

        }

    }

    private void assertObjectReferenceInequality(PhotographerBi_1_M_1_M p1, PhotographerBi_1_M_1_M p2)
    {

        Assert.assertFalse(p1 == p2);
        Assert.assertFalse(p1.getPersonalDetail() == p2.getPersonalDetail());
        Assert.assertFalse(p1.getTweets() == p2.getTweets());

        for (int i = 0; i < p1.getTweets().size(); i++)
        {
            Assert.assertFalse(p1.getTweets().get(i) == p2.getTweets().get(i));
        }

        Assert.assertFalse(p1.getAlbums() == p2.getAlbums());
        for (int i = 0; i < p1.getAlbums().size(); i++)
        {
            Assert.assertFalse(p1.getAlbums().get(i) == p2.getAlbums().get(i));
            Assert.assertFalse(p1.getAlbums().get(i).getPhotos() == p2.getAlbums().get(i).getPhotos());
            Assert.assertFalse(p1.getAlbums().get(i).getPhotographer() == p2.getAlbums().get(i).getPhotographer());

            for (int j = 0; j < p1.getAlbums().get(i).getPhotos().size(); j++)
            {
                PhotoBi_1_M_1_M photo1 = p1.getAlbums().get(i).getPhotos().get(j);
                PhotoBi_1_M_1_M photo2 = p2.getAlbums().get(i).getPhotos().get(j);

                Assert.assertFalse(photo1 == photo2);
                Assert.assertFalse(photo1.getAlbum() == photo2.getAlbum());
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
    private EntityManagerFactoryImpl getEntityManagerFactory()
    {
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();

        Map<String, List<String>> clazzToPu = new HashMap<String, List<String>>();

        List<String> pus = new ArrayList<String>();
        pus.add(_persistenceUnit);

        clazzToPu.put(Store.class.getName(), pus);
        clazzToPu.put(BillingCounter.class.getName(), pus);
        clazzToPu.put(PhotographerBi_1_M_1_M.class.getName(), pus);
        clazzToPu.put(AlbumBi_1_M_1_M.class.getName(), pus);
        clazzToPu.put(PhotoBi_1_M_1_M.class.getName(), pus);

        appMetadata.setClazzToPuMap(clazzToPu);

        EntityMetadata m = new EntityMetadata(Store.class);
        EntityMetadata m1 = new EntityMetadata(BillingCounter.class);
        EntityMetadata m11 = new EntityMetadata(PhotographerBi_1_M_1_M.class);
        EntityMetadata m12 = new EntityMetadata(AlbumBi_1_M_1_M.class);
        EntityMetadata m13 = new EntityMetadata(PhotoBi_1_M_1_M.class);

        TableProcessor processor = new TableProcessor();
        processor.process(Store.class, m);
        processor.process(BillingCounter.class, m1);
        processor.process(PhotographerBi_1_M_1_M.class, m11);
        processor.process(AlbumBi_1_M_1_M.class, m12);
        processor.process(PhotoBi_1_M_1_M.class, m13);

        m.setPersistenceUnit(_persistenceUnit);

        MetamodelImpl metaModel = new MetamodelImpl();
        metaModel.addEntityMetadata(Store.class, m);
        metaModel.addEntityMetadata(BillingCounter.class, m1);
        metaModel.addEntityMetadata(PhotographerBi_1_M_1_M.class, m11);
        metaModel.addEntityMetadata(AlbumBi_1_M_1_M.class, m12);
        metaModel.addEntityMetadata(PhotoBi_1_M_1_M.class, m13);

        metaModel.assignManagedTypes(appMetadata.getMetaModelBuilder(_persistenceUnit).getManagedTypes());
        metaModel.assignEmbeddables(appMetadata.getMetaModelBuilder(_persistenceUnit).getEmbeddables());
        metaModel.assignMappedSuperClass(appMetadata.getMetaModelBuilder(_persistenceUnit).getMappedSuperClassTypes());

        appMetadata.getMetamodelMap().put(_persistenceUnit, metaModel);
        return null;
    }
}
