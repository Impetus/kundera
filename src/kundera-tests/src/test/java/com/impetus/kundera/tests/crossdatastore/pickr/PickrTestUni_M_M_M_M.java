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
package com.impetus.kundera.tests.crossdatastore.pickr;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.persistence.CassandraCli;
import com.impetus.kundera.tests.crossdatastore.pickr.entities.album.AlbumUni_M_M_M_M;
import com.impetus.kundera.tests.crossdatastore.pickr.entities.photo.PhotoUni_M_M_M_M;
import com.impetus.kundera.tests.crossdatastore.pickr.entities.photographer.PhotographerUni_M_M_M_M;

/**
 * @author amresh.singh
 * 
 */
public class PickrTestUni_M_M_M_M extends PickrBaseTest
{
    private static Logger log = LoggerFactory.getLogger(PickrTestUni_M_M_M_M.class);

    @Before
    public void setUp() throws Exception
    {
        log.info("Executing PICKR Test: " + this.getClass().getSimpleName() + "\n======"
                + "==========================================================");
        super.setUp();
    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {
        super.tearDown();
    }

    @Test
    public void test()
    {
        executeTests();
    }

    @Override
    public void addPhotographer()
    {
        List<PhotographerUni_M_M_M_M> ps = populatePhotographers();

        for (PhotographerUni_M_M_M_M p : ps)
        {
            pickr.addPhotographer(p);
        }
    }

    @Override
    protected void getPhotographer()
    {
        PhotographerUni_M_M_M_M p1 = (PhotographerUni_M_M_M_M) pickr.getPhotographer(PhotographerUni_M_M_M_M.class, 1);
        assertPhotographer(p1, 1);

        PhotographerUni_M_M_M_M p2 = (PhotographerUni_M_M_M_M) pickr.getPhotographer(PhotographerUni_M_M_M_M.class, 2);
        assertPhotographer(p2, 2);
    }

    @Override
    protected void updatePhotographer()
    {
        PhotographerUni_M_M_M_M p1 = (PhotographerUni_M_M_M_M) pickr.getPhotographer(PhotographerUni_M_M_M_M.class, 1);
        assertPhotographer(p1, 1);
        p1.setPhotographerName("Amresh2");

        pickr.mergePhotographer(p1);

        PhotographerUni_M_M_M_M p1Modified = (PhotographerUni_M_M_M_M) pickr.getPhotographer(
                PhotographerUni_M_M_M_M.class, 1);

        assertModifiedPhotographer(p1Modified, 1);

        PhotographerUni_M_M_M_M p2 = (PhotographerUni_M_M_M_M) pickr.getPhotographer(PhotographerUni_M_M_M_M.class, 2);
        assertPhotographer(p2, 2);
        p2.setPhotographerName("Vivek2");

        pickr.mergePhotographer(p2);

        PhotographerUni_M_M_M_M p2Modified = (PhotographerUni_M_M_M_M) pickr.getPhotographer(
                PhotographerUni_M_M_M_M.class, 2);
        assertModifiedPhotographer(p2Modified, 2);
    }

    @Override
    protected void getAllPhotographers()
    {
        List<Object> ps = pickr.getAllPhotographers(PhotographerUni_M_M_M_M.class.getSimpleName());

        for (Object p : ps)
        {
            PhotographerUni_M_M_M_M pp = (PhotographerUni_M_M_M_M) p;
            Assert.assertNotNull(pp);
            assertModifiedPhotographer(pp, pp.getPhotographerId());
        }
    }

    @Override
    protected void deletePhotographer()
    {
        PhotographerUni_M_M_M_M p1 = (PhotographerUni_M_M_M_M) pickr.getPhotographer(PhotographerUni_M_M_M_M.class, 1);
        assertModifiedPhotographer(p1, 1);
        pickr.deletePhotographer(p1);

        PhotographerUni_M_M_M_M p1AfterDeletion = (PhotographerUni_M_M_M_M) pickr.getPhotographer(
                PhotographerUni_M_M_M_M.class, 1);
        Assert.assertNull(p1AfterDeletion);

        PhotographerUni_M_M_M_M p2 = (PhotographerUni_M_M_M_M) pickr.getPhotographer(PhotographerUni_M_M_M_M.class, 2);
        Assert.assertNotNull(p2);
        pickr.deletePhotographer(p2);

        PhotographerUni_M_M_M_M p2AfterDeletion = (PhotographerUni_M_M_M_M) pickr.getPhotographer(
                PhotographerUni_M_M_M_M.class, 2);
        Assert.assertNull(p2AfterDeletion);

    }

    private void assertPhotographer(PhotographerUni_M_M_M_M p, int photographerId)
    {

        if (photographerId == 1)
        {
            Assert.assertNotNull(p);
            Assert.assertEquals(1, p.getPhotographerId());
            Assert.assertEquals("Amresh", p.getPhotographerName());

            Assert.assertNotNull(p.getAlbums());
            Assert.assertFalse(p.getAlbums().isEmpty());
            Assert.assertEquals(2, p.getAlbums().size());

            AlbumUni_M_M_M_M album1 = p.getAlbums().get(0);
            Assert.assertNotNull(album1);
            Assert.assertTrue(album1.getAlbumId().startsWith("album_"));

            Assert.assertNotNull(album1.getPhotos());
            Assert.assertFalse(album1.getPhotos().isEmpty());
            Assert.assertEquals(2, album1.getPhotos().size());

            AlbumUni_M_M_M_M album2 = p.getAlbums().get(1);
            Assert.assertNotNull(album2);
            Assert.assertTrue(album2.getAlbumId().startsWith("album_"));
            Assert.assertNotNull(album2.getPhotos());
            Assert.assertFalse(album2.getPhotos().isEmpty());
            Assert.assertEquals(2, album2.getPhotos().size());

        }
        else if (photographerId == 2)
        {
            Assert.assertNotNull(p);
            Assert.assertEquals(2, p.getPhotographerId());
            Assert.assertEquals("Vivek", p.getPhotographerName());

            Assert.assertNotNull(p.getAlbums());
            Assert.assertFalse(p.getAlbums().isEmpty());
            Assert.assertEquals(2, p.getAlbums().size());

            AlbumUni_M_M_M_M album1 = p.getAlbums().get(0);
            Assert.assertNotNull(album1);
            Assert.assertTrue(album1.getAlbumId().startsWith("album_"));

            Assert.assertNotNull(album1.getPhotos());
            Assert.assertFalse(album1.getPhotos().isEmpty());
            Assert.assertEquals(2, album1.getPhotos().size());

            AlbumUni_M_M_M_M album2 = p.getAlbums().get(1);
            Assert.assertNotNull(album2);
            Assert.assertTrue(album2.getAlbumId().startsWith("album_"));
            Assert.assertNotNull(album2.getPhotos());
            Assert.assertFalse(album2.getPhotos().isEmpty());
            Assert.assertEquals(2, album2.getPhotos().size());

        }
        else
        {
            Assert.fail("Invalid Photographer ID: " + photographerId);
        }

    }

    private void assertModifiedPhotographer(PhotographerUni_M_M_M_M p, int photographerId)
    {

        if (photographerId == 1)
        {
            Assert.assertNotNull(p);
            Assert.assertEquals(1, p.getPhotographerId());
            Assert.assertEquals("Amresh2", p.getPhotographerName());

            Assert.assertNotNull(p.getAlbums());
            Assert.assertFalse(p.getAlbums().isEmpty());
            Assert.assertEquals(2, p.getAlbums().size());

            AlbumUni_M_M_M_M album1 = p.getAlbums().get(0);
            Assert.assertNotNull(album1);
            Assert.assertTrue(album1.getAlbumId().startsWith("album_"));

            Assert.assertNotNull(album1.getPhotos());
            Assert.assertFalse(album1.getPhotos().isEmpty());
            Assert.assertEquals(2, album1.getPhotos().size());

            AlbumUni_M_M_M_M album2 = p.getAlbums().get(1);
            Assert.assertNotNull(album2);
            Assert.assertTrue(album2.getAlbumId().startsWith("album_"));
            Assert.assertNotNull(album2.getPhotos());
            Assert.assertFalse(album2.getPhotos().isEmpty());
            Assert.assertEquals(2, album2.getPhotos().size());

        }
        else if (photographerId == 2)
        {
            Assert.assertNotNull(p);
            Assert.assertEquals(2, p.getPhotographerId());
            Assert.assertEquals("Vivek2", p.getPhotographerName());

            Assert.assertNotNull(p.getAlbums());
            Assert.assertFalse(p.getAlbums().isEmpty());
            Assert.assertEquals(2, p.getAlbums().size());

            AlbumUni_M_M_M_M album1 = p.getAlbums().get(0);
            Assert.assertNotNull(album1);
            Assert.assertTrue(album1.getAlbumId().startsWith("album_"));

            Assert.assertNotNull(album1.getPhotos());
            Assert.assertFalse(album1.getPhotos().isEmpty());
            Assert.assertEquals(2, album1.getPhotos().size());

            AlbumUni_M_M_M_M album2 = p.getAlbums().get(1);
            Assert.assertNotNull(album2);
            Assert.assertTrue(album2.getAlbumId().startsWith("album_"));
            Assert.assertNotNull(album2.getPhotos());
            Assert.assertFalse(album2.getPhotos().isEmpty());
            Assert.assertEquals(2, album2.getPhotos().size());

        }
        else
        {
            Assert.fail("Invalid Photographer ID: " + photographerId);
        }

    }

    private List<PhotographerUni_M_M_M_M> populatePhotographers()
    {
        List<PhotographerUni_M_M_M_M> photographers = new ArrayList<PhotographerUni_M_M_M_M>();

        // Photographer 1
        PhotographerUni_M_M_M_M p1 = new PhotographerUni_M_M_M_M();
        p1.setPhotographerId(1);
        p1.setPhotographerName("Amresh");

        // Photographer 2
        PhotographerUni_M_M_M_M p2 = new PhotographerUni_M_M_M_M();
        p2.setPhotographerId(2);
        p2.setPhotographerName("Vivek");

        AlbumUni_M_M_M_M album1 = new AlbumUni_M_M_M_M("album_1", "My Phuket Vacation", "Went Phuket with friends");
        AlbumUni_M_M_M_M album2 = new AlbumUni_M_M_M_M("album_2", "My Shimla Vacation", "Went Shimla with friends");
        AlbumUni_M_M_M_M album3 = new AlbumUni_M_M_M_M("album_3", "My Zurik Vacation", "Went Zurik with friends");

        PhotoUni_M_M_M_M photo1 = new PhotoUni_M_M_M_M("photo_1", "One beach", "On beach with friends");
        PhotoUni_M_M_M_M photo2 = new PhotoUni_M_M_M_M("photo_2", "In Hotel", "Chilling out in room");
        PhotoUni_M_M_M_M photo3 = new PhotoUni_M_M_M_M("photo_3", "At Airport", "So tired");
        PhotoUni_M_M_M_M photo4 = new PhotoUni_M_M_M_M("photo_4", "In Space", "I am flying");

        album1.addPhoto(photo1);
        album1.addPhoto(photo2);
        album2.addPhoto(photo2);
        album2.addPhoto(photo3);
        album3.addPhoto(photo3);
        album3.addPhoto(photo4);

        p1.addAlbum(album1);
        p1.addAlbum(album2);

        p2.addAlbum(album2);
        p2.addAlbum(album3);

        photographers.add(p1);
        photographers.add(p2);

        return photographers;
    }

    @Override
    protected void createCassandraSchema() throws IOException, TException, InvalidRequestException,
            UnavailableException, TimedOutException, SchemaDisagreementException
    {
        /**
         * schema generation for cassandra.
         * */

        KsDef ksDef = null;

        CfDef pCfDef = new CfDef();
        pCfDef.name = "PHOTOGRAPHER";
        pCfDef.keyspace = "Pickr";
        pCfDef.setComparator_type("UTF8Type");
        pCfDef.setDefault_validation_class("UTF8Type");
        ColumnDef pColumnDef2 = new ColumnDef(ByteBuffer.wrap("PHOTOGRAPHER_NAME".getBytes()), "UTF8Type");
        pColumnDef2.index_type = IndexType.KEYS;
        pCfDef.addToColumn_metadata(pColumnDef2);

        CfDef aCfDef = new CfDef();
        aCfDef.name = "ALBUM";
        aCfDef.keyspace = "Pickr";
        aCfDef.setComparator_type("UTF8Type");
        aCfDef.setDefault_validation_class("UTF8Type");
        ColumnDef columnDef = new ColumnDef(ByteBuffer.wrap("ALBUM_NAME".getBytes()), "UTF8Type");
        columnDef.index_type = IndexType.KEYS;
        ColumnDef columnDef3 = new ColumnDef(ByteBuffer.wrap("ALBUM_DESC".getBytes()), "UTF8Type");
        columnDef3.index_type = IndexType.KEYS;
        aCfDef.addToColumn_metadata(columnDef);
        aCfDef.addToColumn_metadata(columnDef3);

        CfDef photoLinkCfDef = new CfDef();
        photoLinkCfDef.name = "PHOTO";
        photoLinkCfDef.keyspace = "Pickr";
        photoLinkCfDef.setComparator_type("UTF8Type");
        photoLinkCfDef.setDefault_validation_class("UTF8Type");
        ColumnDef columnDef1 = new ColumnDef(ByteBuffer.wrap("PHOTO_CAPTION".getBytes()), "UTF8Type");
        columnDef1.index_type = IndexType.KEYS;
        ColumnDef columnDef2 = new ColumnDef(ByteBuffer.wrap("PHOTO_DESC".getBytes()), "UTF8Type");
        columnDef2.index_type = IndexType.KEYS;
        photoLinkCfDef.addToColumn_metadata(columnDef1);
        photoLinkCfDef.addToColumn_metadata(columnDef2);

        CfDef cfDef = new CfDef();
        cfDef.name = "PHOTOGRAPHER_ALBUM";
        cfDef.keyspace = "Pickr";
        cfDef.setComparator_type("UTF8Type");
        cfDef.setDefault_validation_class("UTF8Type");
        ColumnDef columnDef4 = new ColumnDef(ByteBuffer.wrap("PHOTOGRAPHER_ID".getBytes()), "UTF8Type");
        columnDef1.index_type = IndexType.KEYS;
        ColumnDef columnDef6 = new ColumnDef(ByteBuffer.wrap("ALBUM_ID".getBytes()), "UTF8Type");
        columnDef2.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef4);
        cfDef.addToColumn_metadata(columnDef6);

        CfDef album_photo = new CfDef();
        album_photo.name = "ALBUM_PHOTO";
        album_photo.keyspace = "Pickr";
        album_photo.setComparator_type("UTF8Type");
        album_photo.setDefault_validation_class("UTF8Type");
        ColumnDef photo_id = new ColumnDef(ByteBuffer.wrap("PHOTO_ID".getBytes()), "UTF8Type");
        columnDef1.index_type = IndexType.KEYS;
        ColumnDef album_id = new ColumnDef(ByteBuffer.wrap("ALBUM_ID".getBytes()), "UTF8Type");
        columnDef2.index_type = IndexType.KEYS;
        album_photo.addToColumn_metadata(photo_id);
        album_photo.addToColumn_metadata(album_id);

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(pCfDef);
        cfDefs.add(aCfDef);
        cfDefs.add(cfDef);
        cfDefs.add(photoLinkCfDef);
        cfDefs.add(album_photo);
        try
        {
            ksDef = CassandraCli.client.describe_keyspace("Pickr");
            CassandraCli.client.set_keyspace("Pickr");
            if (!CassandraCli.columnFamilyExist("PHOTOGRAPHER", "Pickr")) {
                CassandraCli.client.system_add_column_family(pCfDef);
            } else {
                CassandraCli.truncateColumnFamily("Pickr", "PHOTOGRAPHER");
            }
            if (!CassandraCli.columnFamilyExist("ALBUM", "Pickr")) {
                CassandraCli.client.system_add_column_family(photoLinkCfDef);
            } else {
                CassandraCli.truncateColumnFamily("Pickr", "ALBUM");
            }
            if (!CassandraCli.columnFamilyExist("PHOTO", "Pickr")) {
                CassandraCli.client.system_add_column_family(aCfDef);
            } else {
                CassandraCli.truncateColumnFamily("Pickr", "PHOTO");
            }
            if (!CassandraCli.columnFamilyExist("ALBUM_PHOTO", "Pickr")) {
                CassandraCli.client.system_add_column_family(cfDef);
            } else {
                CassandraCli.truncateColumnFamily("Pickr", "ALBUM_PHOTO");
            }

        }
        catch (NotFoundException e)
        {
            addKeyspace(ksDef, cfDefs);
        }
        catch (InvalidRequestException e)
        {
            log.error(e.getMessage());
        }
        catch (TException e)
        {
            log.error(e.getMessage());
        }
    }

}
