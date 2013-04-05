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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.tests.cli.CassandraCli;
import com.impetus.kundera.tests.crossdatastore.pickr.entities.album.AlbumUni_1_1_1_M;
import com.impetus.kundera.tests.crossdatastore.pickr.entities.photo.PhotoUni_1_1_1_M;
import com.impetus.kundera.tests.crossdatastore.pickr.entities.photographer.PhotographerUni_1_1_1_M;

/**
 * @author amresh.singh
 * 
 */
public class PickrTestUni_1_1_1_M extends PickrBaseTest
{
    private static final Log log = LogFactory.getLog(PickrTestUni_1_1_1_M.class);

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

    /**
     * Test.
     */
    @Test
    public void test()
    {
        executeTests();

    }

    @Override
    public void addPhotographer()
    {
        PhotographerUni_1_1_1_M p = populatePhotographer();
        pickr.addPhotographer(p);
    }

    @Override
    protected void getPhotographer()
    {
        PhotographerUni_1_1_1_M p = (PhotographerUni_1_1_1_M) pickr.getPhotographer(PhotographerUni_1_1_1_M.class,
                photographerId);
        assertPhotographer(p);

    }

    @Override
    protected void updatePhotographer()
    {
        PhotographerUni_1_1_1_M p = (PhotographerUni_1_1_1_M) pickr.getPhotographer(PhotographerUni_1_1_1_M.class,
                photographerId);
        assertPhotographer(p);
        p.setPhotographerName("Vivek");

        pickr.mergePhotographer(p);

        PhotographerUni_1_1_1_M p2 = (PhotographerUni_1_1_1_M) pickr.getPhotographer(PhotographerUni_1_1_1_M.class,
                photographerId);
        assertModifiedPhotographer(p2);
    }

    @Override
    protected void getAllPhotographers()
    {
        List<Object> ps = pickr.getAllPhotographers(PhotographerUni_1_1_1_M.class.getSimpleName());
        PhotographerUni_1_1_1_M p = (PhotographerUni_1_1_1_M) ps.get(0);

        assertModifiedPhotographer(p);

    }

    @Override
    protected void deletePhotographer()
    {
        PhotographerUni_1_1_1_M p = (PhotographerUni_1_1_1_M) pickr.getPhotographer(PhotographerUni_1_1_1_M.class,
                photographerId);
        assertModifiedPhotographer(p);
        pickr.deletePhotographer(p);
        PhotographerUni_1_1_1_M p2 = (PhotographerUni_1_1_1_M) pickr.getPhotographer(PhotographerUni_1_1_1_M.class,
                photographerId);
        Assert.assertNull(p2);

    }

    private void assertPhotographer(PhotographerUni_1_1_1_M p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals(1, p.getPhotographerId());
        Assert.assertEquals("Amresh", p.getPhotographerName());

        Assert.assertNotNull(p.getAlbum());
        AlbumUni_1_1_1_M album = p.getAlbum();
        Assert.assertNotNull(album);
        Assert.assertTrue(album.getAlbumId().equals("album_1"));
        Assert.assertEquals("My Phuket Vacation", album.getAlbumName());
        Assert.assertEquals("Went Phuket with friends", album.getAlbumDescription());

        List<PhotoUni_1_1_1_M> albumPhotos = album.getPhotos();
        Assert.assertNotNull(albumPhotos);
        Assert.assertFalse(albumPhotos.isEmpty());
        Assert.assertEquals(3, albumPhotos.size());

        PhotoUni_1_1_1_M photo1 = albumPhotos.get(0);
        PhotoUni_1_1_1_M photo2 = albumPhotos.get(0);
        PhotoUni_1_1_1_M photo3 = albumPhotos.get(0);

        Assert.assertNotNull(photo1);
        Assert.assertTrue(photo1.getPhotoId().startsWith("photo_"));
        Assert.assertNotNull(photo2);
        Assert.assertTrue(photo2.getPhotoId().startsWith("photo_"));
        Assert.assertNotNull(photo3);
        Assert.assertTrue(photo3.getPhotoId().startsWith("photo_"));

    }

    private void assertModifiedPhotographer(PhotographerUni_1_1_1_M p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals(1, p.getPhotographerId());
        Assert.assertEquals("Vivek", p.getPhotographerName());

        Assert.assertNotNull(p.getAlbum());
        AlbumUni_1_1_1_M album = p.getAlbum();
        Assert.assertNotNull(album);
        Assert.assertTrue(album.getAlbumId().equals("album_1"));
        Assert.assertEquals("My Phuket Vacation", album.getAlbumName());
        Assert.assertEquals("Went Phuket with friends", album.getAlbumDescription());

        List<PhotoUni_1_1_1_M> albumPhotos = album.getPhotos();
        Assert.assertNotNull(albumPhotos);
        Assert.assertFalse(albumPhotos.isEmpty());
        Assert.assertEquals(3, albumPhotos.size());

        PhotoUni_1_1_1_M photo1 = albumPhotos.get(0);
        PhotoUni_1_1_1_M photo2 = albumPhotos.get(0);
        PhotoUni_1_1_1_M photo3 = albumPhotos.get(0);

        Assert.assertNotNull(photo1);
        Assert.assertTrue(photo1.getPhotoId().startsWith("photo_"));
        Assert.assertNotNull(photo2);
        Assert.assertTrue(photo2.getPhotoId().startsWith("photo_"));
        Assert.assertNotNull(photo3);
        Assert.assertTrue(photo3.getPhotoId().startsWith("photo_"));

    }

    private PhotographerUni_1_1_1_M populatePhotographer()
    {
        PhotographerUni_1_1_1_M p = new PhotographerUni_1_1_1_M();
        p.setPhotographerId(photographerId);
        p.setPhotographerName("Amresh");

        AlbumUni_1_1_1_M album = new AlbumUni_1_1_1_M("album_1", "My Phuket Vacation", "Went Phuket with friends");

        album.addPhoto(new PhotoUni_1_1_1_M("photo_1", "One beach", "On beach with friends"));
        album.addPhoto(new PhotoUni_1_1_1_M("photo_2", "In Hotel", "Chilling out in room"));
        album.addPhoto(new PhotoUni_1_1_1_M("photo_3", "At Airport", "So tired"));

        p.setAlbum(album);

        return p;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.tests.crossdatastore.pickr.PickrBaseTest#startServer
     * ()
     */
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
        ColumnDef pColumnDef1 = new ColumnDef(ByteBuffer.wrap("ALBUM_ID".getBytes()), "UTF8Type");
        pColumnDef1.index_type = IndexType.KEYS;
        ColumnDef pColumnDef2 = new ColumnDef(ByteBuffer.wrap("PHOTOGRAPHER_NAME".getBytes()), "UTF8Type");
        pColumnDef2.index_type = IndexType.KEYS;
        pCfDef.addToColumn_metadata(pColumnDef1);
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
        ColumnDef columnDef4 = new ColumnDef(ByteBuffer.wrap("ALBUM_ID".getBytes()), "UTF8Type");
        columnDef4.index_type = IndexType.KEYS;
        photoLinkCfDef.addToColumn_metadata(columnDef1);
        photoLinkCfDef.addToColumn_metadata(columnDef2);
        photoLinkCfDef.addToColumn_metadata(columnDef4);

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(pCfDef);
        cfDefs.add(aCfDef);
        cfDefs.add(photoLinkCfDef);
        try
        {
            ksDef = CassandraCli.client.describe_keyspace("Pickr");
            CassandraCli.client.set_keyspace("Pickr");
            List<CfDef> cfDefn = ksDef.getCf_defs();

            for (CfDef cfDef1 : cfDefn)
            {

                if (cfDef1.getName().equalsIgnoreCase("PHOTOGRAPHER"))
                {
                    CassandraCli.client.system_drop_column_family("PHOTOGRAPHER");
                }
                if (cfDef1.getName().equalsIgnoreCase("ALBUM"))
                {
                    CassandraCli.client.system_drop_column_family("ALBUM");
                }
                if (cfDef1.getName().equalsIgnoreCase("PHOTO"))
                {
                    CassandraCli.client.system_drop_column_family("PHOTO");
                }
            }
            CassandraCli.client.system_add_column_family(pCfDef);
            CassandraCli.client.system_add_column_family(photoLinkCfDef);
            CassandraCli.client.system_add_column_family(aCfDef);
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

        /**
         * schema generation for cassandra.
         * */

        // HBaseCli.createTable("PHOTOGRAPHER");
        // HBaseCli.addColumnFamily("PHOTOGRAPHER", "ALBUM_ID");
        // HBaseCli.addColumnFamily("PHOTOGRAPHER", "PHOTOGRAPHER_NAME");
        //
        // HBaseCli.createTable("PHOTO");
        // HBaseCli.addColumnFamily("PHOTO", "PHOTO_CAPTION");
        // HBaseCli.addColumnFamily("PHOTO", "PHOTO_DESC");
        // HBaseCli.addColumnFamily("PHOTO", "ALBUM_ID");
        //
        // HBaseCli.createTable("ALBUM");
        // HBaseCli.addColumnFamily("ALBUM", "ALBUM_NAME");
        // HBaseCli.addColumnFamily("ALBUM", "ALBUM_DESC");
    }
   

}