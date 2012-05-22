/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.utils;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.configure.Configurator;
import com.impetus.kundera.entity.PersonalDetail;
import com.impetus.kundera.entity.Tweet;
import com.impetus.kundera.entity.album.AlbumUni_1_M_1_M;
import com.impetus.kundera.entity.photo.PhotoUni_1_M_1_M;
import com.impetus.kundera.entity.photographer.PhotographerUni_1_M_1_M;

/**
 * Test case for {@link ObjectUtils}
 * 
 * @author amresh.singh
 */
public class ObjectUtilsTest
{
    
    Configurator configurator = new Configurator("kunderatest");

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        configurator.configure();
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
        PhotographerUni_1_M_1_M a1 = new PhotographerUni_1_M_1_M();
        a1.setPhotographerId(1);
        a1.setPhotographerName("Amresh");
        
        a1.setPersonalDetail(new PersonalDetail("Xamry", "password1", "Single"));
        
        a1.addTweet(new Tweet("My First Tweet", "Web"));
        a1.addTweet(new Tweet("My Second Tweet", "Android"));
        a1.addTweet(new Tweet("My Third Tweet", "iPad"));        
        
        
        AlbumUni_1_M_1_M b11 = new AlbumUni_1_M_1_M("b1", "Album 1", "This is album 1");        
        AlbumUni_1_M_1_M b12 = new AlbumUni_1_M_1_M("b2", "Album 2", "This is album 2");        

        PhotoUni_1_M_1_M c11 = new PhotoUni_1_M_1_M("c1", "Photo 1", "This is Photo 1");        
        PhotoUni_1_M_1_M c12 = new PhotoUni_1_M_1_M("c2", "Photo 2", "This is Photo 2");        
        PhotoUni_1_M_1_M c13 = new PhotoUni_1_M_1_M("c3", "Photo 3", "This is Photo 3");        
        PhotoUni_1_M_1_M c14 = new PhotoUni_1_M_1_M("c4", "Photo 4", "This is Photo 4");        

        b11.addPhoto(c11);
        b11.addPhoto(c12);
        b12.addPhoto(c13);
        b12.addPhoto(c14);
        a1.addAlbum(b11);
        a1.addAlbum(b12);

        
        long t1 = System.currentTimeMillis();
        PhotographerUni_1_M_1_M a2 = (PhotographerUni_1_M_1_M) ObjectUtils.deepCopyUsingMetadata(a1);
        long t2 = System.currentTimeMillis();
        
        System.out.println("Time taken by KUndera:" + (t2 - t1));
        
        long t3 = System.currentTimeMillis();
        PhotographerUni_1_M_1_M a3 = (PhotographerUni_1_M_1_M) ObjectUtils.deepCopy(a1);
        long t4 = System.currentTimeMillis();
        
        System.out.println("Time taken by KUndera:" + (t4 - t3));
        
        
        Assert.assertFalse(a1 == a2);
        Assert.assertTrue(DeepEquals.deepEquals(a1, a2));
    }

}
