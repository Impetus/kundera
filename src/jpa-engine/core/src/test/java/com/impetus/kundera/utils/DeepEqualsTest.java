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
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import junit.framework.TestCase;

import com.impetus.kundera.entity.album.AlbumUni_1_M_1_M;
import com.impetus.kundera.entity.photo.PhotoUni_1_M_1_M;
import com.impetus.kundera.entity.photographer.PhotographerUni_1_M_1_M;

/**
 * Test for DeepEquals (equals() and hashCode())
 * 
 * @author John DeRegnaucourt (jdereg@gmail.com) <br/>
 *         Copyright [2010] John DeRegnaucourt <br/>
 * <br/>
 *         Licensed under the Apache License, Version 2.0 (the "License"); you
 *         may not use this file except in compliance with the License. You may
 *         obtain a copy of the License at <br/>
 * <br/>
 *         http://www.apache.org/licenses/LICENSE-2.0 <br/>
 * <br/>
 *         Unless required by applicable law or agreed to in writing, software
 *         distributed under the License is distributed on an "AS IS" BASIS,
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *         implied. See the License for the specific language governing
 *         permissions and limitations under the License.
 */
public class DeepEqualsTest extends TestCase
{
    public DeepEqualsTest()
    {
    }

    private class Person
    {
        String first;

        String last;

        Pet pet;
    }

    private class Pet
    {
        Pet(String nm, String t)
        {
            name = new String(nm);
            type = new String(t);
        }

        String name;

        String type;
    }

    private class Pathelogical
    {
    }

    private class ComparablePet extends Pet implements Comparable
    {
        ComparablePet(String nm, String t)
        {
            super(nm, t);
        }

        public int compareTo(Object o)
        {
            if (o == null || !(o instanceof ComparablePet))
            {
                return 1;
            }
            ComparablePet that = (ComparablePet) o;
            if (name.compareTo(that.name) == 0)
            {
                return type.compareTo(that.type);
            }
            return name.compareTo(that.name);
        }

    }

    private class ArrayClass
    {
        String name;

        Object[] items;
    }

    private class Cycle
    {
        String value;

        Object next;
    }

    private class FixedHierarchy
    {
        String value;

        Object child1;

        Object child2;

        Object child3;
    }

    private class CollectionClass
    {
        String value;

        Collection items;
    }

    private class MapClass
    {
        String value;

        Map items;
    }

    private class SmartPet extends Pet
    {
        SmartPet(String nm, String t)
        {
            super(nm, t);
        }

        // Pathological equals!!! Intentionally wrong to prove that it is
        // called.
        public boolean equals(Object o)
        {
            if (o == null || !(o instanceof Pet))
            {
                return false;
            }

            Pet that = (Pet) o;
            boolean nameEquals;
            if (name == null || that.name == null)
            {
                nameEquals = name != that.name;
            }
            else
            {
                nameEquals = !name.equals(that.name);
            }

            if (!nameEquals)
            {
                return false;
            }
            boolean typeEquals;
            if (type == null || that.type == null)
            {
                typeEquals = type != that.type;
            }
            else
            {
                typeEquals = !type.equals(that.type);
            }
            return typeEquals;
        }

        public int hashCode()
        {
            int h1 = (name == null) ? 0 : name.hashCode();
            int h2 = (type == null) ? 0 : type.hashCode();
            return h1 + h2;
        }
    }

    public void testHashCodeAndEquals()
    {
        Person p1 = new Person();
        p1.first = new String("John");
        p1.last = new String("DeRegnaucourt");
        int a = DeepEquals.deepHashCode(p1);
        int b = "John".hashCode();
        int c = "DeRegnaucourt".hashCode();
        assertTrue(a != b && b != c);

        Pet pet1 = new Pet("Eddie", "dog");
        Pet pet2 = new Pet("Penny", "dog");
        p1.pet = pet1;

        Person p2 = new Person();
        p2.first = new String("John");
        p2.last = new String("DeRegnaucourt");

        p2.pet = pet2;

        assertTrue(p1.first != p2.first); // Ensure that the Strings are not ==

        assertTrue(!DeepEquals.deepEquals(p1, p2));
        assertTrue(!p1.equals(p2));

        p2.pet = pet1;
        assertTrue(DeepEquals.deepEquals(p1, p2));
        assertTrue(!p1.equals(p2)); // should be different because it would use
                                    // Object.equals() which is instance based
    }

    public void testCycleHandlingHashCode()
    {
        Cycle a = new Cycle();
        a.value = new String("foo");
        Cycle b = new Cycle();
        b.value = new String("bar");
        Cycle c = new Cycle();
        c.value = new String("baz");

        a.next = b;
        b.next = c;
        c.next = a;

        int ha = DeepEquals.deepHashCode(a);
        int hb = DeepEquals.deepHashCode(b);
        int hc = DeepEquals.deepHashCode(c);

        assertTrue(ha == hb && hb == hc);
    }

    public void testCycleHandlingEquals()
    {
        Cycle a1 = new Cycle();
        a1.value = new String("foo");
        Cycle b1 = new Cycle();
        b1.value = new String("bar");
        Cycle c1 = new Cycle();
        c1.value = new String("baz");

        a1.next = b1;
        b1.next = c1;
        c1.next = a1;

        Cycle a2 = new Cycle();
        a2.value = new String("foo");
        Cycle b2 = new Cycle();
        b2.value = new String("bar");
        Cycle c2 = new Cycle();
        c2.value = new String("baz");

        a2.next = b2;
        b2.next = c2;
        c2.next = a2;

        assertTrue(DeepEquals.deepEquals(a1, a2));
        assertTrue(DeepEquals.deepEquals(b1, b2));
        assertTrue(DeepEquals.deepEquals(c1, c2));
        assertFalse(DeepEquals.deepEquals(a1, b2));
        assertFalse(DeepEquals.deepEquals(b1, c2));
        assertFalse(DeepEquals.deepEquals(c1, a2));
    }

    public void testHierarchyCycleEquals()
    {
        FixedHierarchy h1 = new FixedHierarchy();
        h1.value = new String("root");
        FixedHierarchy c1 = new FixedHierarchy();
        c1.value = new String("child1");
        FixedHierarchy c2 = new FixedHierarchy();
        c2.value = new String("child2");

        h1.child1 = c1;
        h1.child2 = c2;
        h1.child3 = c1;

        FixedHierarchy h2 = new FixedHierarchy();
        h2.value = new String("root");
        FixedHierarchy k1 = new FixedHierarchy();
        k1.value = new String("child1");
        FixedHierarchy k2 = new FixedHierarchy();
        k2.value = new String("child2");

        h2.child1 = k1;
        h2.child2 = k2;
        h2.child3 = k1;

        assertTrue(DeepEquals.deepEquals(h1, h2));
    }

    public void testDeepEquals()
    {
        SmartPet smartPet1 = new SmartPet("Fido", "Terrier");
        SmartPet smartPet2 = new SmartPet("Fido", "Terrier");

        assertFalse(DeepEquals.deepEquals(smartPet1, smartPet2)); // Only way to
                                                                  // get false
                                                                  // is if it
                                                                  // calls
                                                                  // .equals()

        ArrayClass ac1 = new ArrayClass();
        ac1.name = new String("Object Array");
        ac1.items = new Object[] { new String("Hello"), 16, 16L, null, 'c', new Boolean(true), 0.04,
                new Object[] { "a", 2, 'c' }, new String[] { "larry", "curly", new String("mo") } };
        ArrayClass ac2 = new ArrayClass();
        ac2.name = new String("Object Array");
        ac2.items = new Object[] { new String("Hello"), 16, 16L, null, 'c', Boolean.TRUE, new Double(0.04),
                new Object[] { "a", 2, 'c' }, new String[] { "larry", new String("curly"), "mo" } };

        assertTrue(DeepEquals.deepEquals(ac1, ac2));
    }

    public void testBasicEquals()
    {
        String one = new String("One");
        String two = new String("Two");
        String a = new String("One");

        assertFalse(DeepEquals.deepEquals(one, two));
        assertTrue(DeepEquals.deepEquals(one, a));

        Double x = 1.04;
        Double y = 1.039999;
        Double z = 1.04;

        assertFalse(DeepEquals.deepEquals(x, y));
        assertTrue(DeepEquals.deepEquals(x, z));
    }

    public void testBasicHashCode()
    {
        String one = new String("One");
        assertTrue(DeepEquals.deepHashCode(one) == one.hashCode());

        Double pi = 3.14159;
        assertTrue(DeepEquals.deepHashCode(pi) == pi.hashCode());

        Calendar c = Calendar.getInstance();
        assertTrue(DeepEquals.deepHashCode(c) == c.hashCode());

        Date date = new Date();
        assertTrue(DeepEquals.deepHashCode(date) == date.hashCode());
    }

    public void testCollection()
    {
        Pet p1 = new Pet("Eddie", "Terrier");
        Pet p2 = new Pet("Eddie", "Terrier");
        Pet p3 = new Pet("Penny", "Chihuahua");
        Pet p4 = new Pet("Penny", "Chihuahua");

        CollectionClass c1 = new CollectionClass();
        c1.value = new String("Animals");
        c1.items = new ArrayList();
        c1.items.add(p1);
        c1.items.add(p3);

        CollectionClass c2 = new CollectionClass();
        c2.value = new String("Animals");
        c2.items = new ArrayList();
        c2.items.add(p2);
        c2.items.add(p4);

        assertTrue(DeepEquals.deepHashCode(c1) == DeepEquals.deepHashCode(c2));
        assertTrue(DeepEquals.deepEquals(c1, c2));
        c1.items.add(new Pet("Tinker", "Doberman"));
        c2.items.add(new Pet("Tinker", "Terrier"));
        assertFalse(DeepEquals.deepHashCode(c1) == DeepEquals.deepHashCode(c2));
        assertFalse(DeepEquals.deepEquals(c1, c2));

        // Try HashSet() [not ordered]
        c1.items = new HashSet();
        c2.items = new HashSet();

        c1.items.add(p1);
        c2.items.add(p2);

        c1.items.add(p3);
        c2.items.add(p4);

        assertTrue(DeepEquals.deepHashCode(c1) == DeepEquals.deepHashCode(c2));
        assertTrue(DeepEquals.deepEquals(c1, c2));
        c1.items.add(new Pet("Tinker", "Doberman"));
        c2.items.add(new Pet("Tinker", "Terrier"));
        assertFalse(DeepEquals.deepHashCode(c1) == DeepEquals.deepHashCode(c2));
        assertFalse(DeepEquals.deepEquals(c1, c2));

        // Try TreeSet() [ordered]
        p1 = new ComparablePet("Eddie", "Terrier");
        p2 = new ComparablePet("Eddie", "Terrier");
        p3 = new ComparablePet("Penny", "Chihuahua");
        p4 = new ComparablePet("Penny", "Chihuahua");

        c1.items = new TreeSet();
        c2.items = new TreeSet();

        c1.items.add(p1);
        c2.items.add(p2);

        c1.items.add(p3);
        c2.items.add(p4);

        assertTrue(DeepEquals.deepHashCode(c1) == DeepEquals.deepHashCode(c2));
        assertTrue(DeepEquals.deepEquals(c1, c2));
        c1.items.add(new ComparablePet("Tinker", "Doberman"));
        c2.items.add(new ComparablePet("Tinker", "Terrier"));
        assertFalse(DeepEquals.deepHashCode(c1) == DeepEquals.deepHashCode(c2));
        assertFalse(DeepEquals.deepEquals(c1, c2));
    }

    public void testMap()
    {
        Pet p1 = new Pet("Eddie", "Terrier");
        Pet p2 = new Pet("Eddie", "Terrier");
        Pet p3 = new Pet("Penny", "Chihuahua");
        Pet p4 = new Pet("Penny", "Chihuahua");

        MapClass m1 = new MapClass();
        m1.value = new String("Hat");
        m1.items = new HashMap();

        MapClass m2 = new MapClass();
        m2.value = new String("Hat");
        m2.items = new HashMap();

        m1.items.put(p1, new String("Cool dog"));
        m1.items.put(p3, new Long(16));
        m2.items.put(p2, new String("Cool dog"));
        m2.items.put(p4, new Long(16));

        assertTrue(DeepEquals.deepHashCode(m1) == DeepEquals.deepHashCode(m2));
        assertTrue(DeepEquals.deepEquals(m1, m2));
        m1.items.put(new Pet("Tinker", "Doberman"), "mean dog");
        m1.items.put(new Pet("Tinker", "Terrier"), "mean dog");
        assertFalse(DeepEquals.deepHashCode(m1) == DeepEquals.deepHashCode(m2));
        assertFalse(DeepEquals.deepEquals(m1, m2));

        m1.items = new HashMap();
        m2.items = new HashMap();

        long now = System.currentTimeMillis();
        Date d1 = new Date(now);
        Date d2 = new Date(now);
        m1.items.put(p1, d1);
        m1.items.put(p3, "2nd");
        m2.items.put(p2, d2);
        m2.items.put(p4, "2nd");

        assertTrue(DeepEquals.deepHashCode(m1) == DeepEquals.deepHashCode(m2));
        assertTrue(DeepEquals.deepEquals(m1, m2));

        Map map1 = new HashMap();
        Map map2 = new HashMap();

        map1.put("Alpha", "One");
        map1.put("Bravo", "Two");

        map2.put("Alpha", "Two");
        map2.put("Bravo", "One");

        assertTrue(DeepEquals.deepHashCode(map1) == DeepEquals.deepHashCode(map2));
        assertFalse(DeepEquals.deepEquals(map1, map2));
        m1.items.put(new Pet("Tinker", "Doberman"), "mean dog");
        m1.items.put(new Pet("Tinker", "Terrier"), "mean dog");
        assertFalse(DeepEquals.deepHashCode(m1) == DeepEquals.deepHashCode(m2));
        assertFalse(DeepEquals.deepEquals(m1, m2));

        m1.items = new TreeMap();
        m2.items = new TreeMap();

        p1 = new ComparablePet("Eddie", "Terrier");
        p2 = new ComparablePet("Eddie", "Terrier");
        p3 = new ComparablePet("Penny", "Chihuahua");
        p4 = new ComparablePet("Penny", "Chihuahua");

        now = System.currentTimeMillis();
        d1 = new Date(now);
        d2 = new Date(now);
        m1.items.put(p1, d1);
        m1.items.put(p3, "2nd");
        m2.items.put(p2, d2);
        m2.items.put(p4, "2nd");

        assertTrue(DeepEquals.deepHashCode(m1) == DeepEquals.deepHashCode(m2));
        assertTrue(DeepEquals.deepEquals(m1, m2));
        m1.items.put(new ComparablePet("Tinker", "Doberman"), "mean dog");
        m1.items.put(new ComparablePet("Tinker", "Terrier"), "mean dog");
        assertFalse(DeepEquals.deepHashCode(m1) == DeepEquals.deepHashCode(m2));
        assertFalse(DeepEquals.deepEquals(m1, m2));

        map1 = new TreeMap();
        map2 = new TreeMap();

        map1.put("Alpha", "One");
        map1.put("Bravo", "Two");

        map2.put("Alpha", "Two");
        map2.put("Bravo", "One");

        assertTrue(DeepEquals.deepHashCode(map1) == DeepEquals.deepHashCode(map2));
        assertFalse(DeepEquals.deepEquals(map1, map2));
    }

    public void testPathelogical()
    {
        Pathelogical a = new Pathelogical();
        Pathelogical b = new Pathelogical();

        assertTrue(DeepEquals.deepEquals(a, b));
    }

    public void testPhotographer()
    {
        // Object 1
        PhotographerUni_1_M_1_M a1 = new PhotographerUni_1_M_1_M();
        a1.setPhotographerId(1);
        AlbumUni_1_M_1_M b11 = new AlbumUni_1_M_1_M();
        b11.setAlbumId("b1");
        AlbumUni_1_M_1_M b12 = new AlbumUni_1_M_1_M();
        b12.setAlbumId("b2");

        PhotoUni_1_M_1_M c11 = new PhotoUni_1_M_1_M();
        c11.setPhotoId("c1");
        PhotoUni_1_M_1_M c12 = new PhotoUni_1_M_1_M();
        c12.setPhotoId("c2");
        PhotoUni_1_M_1_M c13 = new PhotoUni_1_M_1_M();
        c13.setPhotoId("c3");
        PhotoUni_1_M_1_M c14 = new PhotoUni_1_M_1_M();
        c14.setPhotoId("c4");

        b11.addPhoto(c11);
        b11.addPhoto(c12);
        b12.addPhoto(c13);
        b12.addPhoto(c14);
        a1.addAlbum(b11);
        a1.addAlbum(b12);

        // Object2
        PhotographerUni_1_M_1_M a2 = new PhotographerUni_1_M_1_M();
        a2.setPhotographerId(1);
        AlbumUni_1_M_1_M b21 = new AlbumUni_1_M_1_M();
        b21.setAlbumId("b1");
        AlbumUni_1_M_1_M b22 = new AlbumUni_1_M_1_M();
        b22.setAlbumId("b2");

        PhotoUni_1_M_1_M c21 = new PhotoUni_1_M_1_M();
        c21.setPhotoId("c1");
        PhotoUni_1_M_1_M c22 = new PhotoUni_1_M_1_M();
        c22.setPhotoId("c2");
        PhotoUni_1_M_1_M c23 = new PhotoUni_1_M_1_M();
        c23.setPhotoId("c3");
        PhotoUni_1_M_1_M c24 = new PhotoUni_1_M_1_M();
        c24.setPhotoId("c4");

        b21.addPhoto(c21);
        b21.addPhoto(c22);
        b22.addPhoto(c23);
        b22.addPhoto(c24);
        a2.addAlbum(b21);
        a2.addAlbum(b22);

        // Equality test
        assertTrue(DeepEquals.deepEquals(a1, a2));

        // Inequality test
        a2.setPhotographerId(2);
        assertFalse(DeepEquals.deepEquals(a1, a2));
        a2.setPhotographerId(1);

        // Case 1: All same
        assertTrue(DeepEquals.deepEquals(a1, a2));
        assertTrue(DeepEquals.deepEquals(b12, b22));
        assertTrue(DeepEquals.deepEquals(c14, c24));

        // Case 2: Change Photo object
        String originalPhotoCaption = c24.getPhotoCaption();
        c24.setPhotoCaption("AAAAAAAAAAAAA");

        assertTrue(DeepEquals.deepEquals(a1, a2));
        assertTrue(DeepEquals.deepEquals(b12, b22));
        assertFalse(DeepEquals.deepEquals(c14, c24));

        c24.setPhotoCaption(originalPhotoCaption);

        // Case 3: Change Album object
        String originalAlbumDiscription = b22.getAlbumDescription();
        b22.setAlbumDescription("Second Album of Second Photographer");

        assertTrue(DeepEquals.deepEquals(a1, a2));
        assertFalse(DeepEquals.deepEquals(b12, b22));
        assertTrue(DeepEquals.deepEquals(c14, c24));

        b22.setAlbumDescription(originalAlbumDiscription);

        // Case 4: Change album and photo object
        c24.setPhotoCaption("AAAAAAAAAAAAA");
        b22.setAlbumDescription("Second Album of Second Photographer");

        assertTrue(DeepEquals.deepEquals(a1, a2));
        assertFalse(DeepEquals.deepEquals(b12, b22));
        assertFalse(DeepEquals.deepEquals(c14, c24));

        b22.setAlbumDescription(originalAlbumDiscription);
        c24.setPhotoCaption(originalPhotoCaption);

        // Case 5: Change Photographer object
        String originalPhotographerName = a2.getPhotographerName();
        a2.setPhotographerName("Kuldeep");

        assertFalse(DeepEquals.deepEquals(a1, a2));
        assertTrue(DeepEquals.deepEquals(b12, b22));
        assertTrue(DeepEquals.deepEquals(c14, c24));

        a2.setPhotographerName(originalPhotographerName);

        // Case 6: Change Photographer and photo object
        c24.setPhotoCaption("AAAAAAAAAAAAA");
        a2.setPhotographerName("Kuldeep");

        assertFalse(DeepEquals.deepEquals(a1, a2));
        assertTrue(DeepEquals.deepEquals(b12, b22));
        assertFalse(DeepEquals.deepEquals(c14, c24));

        a2.setPhotographerName(originalPhotographerName);
        c24.setPhotoCaption(originalPhotoCaption);

        // Case 7: Change Photographer and album object
        b22.setAlbumDescription("Second Album of Second Photographer");
        a2.setPhotographerName("Kuldeep");

        assertFalse(DeepEquals.deepEquals(a1, a2));
        assertFalse(DeepEquals.deepEquals(b12, b22));
        assertTrue(DeepEquals.deepEquals(c14, c24));

        a2.setPhotographerName(originalPhotographerName);
        b22.setAlbumDescription(originalAlbumDiscription);

        // Case 8: Change Photographer, album and photo object
        c24.setPhotoCaption("AAAAAAAAAAAAA");
        b22.setAlbumDescription("Second Album of Second Photographer");
        a2.setPhotographerName("Kuldeep");

        assertFalse(DeepEquals.deepEquals(a1, a2));
        assertFalse(DeepEquals.deepEquals(b12, b22));
        assertFalse(DeepEquals.deepEquals(c14, c24));

        a2.setPhotographerName(originalPhotographerName);
        b22.setAlbumDescription(originalAlbumDiscription);
        c24.setPhotoCaption(originalPhotoCaption);
    }
}