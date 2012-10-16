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
package com.impetus.client.gis;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.gis.geometry.Point;

/**
 * Test case for GIS
 * 
 * @author amresh.singh
 */
public class MongoGISTest
{

    String persistenceUnit = "mongoTest";

    PersonGISDao dao;

    @Before
    public void setUp() throws Exception
    {
        dao = new PersonGISDao(persistenceUnit);
    }

    @Test
    public void executeTests() throws Exception
    {
        addPerson();
        findPerson();
        updatePerson();
        removePerson();
    }

    @After
    public void tearDown() throws Exception
    {
        dao.close();
    }

    private void addPerson()
    {
        Person person = new Person();
        person.setPersonId(1);
        person.setName("Amresh");
        person.setCurrentLocation(new Point(6.3, 2.8));

        Vehicle vehicleLocation = new Vehicle();
        vehicleLocation.setCurrentLocation(new Point(10.67, 16.59));
        vehicleLocation.setPreviousLocation(new Point(15.67, 21.59));

        person.setVehicleLocation(vehicleLocation);

        dao.createEntityManager();
        dao.addPerson(person);
        dao.closeEntityManager();
    }

    private void findPerson()
    {
        dao.createEntityManager();
        Person person = dao.findPerson(1);

        Assert.assertNotNull(person);
        Assert.assertEquals(1, person.getPersonId());
        Assert.assertEquals("Amresh", person.getName());

        Point currentLocation = person.getCurrentLocation();
        Assert.assertNotNull(currentLocation);
        Assert.assertEquals(6.3, currentLocation.getX());
        Assert.assertEquals(2.8, currentLocation.getY());

        Vehicle vehicleLocation = person.getVehicleLocation();
        Assert.assertNotNull(vehicleLocation);
        Assert.assertNotNull(vehicleLocation.getCurrentLocation());
        Assert.assertEquals(10.67, vehicleLocation.getCurrentLocation().getX());
        Assert.assertEquals(16.59, vehicleLocation.getCurrentLocation().getY());
        Assert.assertNotNull(vehicleLocation.getPreviousLocation());
        Assert.assertEquals(15.67, vehicleLocation.getPreviousLocation().getX());
        Assert.assertEquals(21.59, vehicleLocation.getPreviousLocation().getY());

        dao.closeEntityManager();
    }

    /**
     * 
     */
    private void updatePerson()
    {
        dao.createEntityManager();
        Person person = dao.findPerson(1);

        Assert.assertNotNull(person);
        Assert.assertEquals(1, person.getPersonId());
        Assert.assertEquals("Amresh", person.getName());

        person.setCurrentLocation(new Point(9.3, 5.8));

        Vehicle vehicle = person.getVehicleLocation();
        vehicle.setCurrentLocation(new Point(5.67, 11.59));

        person.setVehicleLocation(vehicle);

        dao.mergePerson(person);
        dao.closeEntityManager();
        dao.createEntityManager();

        person = dao.findPerson(1);

        Assert.assertNotNull(person);
        Assert.assertEquals(1, person.getPersonId());
        Assert.assertEquals("Amresh", person.getName());
        Point currentLocation = person.getCurrentLocation();
        Assert.assertNotNull(currentLocation);
        Assert.assertEquals(9.3, currentLocation.getX());
        Assert.assertEquals(5.8, currentLocation.getY());

        vehicle = person.getVehicleLocation();
        Assert.assertNotNull(vehicle);
        Assert.assertNotNull(vehicle.getCurrentLocation());
        Assert.assertEquals(5.67, vehicle.getCurrentLocation().getX());
        Assert.assertEquals(11.59, vehicle.getCurrentLocation().getY());
        Assert.assertNotNull(vehicle.getPreviousLocation());
        Assert.assertEquals(15.67, vehicle.getPreviousLocation().getX());
        Assert.assertEquals(21.59, vehicle.getPreviousLocation().getY());

        dao.closeEntityManager();

    }

    /**
     * 
     */
    private void removePerson()
    {
        dao.createEntityManager();
        Person person = dao.findPerson(1);

        Assert.assertNotNull(person);

        dao.removePerson(person);

        dao.closeEntityManager();
        dao.createEntityManager();

        person = dao.findPerson(1);

        Assert.assertNull(person);
        dao.closeEntityManager();
    }
}
