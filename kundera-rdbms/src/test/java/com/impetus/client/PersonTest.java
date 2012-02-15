/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
package com.impetus.client;

import org.junit.Test;

/**
 * The Class PersonTest.
 * 
 * @author vivek.mishra
 */
public class PersonTest
{

    /**
     * Test persist.
     */
    @Test
    public void testPersist()
    {

    }

    /**
     * Prepare object.
     * 
     * @return the object
     */
    private Object prepareObject()
    {
        NPerson person = new NPerson();
        person.setPersonId("2_p");
        person.setPersonName("VVivs");
        Address address = new Address();
        address.setAddressId("2_a");
        address.setStreet("sadak");

        // Set<Address> addresses = new HashSet<Address>(1);
        // addresses.add(address);
        // person.setAddresses(addresses);
        person.setAddress(address);
        return person;
    }

}