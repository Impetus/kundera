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
package com.impetus.client.onetoone;

import org.junit.Test;

/**
 * The Class PersonTest.
 *
 * @author vivek.mishra
 */
public class PersonTest
{

    // @Test
    /**
     * Test persist.
     */
    public void testPersist()
    {

    }

    // @Test
    /**
     * Test shared by pk persist.
     */
    public void testSharedByPKPersist()
    {

    }

    /**
     * Test find by id.
     */
    @Test
    public void testFindById()
    {

    }

    /**
     * Prepare pk object.
     *
     * @return the object
     */
    private Object preparePKObject()
    {
        OTONSPerson person = new OTONSPerson();
        person.setPersonId("oop");
        person.setPersonName("PkVivs");
        OTOSAddress address = new OTOSAddress();
        address.setAddressId("ooa");
        address.setStreet("sadakPK");
        person.setAddress(address);
        return person;
    }

    /**
     * Prepare object.
     *
     * @return the object
     */
    private Object prepareObject()
    {
        OTONPerson person = new OTONPerson();
        person.setPersonId("1_p");
        person.setPersonName("VVivs");
        OTOAddress address = new OTOAddress();
        address.setAddressId("1_a");
        address.setStreet("sadak");
        person.setAddress(address);
        return person;
    }

}