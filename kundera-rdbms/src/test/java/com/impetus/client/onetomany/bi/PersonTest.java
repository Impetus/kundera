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
package com.impetus.client.onetomany.bi;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.Test;

/**
 * @author vivek.mishra
 * 
 */
public class PersonTest
{

    @Test
    public void testPersist()
    {
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("testHibernate,kcassandra");

        EntityManager em = emf.createEntityManager();
        em.persist(prepareObject());

    }

    /**
     * @return
     */
    private Object prepareObject()
    {
        OTMBNPerson person = new OTMBNPerson();
        person.setPersonId("b_p");
        person.setPersonName("bVivs");
        OTMBAddress address = new OTMBAddress();
        address.setAddressId("b_a");
        address.setStreet("bsadak");
        person.setAddress(address);
        return person;
    }

}
