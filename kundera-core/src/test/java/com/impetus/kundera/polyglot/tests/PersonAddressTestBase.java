/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.polyglot.tests;

import com.impetus.kundera.client.DummyDatabase;
import com.impetus.kundera.polyglot.dao.PersonAddressDaoImpl;

/**
 * @author amresh.singh
 *
 */
public abstract class PersonAddressTestBase
{  
    PersonAddressDaoImpl dao;
    String pu = "patest";
    
    protected void init()
    {
        dao = new PersonAddressDaoImpl(pu);
    }
    
    protected void close()
    {
        dao.closeEntityManagerFactory();
        dao = null;
        DummyDatabase.INSTANCE.dropDatabase();
    }
    
    /** Insert person with address */
    protected abstract void insert();

    /** Find person by ID */
    protected abstract void find();

    /** Find person by ID using query */
    protected abstract void findPersonByIdColumn();

    /** Find person by name using query */
    protected abstract void findPersonByName();

    /** Find Address by ID using query */
    protected abstract void findAddressByIdColumn();

    /** Find Address by street using query */
    protected abstract void findAddressByStreet();

    /** Update Person */
    protected abstract void update();

    /** Remove Person */
    protected abstract void remove();
    
    protected void executeAllTests()
    {
      insert();
      find();
      findPersonByIdColumn();
      findPersonByName();
      findAddressByIdColumn();
      //findAddressByStreet();
      update();
      remove();
    }

}
