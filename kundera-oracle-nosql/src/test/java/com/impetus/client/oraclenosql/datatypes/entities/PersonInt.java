/**
 * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.oraclenosql.datatypes.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * Entity class with Primary key data type as int primitive
 * @author amresh.singh
 */
@Entity
@Table(name = "PERSON_INT", schema = "KunderaTests@twikvstore")
public class PersonInt
{

        @Id
        @Column(name = "PERSON_ID")
        private int personId;
       
        @Column(name = "PERSON_NAME")
        private String personName;
        

        public PersonInt()
        {
        }

        public PersonInt(int personId, String personName)
        {
            this.personId = personId;
            this.personName = personName;
        }

        /**
         * @return the personId
         */
        public int getPersonId()
        {
            return personId;
        }

        /**
         * @param personId the personId to set
         */
        public void setPersonId(int personId)
        {
            this.personId = personId;
        }

        /**
         * @return the personName
         */
        public String getPersonName()
        {
            return personName;
        }

        /**
         * @param personName the personName to set
         */
        public void setPersonName(String personName)
        {
            this.personName = personName;
        }
        
        
    
    

}
