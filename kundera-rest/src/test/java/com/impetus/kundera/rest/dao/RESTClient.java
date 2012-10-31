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
package com.impetus.kundera.rest.dao;

import java.util.Map;

import com.sun.jersey.api.client.WebResource;

/**
 * REST Client interface 
 * @author amresh.singh
 */
public interface RESTClient
{
    
    /** Handshake and metadata methods */
    void initialize(WebResource wr, String mediaType);

    String getApplicationToken(String persistenceUnit);

    String closeApplication(String applicationToken);

    String getSessionToken(String applicationToken);

    String closeSession(String sessionToken);

    String getSchemaList(String persistenceUnit);
    
    /** Operations on Book entity */
    String insertEntity(String sessionToken, String entityStr, String entityClassName);

    String findEntity(String sessionToken, String pk, String entityClassName);

    String updateEntity(String sessionToken, String newEntityStr, String entityClassName);

    void deleteEntity(String sessionToken, String updatedBook, String pk, String entityClassName);

    String getAllEntities(String sessionToken, String entityClassName);  
    
    String runJPAQuery(String sessionToken, String jpaQuery, Map<String, Object> params);
    
    String runNamedJPAQuery(String sessionToken, String entityClassName, String namedQuery, Map<String, Object> params);
    
    String runNativeQuery(String sessionToken, String entityClassName, String nativeQuery, Map<String, Object> params);
    
    String runNamedNativeQuery(String sessionToken, String entityClassName, String namedNativeQuery, Map<String, Object> params);

    /** Operations on Person entity*/

    String insertPerson(String sessionToken, String person);

    String findPerson(String sessionToken, String isbn);

    String updatePerson(String sessionToken, String oldPerson);

    String getAllPersons(String sessionToken);

    void deletePerson(String sessionToken, String updatedPerson, String isbn);
    
}
