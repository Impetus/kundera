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
 * <Prove description of functionality provided by this Type>
 * 
 * @author amresh.singh
 */
public interface RESTClient
{

    void initialize(WebResource wr, String mediaType);

    String getApplicationToken(String persistenceUnit);

    String closeApplication(String applicationToken);

    String getSessionToken(String applicationToken);

    String closeSession(String sessionToken);

    String insertBook(String sessionToken, String book);

    String findBook(String sessionToken, String isbn);

    String updateBook(String sessionToken, String oldBook);

    void deleteBook(String sessionToken, String updatedBook, String isbn);

    String getAllBooks(String sessionToken);  
    
    String runJPAQuery(String sessionToken, String jpaQuery);
    
    String runNamedJPAQuery(String sessionToken, String entityClassName, String namedQuery, Map<String, Object> params);

    String getSchemaList(String persistenceUnit);

    String insertPerson(String sessionToken, String person);

    String findPerson(String sessionToken, String isbn);

    String updatePerson(String sessionToken, String oldPerson);

    String getAllPersons(String sessionToken);

    void deletePerson(String sessionToken, String updatedPerson, String isbn);

    String executeNamedQuery();
}
