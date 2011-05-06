/*
 * Copyright 2010 Impetus Infotech.
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
package com.impetus.kundera.junit;

import javax.persistence.EntityManager;

import com.impetus.kundera.loader.Configuration;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * @author animesh.kumar
 *l
 */
public class PersistenceUnitTest extends BaseTest {

    
    private EntityManager entityManager;
    private Configuration conf;

	public void setUp() throws Exception {
    	startCassandraServer();
    	conf = new Configuration();
    	entityManager = conf.getEntityManager("test-unit-1");
    }
    
    protected void tearDown() {
    	conf.destroy();
    	
    }
    
	public void testEntityManager() {
		try {
			assertNotNull(entityManager);
				assertNotNull(PropertyAccessorHelper.getObject(entityManager, entityManager.getClass().getDeclaredField("client")));
			} catch (SecurityException e) {
				e.printStackTrace();
			} catch (PropertyAccessException e) {
				e.printStackTrace();
			} catch (NoSuchFieldException e) {
				e.printStackTrace();
			}
	}
}
