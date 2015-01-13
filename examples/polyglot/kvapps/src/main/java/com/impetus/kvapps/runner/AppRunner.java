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
package com.impetus.kvapps.runner;


import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kvapps.entities.User;

/**
 * @author impetus
 * 
 * Sample Application runner. Application demonstrates:
 * <li> poly glot persistence, where user information {@link User} is getting persisted into mysql and corresponding tweets into Cassandra.</li>
 * <li> Fetch user(alongwith tweets) in polyglot way </li>
 * <li> Fetch tweets using native query support </li> 
 * <li> Since {kundera.ddl.auto.prepare} is set to "create"(Please refer src/main/resources/persistence.xml). 
 * hence schema will be automatically managed by Kundera itself.</li>
 * 
 * Also, application takes path to excel data file as an input arguments.
 */
public class AppRunner
{

    /** the log used by this class. */
    private static Log logger = LogFactory.getLog(AppRunner.class);

    /**
     * main runner method
     * @param args takes excel data file as input argument args[0].
     */
    public static void main(String[] args)
    {
    	
    	// Override CQL version while instantiating entity manager factory.

        EntityManagerFactory emf = Persistence.createEntityManagerFactory("twissandra,twingo,twirdbms");
        EntityManager em = emf.createEntityManager();

        try
        {
        	
        //populate user set from excel sheet.
        Set<User> users = UserBroker.brokeUserList(args[0]);

        for (Iterator<User> iterator = users.iterator(); iterator.hasNext();)
        {
            User user = (User) iterator.next();

            // on Persist
            ExecutorService.onPersist(em, user);
            
                 
            // on find by id.
            ExecutorService.findByKey(em,"BigDataUser");
            
            List<User> fetchedUsers = ExecutorService.onQueryByEmail(em, user);
            
            if (fetchedUsers != null && fetchedUsers.size() > 0)
            {
                logger.info(user.toString());
            }

            logger.info("");
            System.out.println("#######################Querying##########################################");
            logger.info("");
            logger.info("");

        }

        // Execute wild search query.
        String query = "Select u from User u";
        logger.info(query);
        ExecutorService.findByQuery(em, query);

        // // Execute native CQL. Fetch tweets for given user.
        logger.info("");
        System.out.println("#######################Querying##########################################");
        logger.info("");
        logger.info("");

        query = "Select * from tweets where user_id='RDBMSUser'";
        logger.info(query);
        ExecutorService.findByNativeQuery(em,query);
    	} finally
    	{
    		onDestroyDBResources(emf, em);
    	}
    }

	/**
	 * After successful processing close entity manager and it's factory instance.
	 * 
	 * @param emf          entity manager factory instance.
	 * @param em           entity manager instance
	 */
	private static void onDestroyDBResources(EntityManagerFactory emf,EntityManager em) {
		if(emf != null)
		{
			emf.close();
		}
		
		if(em != null)
		{
			em.close();
		}
	}


}
