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
package com.impetus.kundera.tests.file.dao;

import java.io.File;
import java.io.IOException;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import com.google.common.io.Files;
import com.impetus.kundera.tests.file.entities.ProfilePicture;

/**
 * DAO for profile picture class
 * 
 * @author amresh.singh
 */
public class ProfilePictureDao
{

    EntityManagerFactory emf;

    EntityManager em;

    public ProfilePictureDao(String persistenceUnitName)
    {
        if (emf == null)
        {
            emf = Persistence.createEntityManagerFactory(persistenceUnitName);
        }
    }

    public void addProfilePicture(int id, File fullPicture)
    {
        EntityManager em = getEntityManager();

        ProfilePicture pp = new ProfilePicture();
        pp.setProfilePicId(id);

        try
        {
            pp.setFullPicture(Files.toByteArray(fullPicture));
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        em.persist(pp);
        closeEntityManager();
    }

    public ProfilePicture getProfilePicture(int id)
    {
        EntityManager em = getEntityManager();

        ProfilePicture pp = em.find(ProfilePicture.class, 1);

        closeEntityManager();
        return pp;
    }

    EntityManager getEntityManager()
    {
        if (em == null)
        {
            em = emf.createEntityManager();
        }
        return em;
    }

    private void closeEntityManager()
    {
        if (em != null)
        {
            em.close();
            em = null;
        }
    }

    void closeEntityManagerfactory()
    {
        if (emf != null)
        {
            emf.close();
            emf = null;
        }
    }
}
