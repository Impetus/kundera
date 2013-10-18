/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.tests.crossdatastore.useraddress;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Metamodel;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;

/**
 * The Class TwinAssociation.
 * 
 * @author vivek.mishra
 */
public abstract class TwinAssociation extends AssociationBase
{

    /** The combinations. */
    protected static List<Map<Class, String>> combinations = new ArrayList<Map<Class, String>>();

    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(TwinAssociation.class);

    /**
     * Inits the.
     * 
     * @param classes
     *            the classes
     * @param persistenceUnits
     *            the persistence units
     */
    public static void init(List<Class> classes, String... persistenceUnits) throws Exception
    {
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
        combinations = null;
        combinations = new ArrayList<Map<Class, String>>();

        /*
         * if (RUN_IN_EMBEDDED_MODE) { CassandraCli.cassandraSetUp();
         * 
         * }
         * 
         * if (AUTO_MANAGE_SCHEMA) { CassandraCli.initClient(); }
         */

        // list of PUS with class.
        Map<Class, String> puClazzMapper = null;

        for (String pu : persistenceUnits)
        {
            for (String p : persistenceUnits)
            {
                puClazzMapper = new HashMap<Class, String>();
                puClazzMapper.put(classes.get(0), pu);

                for (Class c : classes.subList(1, classes.size()))
                {
                    puClazzMapper.put(c, p);
                }
                combinations.add(puClazzMapper);
            }
        }
    }

    /**
     * Try operation.
     * 
     * @param ALL_PUs_UNDER_TEST
     */
    protected void tryOperation(String[] ALL_PUs_UNDER_TEST)
    {
        try
        {
            Metamodel metaModel = null;
            for (int i = 0; i < ALL_PUs_UNDER_TEST.length; i++)
            {
                metaModel = KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(ALL_PUs_UNDER_TEST[i]);

                for (int i1 = 0; i1 < ALL_PUs_UNDER_TEST.length; i1++)
                {
                    if (i != i1)
                    {
                        Map<Class<?>, EntityType<?>> original = getManagedTypes((MetamodelImpl) metaModel);

                        Metamodel m = KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                                ALL_PUs_UNDER_TEST[i1]);
                        Map<Class<?>, EntityType<?>> copy = getManagedTypes((MetamodelImpl) m);
                        if (original != null && copy != null)
                        {
                            original.putAll(copy);
                        }
                    }
                }
            }

            for (Map<Class, String> c : combinations)
            {
                switchPersistenceUnits(c);
                insert();
                // em.clear();
                find();
                findPersonByIdColumn();
                findPersonByName();
                findAddressByIdColumn();
                findAddressByStreet();
                update();
                remove();
                tearDownInternal(ALL_PUs_UNDER_TEST);
            }
        }
        catch (Exception e)
        {
            log.error("Error during switching persistence unit", e);
            Assert.fail("Failure caused by:" + e);
        }
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

    private Map<Class<?>, EntityType<?>> getManagedTypes(MetamodelImpl metaModel)
    {
        try
        {
            Field managedTypesFields = null;
            if (metaModel != null)
                managedTypesFields = metaModel.getClass().getDeclaredField("entityTypes");
            if (managedTypesFields != null && !managedTypesFields.isAccessible())
            {
                managedTypesFields.setAccessible(true);

                return ((Map<Class<?>, EntityType<?>>) managedTypesFields.get(metaModel));
            }
        }
        catch (SecurityException e)
        {
            Assert.fail(e.getMessage());
        }
        catch (NoSuchFieldException e)
        {
            Assert.fail(e.getMessage());
        }
        catch (IllegalArgumentException e)
        {
            Assert.fail(e.getMessage());
        }
        catch (IllegalAccessException e)
        {
            Assert.fail(e.getMessage());
        }
        return null;
    }
}