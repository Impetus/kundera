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
package com.impetus.kundera.cache;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.PersistenceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.utils.DeepEquals;

/**
 * Cache for holding element collection column names and corresponding objects
 * This is a singleton class TODO: Improve singleton implementation code, think
 * performance overhead due to synchronization TODO: Think of a better way to
 * handle element collection object handling. better remove this cache
 * altogether
 * 
 * @author amresh.singh
 */
public class ElementCollectionCacheManager
{
    /** log for this class. */
    private static Log log = LogFactory.getLog(ElementCollectionCacheManager.class);

    /* Single instance */
    /** The instance. */
    private static ElementCollectionCacheManager instance;

    /**
     * Instantiates a new element collection cache manager.
     */
    private ElementCollectionCacheManager()
    {

    }

    /**
     * Gets the single instance of ElementCollectionCacheManager.
     * 
     * @return single instance of ElementCollectionCacheManager
     */
    public static synchronized ElementCollectionCacheManager getInstance()
    {
        if (instance == null)
        {
            instance = new ElementCollectionCacheManager();
        }
        return instance;
    }

    /**
     * Mapping between Row Key and (Map of element collection objects and
     * element collection object name).
     */
    private static Map<Object, Map<Object, String>> elementCollectionCache;

    /**
     * Gets the element collection cache.
     * 
     * @return the elementCollectionCache
     */
    public Map<Object, Map<Object, String>> getElementCollectionCache()
    {
        if (this.elementCollectionCache == null)
        {
            this.elementCollectionCache = new HashMap<Object, Map<Object, String>>();
        }
        return this.elementCollectionCache;
    }

    /**
     * Checks if is cache empty.
     * 
     * @return true, if is cache empty
     */
    public boolean isCacheEmpty()
    {
        return elementCollectionCache == null || elementCollectionCache.isEmpty();
    }

    /**
     * Adds the element collection cache mapping.
     * 
     * @param rowKey
     *            the row key
     * @param elementCollectionObject
     *            the element collection object
     * @param elementCollObjectName
     *            the element coll object name
     */
    public void addElementCollectionCacheMapping(Object rowKey, Object elementCollectionObject,
            String elementCollObjectName)
    {
        Map embeddedObjectMap = new HashMap<Object, String>();
        if (getElementCollectionCache().get(rowKey) == null)
        {
            embeddedObjectMap.put(elementCollectionObject, elementCollObjectName);
            getElementCollectionCache().put(rowKey, embeddedObjectMap);
        }
        else
        {
            getElementCollectionCache().get(rowKey).put(elementCollectionObject, elementCollObjectName);
        }
    }

    /**
     * Gets the element collection object name.
     * 
     * @param rowKey
     *            the row key
     * @param elementCollectionObject
     *            the element collection object
     * @return the element collection object name
     */
    public String getElementCollectionObjectName(Object rowKey, Object elementCollectionObject)
    {
        if (getElementCollectionCache().isEmpty() || getElementCollectionCache().get(rowKey) == null)
        {
            log.debug("No element collection object map found in cache for Row key " + rowKey);
            return null;
        }
        else
        {
            Map<Object, String> elementCollectionObjectMap = getElementCollectionCache().get(rowKey);
            String elementCollectionObjectName = elementCollectionObjectMap.get(elementCollectionObject);
            if (elementCollectionObjectName == null)
            {
                for (Object obj : elementCollectionObjectMap.keySet())
                {
                    if (DeepEquals.deepEquals(elementCollectionObject, obj))
                    {
                        elementCollectionObjectName = elementCollectionObjectMap.get(obj);
                        break;
                    }
                }
            }

            if (elementCollectionObjectName == null)
            {
                log.debug("No element collection object name found in cache for object:" + elementCollectionObject);
                return null;
            }
            else
            {
                return elementCollectionObjectName;
            }
        }
    }

    /**
     * Gets the last element collection object count.
     * 
     * @param rowKey
     *            the row key
     * @return the last element collection object count
     */
    public int getLastElementCollectionObjectCount(Object rowKey)
    {
        if (getElementCollectionCache().get(rowKey) == null)
        {
            log.debug("No element collection object map found in cache for Row key " + rowKey);
            return -1;
        }
        else
        {
            Map<Object, String> elementCollectionMap = getElementCollectionCache().get(rowKey);
            Collection<String> elementCollectionObjectNames = elementCollectionMap.values();
            int max = 0;

            for (String s : elementCollectionObjectNames)
            {
                String elementCollectionCountStr = s.substring(s.indexOf(Constants.EMBEDDED_COLUMN_NAME_DELIMITER) + 1);
                int elementCollectionCount = 0;
                try
                {
                    elementCollectionCount = Integer.parseInt(elementCollectionCountStr);
                }
                catch (NumberFormatException e)
                {
                    log.error("Invalid element collection Object name " + s);
                    throw new PersistenceException("Invalid element collection Object name " + s);
                }
                if (elementCollectionCount > max)
                {
                    max = elementCollectionCount;
                }
            }
            return max;
        }
    }

    /**
     * Clear cache.
     */
    public void clearCache()
    {
        this.elementCollectionCache = null;
        try
        {
            finalize();
        }
        catch (Throwable e)
        {
            log.warn("Unable to reclaim memory while clearing ElementCollection cache. Nothing to worry, will be taken care of by GC");
        }
    }

}
