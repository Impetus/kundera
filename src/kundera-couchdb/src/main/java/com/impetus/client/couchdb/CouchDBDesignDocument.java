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
package com.impetus.client.couchdb;

import java.util.Map;

/**
 * Design document.
 * 
 * 
 * @author Kuldeep Mishra
 */
public class CouchDBDesignDocument
{

    /** The _rev. */
    private String _rev;

    /** The language. */
    private String language;

    /** The views. */
    private Map<String, MapReduce> views;

    /**
     * Gets the language.
     * 
     * @return the language
     */
    public String getLanguage()
    {
        return language;
    }

    /**
     * Gets the views.
     * 
     * @return the views
     */
    public Map<String, MapReduce> getViews()
    {
        return views;
    }

    /**
     * Sets the language.
     * 
     * @param language
     *            the new language
     */
    public void setLanguage(String language)
    {
        this.language = language;
    }

    /**
     * Sets the views.
     * 
     * @param views
     *            the views
     */
    public void setViews(Map<String, MapReduce> views)
    {
        this.views = views;
    }

    /**
     * Gets the _rev.
     * 
     * @return the _rev
     */
    public String get_rev()
    {
        return _rev;
    }

    /**
     * Sets the _rev.
     * 
     * @param _rev
     *            the new _rev
     */
    public void set_rev(String _rev)
    {
        this._rev = _rev;
    }

    /**
     * Holds Map Reduce functions.
     * 
     * @author Kuldeep Mishra
     */
    public static class MapReduce
    {

        /** The map. */
        private String map;

        /** The reduce. */
        private String reduce;

        /**
         * Gets the map.
         * 
         * @return the map
         */
        public String getMap()
        {
            return map;
        }

        /**
         * Gets the reduce.
         * 
         * @return the reduce
         */
        public String getReduce()
        {
            return reduce;
        }

        /**
         * Sets the map.
         * 
         * @param map
         *            the new map
         */
        public void setMap(String map)
        {
            this.map = map;
        }

        /**
         * Sets the reduce.
         * 
         * @param reduce
         *            the new reduce
         */
        public void setReduce(String reduce)
        {
            this.reduce = reduce;
        }
    }
}