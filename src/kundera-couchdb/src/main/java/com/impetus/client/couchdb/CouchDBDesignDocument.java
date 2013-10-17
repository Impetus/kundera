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
    private String _rev;

    private String language;

    private Map<String, MapReduce> views;

    public String getLanguage()
    {
        return language;
    }

    public Map<String, MapReduce> getViews()
    {
        return views;
    }

    public void setLanguage(String language)
    {
        this.language = language;
    }

    public void setViews(Map<String, MapReduce> views)
    {
        this.views = views;
    }

    public String get_rev()
    {
        return _rev;
    }

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
        private String map;

        private String reduce;

        public String getMap()
        {
            return map;
        }

        public String getReduce()
        {
            return reduce;
        }

        public void setMap(String map)
        {
            this.map = map;
        }

        public void setReduce(String reduce)
        {
            this.reduce = reduce;
        }
    }
}