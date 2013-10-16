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

//    @Override
//    public int hashCode()
//    {
//        int result = super.hashCode();
//        result = result + ((language == null) ? 0 : language.hashCode());
//        result = result + ((views == null) ? 0 : views.hashCode());
//        return result;
//    }
//
//    /**
//     * Indicates whether some other design document is equals to this one.
//     */
//    @Override
//    public boolean equals(Object obj)
//    {
//        if (this == obj)
//            return true;
//        if (getClass() != obj.getClass())
//            return false;
//        CouchDBDesignDocument other = (CouchDBDesignDocument) obj;
//
//        return (language != null ? language.equals(other.language) : false)
//                && (views != null ? views.equals(other.views) : null);
//    }

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

//        @Override
//        public int hashCode()
//        {
//            int result = 1;
//            result = result + ((map == null) ? 0 : map.hashCode());
//            result = result + ((reduce == null) ? 0 : reduce.hashCode());
//            return result;
//        }
//
//        @Override
//        public boolean equals(Object obj)
//        {
//            if (this == obj)
//                return true;
//            if (obj == null)
//                return false;
//            if (getClass() != obj.getClass())
//                return false;
//            MapReduce other = (MapReduce) obj;
//            return (map != null ? map.equals(other.map) : false)
//                    && (reduce != null ? reduce.equals(other.reduce) : false);
//        }
    }

    public String get_rev()
    {
        return _rev;
    }

    public void set_rev(String _rev)
    {
        this._rev = _rev;
    }
}