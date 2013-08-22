package com.impetus.kundera.utils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.impetus.kundera.property.PropertyAccessorHelper;

public class KunderaCoreUtils
{
    /**
     * Retrun map of external properties for given pu;
     * 
     * @param pu
     * @param externalProperties
     * @param persistenceUnits
     * @return
     */
    public static Map<String, Object> getExternalProperties(String pu, Map<String, Object> externalProperties,
            String... persistenceUnits)
    {
        Map<String, Object> puProperty;
        if (persistenceUnits != null && persistenceUnits.length > 1 && externalProperties != null)
        {
            puProperty = (Map<String, Object>) externalProperties.get(pu);

            // if property found then return it, if it is null by pass it, else
            // throw invalidConfiguration.
            if (puProperty != null)
            {
                return fetchPropertyMap(puProperty);
            }
            return null;
        }
        return externalProperties;
    }

    /**
     * @param puProperty
     * @return
     */
    private static Map<String, Object> fetchPropertyMap(Map<String, Object> puProperty)
    {
        if (puProperty.getClass().isAssignableFrom(Map.class) || puProperty.getClass().isAssignableFrom(HashMap.class))
        {
            return puProperty;
        }
        else
        {
            throw new InvalidConfigurationException(
                    "For cross data store persistence, please specify as: Map {pu,Map of properties}");
        }
    }

    public static boolean isEmptyOrNull(Object o)
    {
        if (o == null)
        {
            return true;
        }
        if (PropertyAccessorHelper.isCollection(o.getClass()))
        {
            Collection c = (Collection) o;
            if (c.isEmpty())
            {
                return true;
            }
        }
        else if (Map.class.isAssignableFrom(o.getClass()))
        {
            Map m = (Map) o;
            if (m.isEmpty())
            {
                return true;
            }
        }
        return false;
    }
}
