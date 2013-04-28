package com.impetus.kundera.utils;

import java.util.HashMap;
import java.util.Map;

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
        if (persistenceUnits.length > 1 && externalProperties != null)
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
}
