package com.impetus.kundera.configure;

import java.util.Map;

public abstract class AbstractSchemaConfiguration
{

    /** Holding instance for persistence units. */
    protected String[] persistenceUnits;

    /** Holding persistenceUnit properties */
    protected Map externalPropertyMap;

    public AbstractSchemaConfiguration(final String[] persistenceUnits,final Map externalPropertyMap)
    {
        this.persistenceUnits= persistenceUnits;
        this.externalPropertyMap = externalPropertyMap;
    }
        
}
