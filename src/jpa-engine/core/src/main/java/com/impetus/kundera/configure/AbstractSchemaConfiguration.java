package com.impetus.kundera.configure;

import java.util.Map;

import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

public abstract class AbstractSchemaConfiguration
{

    /** Holding instance for persistence units. */
    protected String[] persistenceUnits;

    /** Holding persistenceUnit properties */
    protected Map externalPropertyMap;

    protected KunderaMetadata kunderaMetadata;

    public AbstractSchemaConfiguration(final String[] persistenceUnits, final Map externalPropertyMap,
            final KunderaMetadata kunderaMetadata)
    {
        this.persistenceUnits = persistenceUnits;
        this.externalPropertyMap = externalPropertyMap;
        this.kunderaMetadata = kunderaMetadata;
    }

}
