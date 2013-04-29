/**
 * 
 */
package com.impetus.kundera.configure;

import com.impetus.kundera.KunderaException;

/**
 * @author Kuldeep Mishra
 * 
 */
public class PersistenceUnitConfigurationException extends KunderaException
{

    /** Default Serial Version UID. */
    private static final long serialVersionUID = 1L;

    /**
     * 
     */
    public PersistenceUnitConfigurationException()
    {
        super();
        // TODO Auto-generated constructor stub
    }

    /**
     * @param paramString
     * @param paramThrowable
     */
    public PersistenceUnitConfigurationException(String paramString, Throwable paramThrowable)
    {
        super(paramString, paramThrowable);
        // TODO Auto-generated constructor stub
    }

    /**
     * @param paramString
     */
    public PersistenceUnitConfigurationException(String paramString)
    {
        super(paramString);
        // TODO Auto-generated constructor stub
    }

    /**
     * @param paramThrowable
     */
    public PersistenceUnitConfigurationException(Throwable paramThrowable)
    {
        super(paramThrowable);
        // TODO Auto-generated constructor stub
    }
}
