package com.impetus.kundera.ejb.event;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Class to hold class-method instances for EntityListeners.
 * 
 * @author animesh.kumar
 */
public final class ExternalCallbackMethod implements CallbackMethod
{

    /** The clazz. */
    private Class<?> clazz;

    /** The method. */
    private Method method;

    /**
     * Instantiates a new external callback method.
     * 
     * @param clazz
     *            the clazz
     * @param method
     *            the method
     */
    public ExternalCallbackMethod(Class<?> clazz, Method method)
    {
        this.clazz = clazz;
        this.method = method;
    }

    /*
     * @see
     * com.impetus.kundera.ejb.event.CallbackMethod#invoke(java.lang.Object)
     */
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.ejb.event.CallbackMethod#invoke(java.lang.Object)
     */
    public void invoke(Object entity) throws IllegalArgumentException, IllegalAccessException,
            InvocationTargetException, InstantiationException
    {
        if (!method.isAccessible())
            method.setAccessible(true);
        method.invoke(clazz.newInstance(), new Object[] { entity });
    }

    /* @see java.lang.Object#toString() */
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(clazz.getName() + "." + method.getName());
        return builder.toString();
    }
}