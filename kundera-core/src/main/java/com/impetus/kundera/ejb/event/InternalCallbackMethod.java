package com.impetus.kundera.ejb.event;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import com.impetus.kundera.metadata.model.EntityMetadata;

/**
 * The Class InternalCallbackMethod.
 * 
 * @author animesh.kumar
 */
public final class InternalCallbackMethod implements CallbackMethod
{

    /**
     * 
     */
    private final EntityMetadata entityMetadata;

    /** The method. */
    private Method method;

    /**
     * Instantiates a new internal callback method.
     * 
     * @param method
     *            the method
     * @param entityMetadata
     *            TODO
     */
    public InternalCallbackMethod(EntityMetadata entityMetadata, Method method)
    {
        this.entityMetadata = entityMetadata;
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
        method.invoke(entity, new Object[] {});
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
        builder.append(this.entityMetadata.getEntityClazz().getName() + "." + method.getName());
        return builder.toString();
    }
}