package com.impetus.kundera.metadata;

import javax.persistence.metamodel.Bindable.BindableType;
import javax.persistence.metamodel.Type.PersistenceType;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.metadata.entities.SingularEntity;
import com.impetus.kundera.metadata.model.type.DefaultEntityType;



public class DefaultEntityTypeTest{
	
	
	/** The builder. */
    @SuppressWarnings("rawtypes")
    private DefaultEntityType defaultTypeEntityObj;

    /**
     * Sets the up.
     * 
     * @param <X>
     *            the generic type
     * @throws Exception
     *             the exception
     */
    @SuppressWarnings("rawtypes")
    @Before
    public <X extends Class> void setUp() throws Exception
    {
    	
    	X clazz = (X) SingularEntity.class;
    	//AbstractManagedType<X> managedType = null;
    	defaultTypeEntityObj = new DefaultEntityType<X>(clazz, PersistenceType.ENTITY, null);
    }

	
	/**
     * Sets the up.
     * 
     * @param <X>
     *            the generic type
     
     * @throws Exception
     *             the exception
     */
    /*@SuppressWarnings("rawtypes")
    @Before
    public <X extends Class> void setUp() throws Exception
    {
    	X clazz = (X) SingularEntity.class;
    	AbstractManagedType<X> managedType = null;
    	managedType = new DefaultEntityType<X>(clazz, PersistenceType.ENTITY, null);
    }*/

   
   
    @Test
    public void testPersistenceClassBindableType()
    {
        
    	Assert.assertEquals(defaultTypeEntityObj.getBindableType(), BindableType.ENTITY_TYPE);
    }
    
   
    
    @Test
    public void testName()
    {
       
    	Assert.assertEquals(defaultTypeEntityObj.getName(), "SingularEntity");
    }
    
    



}
