/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.proxy.cglib;

import java.util.Set;

import net.sf.cglib.transform.impl.InterceptFieldCallback;

import com.impetus.kundera.intercept.AbstractFieldInterceptor;
import com.impetus.kundera.intercept.FieldInterceptor;
import com.impetus.kundera.proxy.KunderaProxy;
import com.impetus.kundera.proxy.LazyInitializer;

/**
 * A field-level interceptor that initializes lazily fetched properties.
 * This interceptor can be attached to classes instrumented by CGLIB.
 * Note that this implementation assumes that the instance variable
 * name is the same as the name of the persistent property that must
 * be loaded. 
 * @author amresh.singh
 */
public class CglibFieldInterceptorImpl extends AbstractFieldInterceptor implements FieldInterceptor,
        InterceptFieldCallback
{

    public CglibFieldInterceptorImpl(Set uninitializedFields, String entityName)
    {
        super(uninitializedFields, entityName);
    }   
    
    @Override
    public boolean readBoolean(Object target, String name, boolean oldValue) {
        return ( ( Boolean ) intercept( target, name, oldValue  ? Boolean.TRUE : Boolean.FALSE ) )
                .booleanValue();
    }

    @Override
    public byte readByte(Object target, String name, byte oldValue) {
        return ( ( Byte ) intercept( target, name, new Byte( oldValue ) ) ).byteValue();
    }

    @Override
    public char readChar(Object target, String name, char oldValue) {
        return ( ( Character ) intercept( target, name, new Character( oldValue ) ) )
                .charValue();
    }

    @Override
    public double readDouble(Object target, String name, double oldValue) {
        return ( ( Double ) intercept( target, name, new Double( oldValue ) ) )
                .doubleValue();
    }

    @Override
    public float readFloat(Object target, String name, float oldValue) {
        return ( ( Float ) intercept( target, name, new Float( oldValue ) ) )
                .floatValue();
    }

    @Override
    public int readInt(Object target, String name, int oldValue) {
        return ( ( Integer ) intercept( target, name, new Integer( oldValue ) ) )
                .intValue();
    }

    @Override
    public long readLong(Object target, String name, long oldValue) {
        return ( ( Long ) intercept( target, name, new Long( oldValue ) ) ).longValue();
    }

    @Override
    public short readShort(Object target, String name, short oldValue) {
        return ( ( Short ) intercept( target, name, new Short( oldValue ) ) )
                .shortValue();
    }
    
    @Override
    public Object readObject(Object target, String name, Object oldValue) {
        Object value = intercept( target, name, oldValue );
        if (value instanceof KunderaProxy) {
            LazyInitializer li = ( (KunderaProxy) value ).getKunderaLazyInitializer();
            if ( li.isUnwrap() ) {
                value = li.getImplementation();
            }
        }
        return value;
    }

    @Override
    public boolean writeBoolean(Object target, String name, boolean oldValue, boolean newValue) {
        dirty();
        intercept( target, name, oldValue ? Boolean.TRUE : Boolean.FALSE );
        return newValue;
    }

    @Override
    public byte writeByte(Object target, String name, byte oldValue, byte newValue) {
        dirty();
        intercept( target, name, new Byte( oldValue ) );
        return newValue;
    }

    @Override
    public char writeChar(Object target, String name, char oldValue, char newValue) {
        dirty();
        intercept( target, name, new Character( oldValue ) );
        return newValue;
    }

    @Override
    public double writeDouble(Object target, String name, double oldValue, double newValue) {
        dirty();
        intercept( target, name, new Double( oldValue ) );
        return newValue;
    }

    @Override
    public float writeFloat(Object target, String name, float oldValue, float newValue) {
        dirty();
        intercept( target, name, new Float( oldValue ) );
        return newValue;
    }

    @Override
    public int writeInt(Object target, String name, int oldValue, int newValue) {
        dirty();
        intercept( target, name, new Integer( oldValue ) );
        return newValue;
    }    
    
    @Override
    public long writeLong(Object target, String name, long oldValue, long newValue) {
        dirty();
        intercept( target, name, new Long( oldValue ) );
        return newValue;
    }

    @Override
    public short writeShort(Object target, String name, short oldValue, short newValue) {
        dirty();
        intercept( target, name, new Short( oldValue ) );
        return newValue;
    }

    @Override
    public Object writeObject(Object target, String name, Object oldValue, Object newValue) {
        dirty();
        intercept( target, name, oldValue );
        return newValue;
    }

    public String toString() {
        return "CglibFieldInterceptorImpl(" +
            "entityName=" + getEntityName() +
            ",dirty=" + isDirty() +
            ",uninitializedFields=" + getUninitializedFields() +
            ')';
    }

}
