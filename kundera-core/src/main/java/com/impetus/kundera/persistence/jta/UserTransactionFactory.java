/*******************************************************************************

 * * Copyright 2012 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/

package com.impetus.kundera.persistence.jta;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Hashtable;

import javax.naming.BinaryRefAddr;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

/**
 * The factory for JNDI lookup of <code> KunderaJTAUserTransaction</code>
 * objects.
 * 
 * @author vivek.mishra@impetus.co.in
 */

public class UserTransactionFactory implements ObjectFactory
{
    /**
     * Default constructor.
     */
    public UserTransactionFactory()
    {
    }

    /**
     * Returns reference to userTransaction object.
     * 
     * @see javax.naming.spi.ObjectFactory
     */
    @Override
    public Object getObjectInstance(Object obj, Name name, Context nameCtx, Hashtable environment) throws Exception
    {
        Reference ref = (Reference) obj;
        Object ret = null;

        if (ref.getClassName().equals("javax.transaction.UserTransaction")
                || ref.getClassName().equals("com.impetus.kundera.persistence.jta.KunderaJTAUserTransaction"))
        {
            ret = KunderaJTAUserTransaction.getCurrentTx();
        }

        if (ret == null)
        {
            ret = new KunderaJTAUserTransaction();
        }
        return ret;

    }

    /**
     * Method to return reference for serialized object.(i.e.
     * KunderJTAUserTransaction)
     * 
     * @param object
     *            serilized object.
     * @return reference to that object.
     * @throws NamingException
     *             naming exception.
     */
    public static Reference getReference(Serializable object) throws NamingException
    {
        ByteArrayOutputStream outStream;
        try
        {
            outStream = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(outStream);
            out.writeObject(object);
            out.close();
        }
        catch (IOException e)
        {
            throw new NamingException(e.getMessage());
        }

        BinaryRefAddr handle = new BinaryRefAddr("com.impetus.kundera.persistence.jta", outStream.toByteArray());
        Reference ret = new Reference(object.getClass().getName(), handle, UserTransactionFactory.class.getName(), null);
        return ret;
    }
}
