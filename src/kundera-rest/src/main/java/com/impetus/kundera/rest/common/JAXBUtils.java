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
package com.impetus.kundera.rest.common;

import java.io.InputStream;
import java.io.StringWriter;

import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.json.JSONJAXBContext;
import com.sun.jersey.api.json.JSONMarshaller;
import com.sun.jersey.api.json.JSONUnmarshaller;

/**
 * Utility for converting objects into XML and vice versa
 * 
 * @author amresh.singh
 */
public class JAXBUtils {
	private static Logger log = LoggerFactory.getLogger(JAXBUtils.class);

	/**
	 * Converts <code>InputStream</code> to Object using JAXB
	 * 
	 * @param str
	 * @param objectClass
	 * @return
	 */
	public static Object toObject(InputStream is, Class<?> objectClass,
			String mediaType) {
		Object output = null;

		try {
			output = objectClass.newInstance();

			if (MediaType.APPLICATION_XML.equals(mediaType)) {
				JAXBContext jaxbContext = JAXBContext.newInstance(objectClass);

				Unmarshaller jaxbUnmarshaller = jaxbContext
						.createUnmarshaller();

				output = jaxbUnmarshaller.unmarshal(is);
			} else if (MediaType.APPLICATION_JSON.equals(mediaType)) {

				JAXBContext context = JSONJAXBContext.newInstance(objectClass);

				Unmarshaller m = context.createUnmarshaller();
				JSONUnmarshaller unmarshaller = JSONJAXBContext
						.getJSONUnmarshaller(m, context);

				output = unmarshaller.unmarshalFromJSON(is, objectClass);

			}

		} catch (JAXBException e) {
			log.warn("Error while converting String to Object using JAXB:"
					+ e.getMessage());
			return null;
		} catch (InstantiationException e) {
			log.warn("Error while converting String to Object using JAXB:"
					+ e.getMessage());
			return null;
		} catch (IllegalAccessException e) {
			log.warn("Error while converting String to Object using JAXB:"
					+ e.getMessage());
			return null;
		}
		return output;
	}

	public static String toString(Class<?> objectClass, Object object,
			String mediaType) {
		try {
			if (MediaType.APPLICATION_XML.equals(mediaType)) {
				JAXBContext jaxbContext = JAXBContext.newInstance(objectClass);
				Marshaller jaxbMarshaller = jaxbContext.createMarshaller();

				StringWriter writer = new StringWriter();

				jaxbMarshaller.marshal(object, writer);
				return writer.toString();

			} else if (MediaType.APPLICATION_JSON.equals(mediaType)) {
				StringWriter writer = new StringWriter();
				JAXBContext context = JSONJAXBContext
						.newInstance(new Class[] { objectClass });

				Marshaller m = context.createMarshaller();
				JSONMarshaller marshaller = JSONJAXBContext.getJSONMarshaller(
						m, context);

				marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
				marshaller.marshallToJSON(object, writer);
				return writer.toString();
			} else {
				return null;
			}
		} catch (JAXBException e) {
			log.error("Error during translation, Caused by:" + e.getMessage()
					+ ", returning null");
			return null;
		}
	}



}
