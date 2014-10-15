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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.SchemaOutputResolver;
import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.factories.SchemaFactoryWrapper;

/**
 * Utility for converting objects into XML and vice versa
 * 
 * @author amresh.singh
 */
public class JAXBUtils {
    private static Logger log = LoggerFactory.getLogger(JAXBUtils.class);

    private static Map<Class<?>, String> schemaMap;

    public final static ObjectMapper mapper;
    private final static XmlMapper xmlMapper;
    static {
        mapper = new ObjectMapper();
        xmlMapper = new XmlMapper();
    }

    /**
     * Converts <code>InputStream</code> to Object using JAXB
     * 
     * @param str
     * @param objectClass
     * @return
     */
    public static Object toObject(InputStream is, Class<?> objectClass, String mediaType) {
        Object output = null;

        try {
            output = objectClass.newInstance();

            if (MediaType.APPLICATION_XML.equals(mediaType)) {

                output = xmlMapper.readValue(is, objectClass);

            } else if (MediaType.APPLICATION_JSON.equals(mediaType)) {

                output = mapper.readValue(is, objectClass);

            }
        } catch (InstantiationException e) {
            log.warn("Error while converting String to Object using JAXB:" + e.getMessage());
            return null;
        } catch (IllegalAccessException e) {
            log.warn("Error while converting String to Object using JAXB:" + e.getMessage());
            return null;
        } catch (JsonParseException e) {
            log.error(e.getMessage());
        } catch (JsonMappingException e) {
            log.error(e.getMessage());
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return output;
    }

    /**
     * Converts <code>InputStream</code> to Object using JAXB
     * 
     * @param str
     * @param objectClass
     * @return
     */
    public static Object toObject(String data, Class<?> objectClass, String mediaType) {
        Object output = null;

        try {
            output = objectClass.newInstance();

            if (MediaType.APPLICATION_XML.equals(mediaType)) {
                output = xmlMapper.readValue(data, objectClass);

            } else if (MediaType.APPLICATION_JSON.equals(mediaType)) {
                if (MediaType.APPLICATION_JSON.equals(mediaType)) {
                    output = mapper.readValue(data, objectClass);
                }
                return output;
            }
        } catch (InstantiationException e) {
            log.warn("Error while converting String to Object using JAXB:" + e.getMessage());
            return null;
        } catch (IllegalAccessException e) {
            log.warn("Error while converting String to Object using JAXB:" + e.getMessage());
            return null;
        } catch (JsonParseException e) {
            log.error(e.getMessage());
        } catch (JsonMappingException e) {
            log.error(e.getMessage());
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return output;
    }

    public static String toString(Object object, String mediaType) {
        try {
            if (MediaType.APPLICATION_XML.equals(mediaType)) {
                return xmlMapper.writeValueAsString(object);

            } else if (MediaType.APPLICATION_JSON.equals(mediaType)) {
                return mapper.writeValueAsString(object);
            }

        } catch (JsonProcessingException e) {
            log.error(e.getMessage());
        }
        return null;
    }

    /**
     * @param objectClass
     * @param mediaType
     * @return
     */
    public static String getSchema(Class<?> objectClass, String mediaType) {
        try {

            if (mediaType == MediaType.APPLICATION_JSON) {

                String schemaDef = null;

                if (schemaMap == null) {
                    schemaMap = new HashMap<Class<?>, String>();
                }
                if (schemaMap.containsKey(objectClass)) {
                    schemaDef = schemaMap.get(objectClass);

                } else {

                    ObjectMapper objectMapper = new ObjectMapper();
                    SchemaFactoryWrapper visitor = new SchemaFactoryWrapper();
                    objectMapper.acceptJsonFormatVisitor(objectClass, visitor);
                    JsonSchema schema = visitor.finalSchema();
                    schemaDef = objectMapper.writeValueAsString(schema);
                    schemaMap.put(objectClass, schemaDef);
                }

                return schemaDef;

            } else if (mediaType == MediaType.APPLICATION_XML) {
                JAXBContext jc = JAXBContext.newInstance(objectClass);
                // generate the schemas
                final ArrayList<ByteArrayOutputStream> schemaStreams = new ArrayList<ByteArrayOutputStream>();
                jc.generateSchema(new SchemaOutputResolver() {
                    @Override
                    public Result createOutput(String namespaceUri, String suggestedFileName) throws IOException {
                        ByteArrayOutputStream out = new ByteArrayOutputStream();
                        schemaStreams.add(out);
                        StreamResult streamResult = new StreamResult(out);
                        streamResult.setSystemId("");
                        return streamResult;
                    }
                });

                // convert to a list of string
                List<String> schemas = new ArrayList<String>();
                for (ByteArrayOutputStream os : schemaStreams) {
                    schemas.add(os.toString());

                }

                return schemaStreams.get(0).toString();

            }

        } catch (JAXBException e) {
            log.error("Error during translation, Caused by:" + e.getMessage() + ", returning null");
            return null;
        } catch (IOException e) {
            log.error("Error during translation, Caused by:" + e.getMessage() + ", returning null");
        }
        return null;
    }

}
