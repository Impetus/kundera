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
package com.impetus.kundera.loader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.PersistenceException;
import javax.persistence.spi.PersistenceUnitTransactionType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.utils.InvalidConfigurationException;

/**
 * The Class PersistenceXMLLoader.
 * 
 * @author amresh.singh
 */
public class PersistenceXMLLoader
{
    /** The log. */
    private static Logger log = LoggerFactory.getLogger(PersistenceXMLLoader.class);

    /**
     * Instantiates a new persistence xml loader.
     */
    private PersistenceXMLLoader()
    {
    }

    /**
     * Gets the document.
     * 
     * @param configURL
     *            the config url
     * @return the document
     * @throws Exception
     *             the exception
     */
    private static Document getDocument(URL configURL) throws InvalidConfigurationException
    {
        InputStream is = null;
        Document doc;
        try
        {
            is = null;
            if (configURL != null)
            {
                URLConnection conn = configURL.openConnection();
                conn.setUseCaches(false); // avoid JAR locking on Windows and
                                          // Tomcat
                is = conn.getInputStream();
            }
            if (is == null)
            {
                throw new IOException("Failed to obtain InputStream from url: " + configURL);
            }

            DocumentBuilderFactory docBuilderFactory = null;
            docBuilderFactory = DocumentBuilderFactory.newInstance();
            docBuilderFactory.setNamespaceAware(true);

            final Schema v2Schema = SchemaFactory.newInstance(javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI).newSchema(
                    new StreamSource(getStreamFromClasspath("persistence_2_0.xsd")));
            final Validator v2Validator = v2Schema.newValidator();
            final Schema v1Schema = SchemaFactory.newInstance(javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI).newSchema(
                    new StreamSource(getStreamFromClasspath("persistence_1_0.xsd")));
            final Validator v1Validator = v1Schema.newValidator();

            InputSource source = new InputSource(is);
            DocumentBuilder docBuilder = null;
            try
            {
                docBuilder = docBuilderFactory.newDocumentBuilder();
            }
            catch (ParserConfigurationException e)
            {
                log.error("Error during parsing, Caused by: " + e.getMessage());
                throw new PersistenceLoaderException(e);
            }

            List errors = new ArrayList();
            docBuilder.setErrorHandler(new ErrorLogger("XML InputStream", errors));
            doc = docBuilder.parse(source);

            if (errors.size() == 0)
            {
                v2Validator.setErrorHandler(new ErrorLogger("XML InputStream", errors));
                v2Validator.validate(new DOMSource(doc));
                boolean isV1Schema = false;
                if (errors.size() != 0)
                {
                    // v2 fails, it could be because the file is v1.
                    Exception exception = (Exception) errors.get(0);
                    final String errorMessage = exception.getMessage();
                    // is it a validation error due to a v1 schema validated by
                    // a v2
                    isV1Schema = errorMessage.contains("1.0") && errorMessage.contains("2.0")
                            && errorMessage.contains("version");
                }
                if (isV1Schema)
                {
                    errors.clear();
                    v1Validator.setErrorHandler(new ErrorLogger("XML InputStream", errors));
                    v1Validator.validate(new DOMSource(doc));
                }
            }
            else
            {
                throw new InvalidConfigurationException("invalid persistence.xml", (Throwable) errors.get(0));
            }
        }
        catch (IOException e)
        {
            throw new InvalidConfigurationException(e);
        }
        catch (SAXException e)
        {
            throw new InvalidConfigurationException(e);
        }
        finally
        {
            try
            {
                is.close();
            }
            catch (IOException e)
            {
                throw new InvalidConfigurationException(e);
            }
        }

        return doc;
    }

    /**
     * Get stream from classpath.
     * 
     * @param fileName
     *            the file name
     * @return the stream
     * @throws Exception
     *             the exception
     */
    private static InputStream getStreamFromClasspath(String fileName)
    {
        String path = fileName;
        InputStream dtdStream = PersistenceXMLLoader.class.getClassLoader().getResourceAsStream(path);
        return dtdStream;
    }

    /**
     * Find persistence units.
     * 
     * @param url
     *            the url
     * @return the list
     * @throws Exception
     *             the exception
     */
    public static List<PersistenceUnitMetadata> findPersistenceUnits(URL url) throws Exception
    {
        return findPersistenceUnits(url, PersistenceUnitTransactionType.JTA);
    }

    /**
     * Find persistence units.
     * 
     * @param url
     *            the url
     * @param defaultTransactionType
     *            the default transaction type
     * @return the list
     * @throws Exception
     *             the exception
     */
    public static List<PersistenceUnitMetadata> findPersistenceUnits(URL url,
            PersistenceUnitTransactionType defaultTransactionType) throws InvalidConfigurationException
    {

        Document doc;
        try
        {
            doc = getDocument(url);
        }
        catch (InvalidConfigurationException e)
        {
            throw e;
        }
        Element top = doc.getDocumentElement();
        NodeList children = top.getChildNodes();
        ArrayList<PersistenceUnitMetadata> units = new ArrayList<PersistenceUnitMetadata>();

        // parse for persistenceUnitRootInfoURL.
        for (int i = 0; i < children.getLength(); i++)
        {
            if (children.item(i).getNodeType() == Node.ELEMENT_NODE)
            {
                Element element = (Element) children.item(i);
                String tag = element.getTagName();
                // look for "persistence-unit" element
                if (tag.equals("persistence-unit"))
                {
                    PersistenceUnitMetadata metadata = parsePersistenceUnit(element);
                    metadata.setPersistenceUnitRootUrl(getPersistenceRootUrl(url));
                    units.add(metadata);
                }
            }
        }
        return units;
    }

    /**
     * Parses the persistence unit.
     * 
     * @param top
     *            the top
     * @return the persistence metadata
     * @throws Exception
     *             the exception
     */
    private static PersistenceUnitMetadata parsePersistenceUnit(Element top)
    {
        PersistenceUnitMetadata metadata = new PersistenceUnitMetadata();

        String puName = top.getAttribute("name");
        if (!isEmpty(puName))
        {
            log.trace("Persistent Unit name from persistence.xml: " + puName);
            metadata.setPersistenceUnitName(puName);
            String transactionType = top.getAttribute("transaction-type");
            if (StringUtils.isEmpty(transactionType)
                    || PersistenceUnitTransactionType.RESOURCE_LOCAL.name().equals(transactionType))
            {

                metadata.setTransactionType(PersistenceUnitTransactionType.RESOURCE_LOCAL);

            }
            else if (PersistenceUnitTransactionType.JTA.name().equals(transactionType))
            {
                metadata.setTransactionType(PersistenceUnitTransactionType.JTA);
            }
        }

        NodeList children = top.getChildNodes();
        for (int i = 0; i < children.getLength(); i++)
        {
            if (children.item(i).getNodeType() == Node.ELEMENT_NODE)
            {
                Element element = (Element) children.item(i);
                String tag = element.getTagName();

                if (tag.equals("provider"))
                {
                    metadata.setProvider(getElementContent(element));
                }

                /*
                 * else if (tag.equals("transaction-type")) { String
                 * transactionType = getElementContent(element);
                 * 
                 * if (StringUtils.isEmpty(transactionType) ||
                 * PersistenceUnitTransactionType
                 * .RESOURCE_LOCAL.name().equals(transactionType)) {
                 * 
                 * metadata.setTransactionType(PersistenceUnitTransactionType.
                 * RESOURCE_LOCAL);
                 * 
                 * } else if
                 * (PersistenceUnitTransactionType.JTA.name().equals(transactionType
                 * )) {
                 * metadata.setTransactionType(PersistenceUnitTransactionType
                 * .JTA); }
                 * 
                 * }
                 */else if (tag.equals("properties"))
                {
                    NodeList props = element.getChildNodes();
                    for (int j = 0; j < props.getLength(); j++)
                    {
                        if (props.item(j).getNodeType() == Node.ELEMENT_NODE)
                        {
                            Element propElement = (Element) props.item(j);
                            // if element is not "property" then skip
                            if (!"property".equals(propElement.getTagName()))
                            {
                                continue;
                            }

                            String propName = propElement.getAttribute("name").trim();
                            String propValue = propElement.getAttribute("value").trim();
                            if (isEmpty(propValue))
                            {
                                propValue = getElementContent(propElement, "");
                            }
                            metadata.getProperties().put(propName, propValue);
                        }
                    }
                }
                // Kundera doesn't support "class", "jar-file" and
                // "excluded-unlisted-classes" for now.. but will someday.
                // let's parse it for now.
                else if (tag.equals("class"))
                {
                    metadata.getClasses().add(getElementContent(element));
                }
                else if (tag.equals("jar-file"))
                {
                    metadata.addJarFile(getElementContent(element));
                }
                else if (tag.equals("exclude-unlisted-classes"))
                {
                    metadata.setExcludeUnlistedClasses(true);
                }
            }
        }
        PersistenceUnitTransactionType transactionType = getTransactionType(top.getAttribute("transaction-type"));
        if (transactionType != null)
        {
            metadata.setTransactionType(transactionType);
        }

        return metadata;
    }

    /**
     * Gets the transaction type.
     * 
     * @param elementContent
     *            the element content
     * @return the transaction type
     */
    public static PersistenceUnitTransactionType getTransactionType(String elementContent)
    {

        if (elementContent == null || elementContent.isEmpty())
        {
            return null;
        }
        else if (elementContent.equalsIgnoreCase("JTA"))
        {
            return PersistenceUnitTransactionType.JTA;
        }
        else if (elementContent.equalsIgnoreCase("RESOURCE_LOCAL"))
        {
            return PersistenceUnitTransactionType.RESOURCE_LOCAL;
        }
        else
        {
            throw new PersistenceException("Unknown TransactionType: " + elementContent);
        }
    }

    /**
     * The Class ErrorLogger.
     */
    public static class ErrorLogger implements ErrorHandler
    {

        /** The file. */
        private String file;

        /** The errors. */
        private List errors;

        /**
         * Instantiates a new error logger.
         * 
         * @param file
         *            the file
         * @param errors
         *            the errors
         */
        ErrorLogger(String file, List errors)
        {
            this.file = file;
            this.errors = errors;
        }

        /* @see org.xml.sax.ErrorHandler#error(org.xml.sax.SAXParseException) */
        /*
         * (non-Javadoc)
         * 
         * @see org.xml.sax.ErrorHandler#error(org.xml.sax.SAXParseException)
         */
        public void error(SAXParseException error)
        {
            log.error("Error parsing XML: " + file + '(' + error.getLineNumber() + ") " + error.getMessage());
            errors.add(error);
        }

        /*
         * @see
         * org.xml.sax.ErrorHandler#fatalError(org.xml.sax.SAXParseException)
         */
        /*
         * (non-Javadoc)
         * 
         * @see
         * org.xml.sax.ErrorHandler#fatalError(org.xml.sax.SAXParseException)
         */
        public void fatalError(SAXParseException error)
        {
            log.error("Error parsing XML: " + file + '(' + error.getLineNumber() + ") " + error.getMessage());
            errors.add(error);
        }

        /* @see org.xml.sax.ErrorHandler#warning(org.xml.sax.SAXParseException) */
        /*
         * (non-Javadoc)
         * 
         * @see org.xml.sax.ErrorHandler#warning(org.xml.sax.SAXParseException)
         */
        public void warning(SAXParseException warn)
        {
            log.warn("Warning parsing XML: " + file + '(' + warn.getLineNumber() + ") " + warn.getMessage());
        }
    }

    /**
     * Checks if is empty.
     * 
     * @param str
     *            the str
     * @return true, if is empty
     */
    private static boolean isEmpty(String str)
    {
        return null == str || str.isEmpty();
    }

    /**
     * Gets the element content.
     * 
     * @param element
     *            the element
     * @return the element content
     * @throws Exception
     *             the exception
     */
    public static String getElementContent(final Element element)
    {
        return getElementContent(element, null);
    }

    /**
     * Get the content of the given element.
     * 
     * @param element
     *            The element to get the content for.
     * @param defaultStr
     *            The default to return when there is no content.
     * @return The content of the element or the default.
     * @throws Exception
     *             the exception
     */
    private static String getElementContent(Element element, String defaultStr)
    {
        if (element == null)
        {
            return defaultStr;
        }

        NodeList children = element.getChildNodes();
        StringBuilder result = new StringBuilder("");
        for (int i = 0; i < children.getLength(); i++)
        {
            if (children.item(i).getNodeType() == Node.TEXT_NODE
                    || children.item(i).getNodeType() == Node.CDATA_SECTION_NODE)
            {
                result.append(children.item(i).getNodeValue());
            }
        }
        return result.toString().trim();
    }

    /**
     * Returns persistence unit root url
     * 
     * @param url
     *            raw url
     * @return rootUrl rootUrl
     */
    private static URL getPersistenceRootUrl(URL url)
    {

        String f = url.getFile();
        f = parseFilePath(f);
        URL jarUrl = url;
        try
        {
            if (AllowedProtocol.isJarProtocol(url.getProtocol()))
            {
                jarUrl = new URL(f);
                if (jarUrl.getProtocol() != null
                        && AllowedProtocol.FILE.name().equals(jarUrl.getProtocol().toUpperCase())
                        && StringUtils.contains(f, " "))
                {
                    jarUrl = new File(f).toURI().toURL();
                }
            }
            else if (AllowedProtocol.isValidProtocol(url.getProtocol()))
            {
                if (StringUtils.contains(f, " "))
                {
                    jarUrl = new File(f).toURI().toURL();
                }
                else
                {
                    jarUrl = new File(f).toURL();
                }
            }
        }
        catch (MalformedURLException mex)
        {
            log.error("Error during getPersistenceRootUrl(), Caused by: " + mex.getMessage());
            throw new IllegalArgumentException("invalid jar URL[] provided!" + url);
        }

        return jarUrl;
    }

    /**
     * Parse and exclude path till META-INF
     * 
     * @param file
     *            raw file path.
     * @return extracted/parsed file path.
     */
    private static String parseFilePath(String file)
    {
        final String excludePattern = "/META-INF/persistence.xml";
        file = file.substring(0, file.length() - excludePattern.length());

        // in case protocol is "file".
        file = file.endsWith("!") ? file.substring(0, file.length() - 1) : file;

        return file;
    }

    /**
     * Allowed protocols
     */
    private enum AllowedProtocol
    {
        WSJAR, JAR, ZIP, FILE, VFSZIP;

        /**
         * In case it is jar protocol
         * 
         * @param protocol
         * @return
         */
        public static boolean isJarProtocol(String protocol)
        {
            return protocol != null
                    && (protocol.toUpperCase().equals(JAR.name()) || protocol.toUpperCase().equals(WSJAR.name()));
        }

        /**
         * If provided protocol is within allowed protocol.
         * 
         * @param protocol
         *            protocol
         * @return true, if it is in allowed protocol.
         */
        public static boolean isValidProtocol(String protocol)
        {
            try
            {
                AllowedProtocol.valueOf(protocol.toUpperCase());
                return true;
            }
            catch (IllegalArgumentException iex)
            {
                return false;
            }
        }
    }

}
