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
import javax.xml.XMLConstants;

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
     * Reads the persistence xml content into an object graph and validates it against the related xsd schema.
     *
     * @param pathToPersistenceXml
     *            path to the persistence.xml file
     * @return parsed persistence xml as object graph
     * @throws InvalidConfigurationException if the file could not be parsed or is not valid against the schema
     */
    private static Document getDocument(URL pathToPersistenceXml) throws InvalidConfigurationException {
        InputStream is = null;
        Document xmlRootNode = null;

        try {
            if (pathToPersistenceXml != null) {
                URLConnection conn = pathToPersistenceXml.openConnection();
                conn.setUseCaches(false); // avoid JAR locking on Windows and Tomcat.
                is = conn.getInputStream();
            }

            if (is == null) {
                throw new IOException("Failed to obtain InputStream from url: " + pathToPersistenceXml);
            }

            xmlRootNode = parseDocument(is);
            validateDocumentAgainstSchema(xmlRootNode);
        } catch (IOException e) {
            throw new InvalidConfigurationException(e);
        } finally {
            if(is != null) {
                try {
                    is.close();
                } catch (IOException ex) {
                    log.warn("Input stream could not be closed after parsing persistence.xml, caused by: {}", ex);
                }
            }
        }
        
        return xmlRootNode;
    }

    /**
     * Reads the content of the persistence.xml file into an object model, with the root node of type {@link Document}.
     *
     * @param is {@link InputStream} of the persistence.xml content
     * @return root node of the parsed xml content
     * @throws InvalidConfigurationException if the content could not be read due to an I/O error or could not be
     * parsedÏ
     */
    private static Document parseDocument(final InputStream is) throws InvalidConfigurationException {
        Document persistenceXmlDoc;
        final List parsingErrors = new ArrayList();
        final InputSource source = new InputSource(is);
        final DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
        docBuilderFactory.setNamespaceAware(true);

        try {
            DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
            docBuilder.setErrorHandler(new ErrorLogger("XML InputStream", parsingErrors));
            persistenceXmlDoc = docBuilder.parse(source);
        } catch (ParserConfigurationException e) {
            log.error("Error during parsing, Caused by: {}.", e);
            throw new PersistenceLoaderException("Error during parsing persistence.xml, caused by: ", e);
        } catch (IOException e) {
            throw new InvalidConfigurationException("Error reading persistence.xml, caused by: ", e);
        } catch (SAXException e) {
            throw new InvalidConfigurationException("Error parsing persistence.xml, caused by: ", e);
        }

        if (!parsingErrors.isEmpty()) {
            throw new InvalidConfigurationException("Invalid persistence.xml", (Throwable) parsingErrors.get(0));
        }

        return persistenceXmlDoc;
    }

    /**
     * Validates an xml object graph against its schema. Therefore it reads the version from the root tag
     * and tries to load the related xsd file from the classpath.
     * 
     * @param xmlRootNode root xml node of the document to validate
     * @throws InvalidConfigurationException if the validation could not be performed or the xml graph is invalid
     * against the schema
     */
    private static void validateDocumentAgainstSchema(final Document xmlRootNode) throws InvalidConfigurationException {
        final Element rootElement = xmlRootNode.getDocumentElement();
        final String version = rootElement.getAttribute("version");
        String schemaFileName = "persistence_" + version.replace(".", "_") + ".xsd";
        
        try {
        final List validationErrors = new ArrayList();
        final String schemaLanguage = XMLConstants.W3C_XML_SCHEMA_NS_URI;
        final StreamSource streamSource = new StreamSource(getStreamFromClasspath(schemaFileName));
        final Schema schemaDefinition = SchemaFactory.newInstance(schemaLanguage).newSchema(streamSource);

        final Validator schemaValidator = schemaDefinition.newValidator();
        schemaValidator.setErrorHandler(new ErrorLogger("XML InputStream", validationErrors));
        schemaValidator.validate(new DOMSource(xmlRootNode));
        
        if(!validationErrors.isEmpty()) {
            final String exceptionText = "persistence.xml is not conform against the supported schema definitions.";
            throw new InvalidConfigurationException(exceptionText);
        }
        } catch(SAXException e) {
            final String exceptionText = "Error validating persistence.xml against schema defintion, caused by: ";
            throw new InvalidConfigurationException(exceptionText , e);
        } catch(IOException e) {
            final String exceptionText = "Error opening xsd schema file. The given persistence.xml descriptor version "
                    + version + " might not be supported yet.";
            throw new InvalidConfigurationException(exceptionText, e);
        }
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
    public static List<PersistenceUnitMetadata> findPersistenceUnits(final URL url,
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
        doc.getXmlVersion();
        Element top = doc.getDocumentElement();

        String versionName = top.getAttribute("version");

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
                    PersistenceUnitMetadata metadata = parsePersistenceUnit(url, element, versionName);
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
    private static PersistenceUnitMetadata parsePersistenceUnit(final URL url, Element top, final String versionName)
    {
        PersistenceUnitMetadata metadata = new PersistenceUnitMetadata(versionName, getPersistenceRootUrl(url), url);

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
                else if (tag.equals("properties"))
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
            log.error("Error during getPersistenceRootUrl(), Caused by: {}.", mex);
            throw new IllegalArgumentException("Invalid jar URL[] provided!" + url);
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
    public enum AllowedProtocol
    {
        WSJAR, JAR, ZIP, FILE, VFSZIP, VFS;

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
