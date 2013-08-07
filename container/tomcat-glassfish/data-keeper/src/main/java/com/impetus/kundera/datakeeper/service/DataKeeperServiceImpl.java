package com.impetus.kundera.datakeeper.service;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.PersistenceException;

import org.primefaces.model.DefaultStreamedContent;
import org.primefaces.model.StreamedContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.datakeeper.dao.DataKeeperDao;
import com.impetus.kundera.datakeeper.entities.DocumentInfo;
import com.impetus.kundera.datakeeper.entities.Employee;
import com.impetus.kundera.datakeeper.entities.SubordinatesCounter;

/**
 * @author Kuldeep.Mishra
 * 
 */
public class DataKeeperServiceImpl implements DataKeeperService
{
    /**
     * logger used for logging statement.
     */
    private static final Logger log = LoggerFactory.getLogger(DataKeeperServiceImpl.class);

    private DataKeeperDao dao;

    public DataKeeperServiceImpl()
    {
    }

    public DataKeeperDao getDao()
    {
        return dao;
    }

    public void setDao(DataKeeperDao dao)
    {
        this.dao = dao;
    }

    @Override
    public void insertEmployee(Employee employee)
    {
        employee.setPassword(encriptPassword(employee.getPassword()));
        dao.insert(employee);
        log.info("Employee {} information successfully inserted.", employee.getEmployeeName());

        if (employee.getManager() != null)
        {
            SubordinatesCounter counter = new SubordinatesCounter();
            counter.setNoOfSubordinates(1);
            counter.setEmployeeId(employee.getManager().getEmployeeId());
            dao.insert(counter);
            log.info("Incremented no of subordinates for manager {}, successfully.", employee.getManager()
                    .getEmployeeName());
        }
    }

    @Override
    public Employee findEmployeeByName(String employeeName)
    {
        Employee employee = null;
        String query = "Select e from " + Employee.class.getSimpleName() + " e where e.employeeName = " + employeeName;
        List<Employee> employees = (List<Employee>) dao.findByQuery(query);
        if (!employees.isEmpty() && employees.get(0) != null)
        {
            employee = employees.get(0);
        }
        return employee;
    }

    @Override
    public List<Employee> findSubOrdinates(int managerId)
    {
        Employee manager = dao.findById(Employee.class, managerId);
        List<Employee> subOrdinates = manager != null ? manager.getSubordinates() : new ArrayList<Employee>();
        return subOrdinates;
    }

    @Override
    public void insertData(DocumentInfo data)
    {
        dao.insert(data);
        log.info("Data uploaded by  employee {} successfully inserted.", data.getOwnerName());
    }

    @Override
    public Employee findEmployee(Object employeeId)
    {
        log.info("Finding employee by id {} .", employeeId);
        return dao.findById(Employee.class, employeeId);
    }

    @Override
    public List<DocumentInfo> findDocumentByEmployeeId(String employeeId)
    {
        List<DocumentInfo> documents = new ArrayList<DocumentInfo>();
        try
        {
            Integer.parseInt(employeeId);
            documents = findDocumentByEmployeeId(Integer.parseInt(employeeId));
        }
        catch (NumberFormatException nfex)
        {
            log.warn("{} not a valid employee id.", employeeId);
        }
        return documents;
    }

    public List<DocumentInfo> findDocumentByEmployeeId(int employeeId)
    {
        List<DocumentInfo> documents = new ArrayList<DocumentInfo>();
        log.info("Finding document by employee id {} .", employeeId);
        String query = "Select d from " + DocumentInfo.class.getSimpleName() + " d where d.ownerId =  " + employeeId;
        documents = (List<DocumentInfo>) dao.findByQuery(query);

        for (DocumentInfo documentInfo : documents)
        {
            StreamedContent file = toStreamedContent(documentInfo.getData(), documentInfo.getDocumentName(),
                    "application/pdf");
            documentInfo.addFile(file);
        }
        return documents;
    }

    @Override
    public List<DocumentInfo> findDocumentByEmployeeName(String employeeName)
    {
        List<DocumentInfo> documents = new ArrayList<DocumentInfo>();
        log.info("Finding document by employee name {} .", employeeName);
        String query = "Select d from " + DocumentInfo.class.getSimpleName() + " d where d.ownerName =  "
                + employeeName;
        documents = (List<DocumentInfo>) dao.findByQuery(query);
        for (DocumentInfo documentInfo : documents)
        {
            StreamedContent file = toStreamedContent(documentInfo.getData(), documentInfo.getDocumentName(),
                    "application/pdf");
            documentInfo.addFile(file);
        }
        return documents;
    }

    @Override
    public void removeEmployee(Employee employee)
    {
        dao.remove(employee);
        List<DocumentInfo> documents = findDocumentByEmployeeId(employee.getEmployeeId());
        for (DocumentInfo document : documents)
        {
            if (document != null)
            {
                dao.remove(document);
            }
        }

        log.info("Employee successfully removed");
    }

    @Override
    public boolean authenticateEmployee(Employee employee, String password)
    {
        boolean success = false;
        if (employee != null && (employee.getPassword().equals(encriptPassword(password))))
        {
            log.info("Employee {} successfully authenticated.", employee.getEmployeeName());
            success = true;
        }
        else
        {
            log.info("Employee not authenticated, caused by either wrong userName or wrong password.");
        }
        return success;
    }

    /**
     * encriptPassword method used for encrypting password.
     * 
     * @param password
     *            the password
     * @return the string
     */
    public String encriptPassword(String password)
    {
        String newpassword = null;
        byte[] defaultBytes = password.getBytes();
        try
        {
            MessageDigest algorithm = MessageDigest.getInstance("MD5");
            algorithm.reset();
            algorithm.update(defaultBytes);
            byte messageDigest[] = algorithm.digest();

            StringBuffer hexString = new StringBuffer();
            for (int i = 0; i < messageDigest.length; i++)
            {
                hexString.append(Integer.toHexString(0xFF & messageDigest[i]));
            }
            newpassword = hexString.toString();
        }
        catch (NoSuchAlgorithmException e)
        {
            log.error("Error while encripting password {}, caused by : .", password, e);
            throw new PersistenceException(e);
        }
        return newpassword;
    }

    @Override
    public DocumentInfo findDocumentByDocumentId(int documentId)
    {
        log.info("Finding document by id.");
        DocumentInfo document = dao.findById(DocumentInfo.class, documentId);
        return document;
    }

    @Override
    public List<Employee> findEmployeeByDateOfJoining(final int noOfYears, final String company)
    {
        long currentTimeStamp = System.currentTimeMillis();
        long newTimeStamp = currentTimeStamp - (long) noOfYears * 365 * 24 * 60 * 60 * 1000;
        String query = "Select e from " + Employee.class.getSimpleName() + " e where e.company = " + company
                + " and e.timestamp <= :timestamp";
        List<Employee> subOrdinates = (List<Employee>) dao.findByQuery(query, "timestamp", newTimeStamp);
        return subOrdinates;
    }

    @Override
    public void incrementCounter(SubordinatesCounter counter)
    {
        dao.insert(counter);
    }

    private StreamedContent toStreamedContent(byte[] bytes, String fileName, String contentType)
    {
        InputStream is = new ByteArrayInputStream(bytes);

        StreamedContent file = new DefaultStreamedContent(is, contentType, fileName);

        return file;
    }
}
