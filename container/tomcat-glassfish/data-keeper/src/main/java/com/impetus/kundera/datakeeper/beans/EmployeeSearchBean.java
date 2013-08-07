package com.impetus.kundera.datakeeper.beans;

import java.util.ArrayList;
import java.util.List;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.RequestScoped;
import javax.servlet.http.HttpSession;

import com.impetus.kundera.datakeeper.entities.Employee;
import com.impetus.kundera.datakeeper.service.DataKeeperService;
import com.impetus.kundera.datakeeper.utils.DataKeeperConstants;
import com.impetus.kundera.datakeeper.utils.DataKeeperUtils;
import com.impetus.kundera.datakeeper.utils.FacesUtils;

@ManagedBean(name = "subordinateSearchBean")
@RequestScoped
public class EmployeeSearchBean
{
    private List<Employee> subordinates = new ArrayList<Employee>();

    private int managerId;

    private int noOfYears;

    private String searchText;

    private int searchBy;

    public String getSearchText()
    {
        return searchText;
    }

    public void setSearchText(String searchText)
    {
        this.searchText = searchText;
    }

    public int getSearchBy()
    {
        return searchBy;
    }

    public void setSearchBy(int searchBy)
    {
        this.searchBy = searchBy;
    }

    /**
     * @return the managerName
     */
    public int getManagerId()
    {
        return managerId;
    }

    /**
     * @param managerName
     *            the managerName to set
     */
    public void setManagerId(int managerId)
    {
        this.managerId = managerId;
    }

    public int getNoOfYears()
    {
        return noOfYears;
    }

    public void setNoOfYears(int noOfYears)
    {
        this.noOfYears = noOfYears;
    }

    public List<Employee> getSubordinates()
    {
        return subordinates;
    }

    public void setSubordinates(List<Employee> subordinates)
    {
        this.subordinates = subordinates;
    }

    public String searchSubordinates()
    {
        DataKeeperService service = DataKeeperUtils.getService();
        setManagerId(Integer.parseInt(FacesUtils.getRequest().getParameter("managerId")));
        List<Employee> subordinates = service.findSubOrdinates(getManagerId());
        if (subordinates != null)
        {
            this.subordinates = subordinates;
        }
        return "foundEmployess";
    }

    public String searchEmployee()
    {
        DataKeeperService service = DataKeeperUtils.getService();
        Employee employee = null;
        switch (SearchType.getSearchType(searchBy))
        {
        case ID:
            employee = service.findEmployee(Integer.parseInt(getSearchText()));
            break;
        case NAME:
            employee = service.findEmployeeByName(getSearchText());
            break;
        }
        if (employee != null)
        {
            List<Employee> employees = new ArrayList<Employee>();
            employees.add(employee);
            this.subordinates = employees;
        }
        return "foundEmployess";
    }

    public String searchEmployeeByYearOfComplition()
    {
        DataKeeperService service = DataKeeperUtils.getService();
        HttpSession session = FacesUtils.getSession();
        Employee employee = (Employee) session.getAttribute(DataKeeperConstants.EMPLOYEE);
        List<Employee> employees = service.findEmployeeByDateOfJoining(getNoOfYears(), employee.getCompany());
        if (employees != null)
        {
            this.subordinates = employees;
        }
        return "foundEmployess";
    }

}
