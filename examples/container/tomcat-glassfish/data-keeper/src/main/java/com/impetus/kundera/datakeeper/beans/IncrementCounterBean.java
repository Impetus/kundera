package com.impetus.kundera.datakeeper.beans;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.RequestScoped;
import javax.servlet.http.HttpSession;

import com.impetus.kundera.datakeeper.entities.Employee;
import com.impetus.kundera.datakeeper.entities.SubordinatesCounter;
import com.impetus.kundera.datakeeper.service.DataKeeperService;
import com.impetus.kundera.datakeeper.utils.DataKeeperConstants;
import com.impetus.kundera.datakeeper.utils.DataKeeperUtils;
import com.impetus.kundera.datakeeper.utils.FacesUtils;

@ManagedBean(name = "incrementCounterBean")
@RequestScoped
public class IncrementCounterBean
{
    private SubordinatesCounter counter = new SubordinatesCounter();

    public IncrementCounterBean()
    {

    }

    public SubordinatesCounter getCounter()
    {
        return counter;
    }

    public void setCounter(SubordinatesCounter counter)
    {
        this.counter = counter;
    }

    public String incrementCounter()
    {
        DataKeeperService service = DataKeeperUtils.getService();

        HttpSession session = FacesUtils.getSession();
        Employee employee = (Employee) session.getAttribute(DataKeeperConstants.EMPLOYEE);
        counter.setEmployeeId(employee.getEmployeeId());
        
        service.incrementCounter(counter);
        
        return "success";
    }

}
