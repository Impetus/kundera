package com.impetus.kundera.datakeeper.utils;

import javax.faces.context.FacesContext;
import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.impetus.kundera.datakeeper.service.DataKeeperService;

public class DataKeeperUtils
{
    public static DataKeeperService getService()
    {
        HttpSession session = (HttpSession) FacesContext.getCurrentInstance().getExternalContext().getSession(true);
        DataKeeperService datakeeper = (DataKeeperService) session.getAttribute("datakeeper");
        if (datakeeper == null)
        {
            BeanFactory beanfactory = new ClassPathXmlApplicationContext("appContext.xml");
            datakeeper = (DataKeeperService) beanfactory.getBean("datakeeper");
            session.setAttribute("datakeeper", datakeeper);
        }
        return datakeeper;
    }
}
