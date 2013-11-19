package com.impetus.kundera.datakeeper.utils;

import javax.faces.context.FacesContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

/**
 * The Class FacesUtils has method for getting session variables and request
 * variables.
 */
public class FacesUtils
{

    /**
     * getSession method used for getting session variable.
     * 
     * @return the session
     */
    public static HttpSession getSession()
    {
        return (HttpSession) FacesContext.getCurrentInstance().getExternalContext().getSession(true);
    }

    /**
     * getRequest method used for getting request variable.
     * 
     * @return the request
     */
    public static HttpServletRequest getRequest()
    {
        return (HttpServletRequest) FacesContext.getCurrentInstance().getExternalContext().getRequest();
    }
}
