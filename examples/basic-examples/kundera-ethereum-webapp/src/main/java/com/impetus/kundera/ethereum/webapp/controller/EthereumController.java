package com.impetus.kundera.ethereum.webapp.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

import com.impetus.kundera.ethereum.webapp.dao.EthereumDao;

@Controller
public class EthereumController
{
    @Autowired
    EthereumDao ethereumDao;

    @RequestMapping(value = "/", method = RequestMethod.GET)
    public ModelAndView root()
    {
        ModelAndView model = new ModelAndView();
        model.setViewName("home");
        return model;
    }

    @RequestMapping(value = "/home", method = RequestMethod.GET)
    public ModelAndView home()
    {
        ModelAndView model = new ModelAndView();
        model.setViewName("home");
        return model;
    }

    @RequestMapping(value = "/import", method = RequestMethod.POST)
    public ModelAndView importBlocks(@RequestParam("from_block") String from, @RequestParam("to_block") String to)
    {
        ModelAndView model = new ModelAndView();

        ethereumDao.importBlocks(Long.parseLong(from), Long.parseLong(to));
        model.addObject("importStatus", "Import of blocks from " + from + " to " + to + " is successful");
        model.setViewName("home");
        return model;
    }

    @RequestMapping(value = "/queryresult", method = RequestMethod.POST)
    public ModelAndView queryBlocks(@RequestParam String query)
    {
        ModelAndView model = new ModelAndView();
        List res = ethereumDao.runJPAQuery(query);
        model.addObject("result", res);
        model.setViewName("queryresult");
        return model;
    }

}
