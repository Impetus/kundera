package com.impetus.kundera.datakeeper.beans;

import java.util.ArrayList;
import java.util.List;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.RequestScoped;

import com.impetus.kundera.datakeeper.entities.DocumentInfo;
import com.impetus.kundera.datakeeper.service.DataKeeperService;
import com.impetus.kundera.datakeeper.utils.DataKeeperUtils;

@ManagedBean(name = "documentSearchBean")
@RequestScoped
public class DocumentSearchBean
{
    private String searchText;

    private int searchBy;

    private List<DocumentInfo> documents;

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

    public List<DocumentInfo> getDocuments()
    {
        return documents;
    }

    public void setDocuments(List<DocumentInfo> documents)
    {
        this.documents = documents;
    }

    public List<DocumentInfo> search()
    {
        documents = new ArrayList<DocumentInfo>();
        DataKeeperService service = DataKeeperUtils.getService();

        switch (SearchType.getSearchType(searchBy))
        {
        case ID:
            documents = service.findDocumentByEmployeeId(getSearchText());
            break;
        case NAME:
            documents = service.findDocumentByEmployeeName(getSearchText());
            break;
        }
        return documents;
    }
}
