package com.impetus.kundera.datakeeper.beans;

import java.util.Date;

import javax.faces.application.FacesMessage;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.RequestScoped;
import javax.faces.context.FacesContext;
import javax.servlet.http.HttpSession;

import org.primefaces.event.FileUploadEvent;
import org.primefaces.model.UploadedFile;

import com.impetus.kundera.datakeeper.entities.DocumentInfo;
import com.impetus.kundera.datakeeper.entities.Employee;
import com.impetus.kundera.datakeeper.service.DataKeeperService;
import com.impetus.kundera.datakeeper.utils.DataKeeperConstants;
import com.impetus.kundera.datakeeper.utils.DataKeeperUtils;
import com.impetus.kundera.datakeeper.utils.FacesUtils;

@ManagedBean(name = "documentUploadBean")
@RequestScoped
public class DocumentUploadBean
{
    public DocumentUploadBean()
    {
    }

    public String handleFileUpload(FileUploadEvent event)
    {
        HttpSession session = FacesUtils.getSession();
        Employee employee = (Employee) session.getAttribute(DataKeeperConstants.EMPLOYEE);

        DataKeeperService service = DataKeeperUtils.getService();

        UploadedFile file = event.getFile();

        if (file != null)
        {
            DocumentInfo dataInfo = new DocumentInfo();
            dataInfo.setData(file.getContents());
            dataInfo.setDocumentName(file.getFileName());
            dataInfo.setSize(file.getSize());
            dataInfo.setOwnerName(employee.getEmployeeName());
            dataInfo.setOwnerId(employee.getEmployeeId());
            dataInfo.setUplodedDate(new Date());

            service.insertData(dataInfo);

            FacesMessage msg = new FacesMessage("Succesful", file.getFileName() + " is uploaded.");
            FacesContext.getCurrentInstance().addMessage(null, msg);

            return "successfully uploaded";
        }
        return "uploading fail";
    }
}
