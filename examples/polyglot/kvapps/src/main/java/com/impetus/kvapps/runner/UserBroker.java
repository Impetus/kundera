/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.kvapps.runner;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import com.impetus.kvapps.entities.PersonalDetail;
import com.impetus.kvapps.entities.Tweets;
import com.impetus.kvapps.entities.User;
import com.impetus.kvapps.entities.Video;

/**
 * @author impetus
 *   
 */
public class UserBroker
{

    private Sheet m_sheet;

    private int m_iNbRows;

    private int m_iCurrentRow = 0;

    private static final String JAVA_TOSTRING = "EEE MMM dd HH:mm:ss zzz yyyy";

    public UserBroker(Sheet sheet)
    {
        m_sheet = sheet;
        m_iNbRows = sheet.getPhysicalNumberOfRows();
    }

    /*
     * Returns the contents of an Excel row in the form of a String array.
     * 
     * @see com.ibm.ccd.common.parsing.Parser#splitLine()
     */
    public User addUser(Set<User> users) throws Exception
    {

        if (m_iCurrentRow == m_iNbRows)
            return null;

        Row row = m_sheet.getRow(m_iCurrentRow);
        if (row == null)
        {
            return null;
        }
        else
        {
            // extract fields from excel sheet
            String userName = extractCellData(row, 0);
            String tweetBody = extractCellData(row, 1);
            String videoId = extractCellData(row, 2);
            String videoName = extractCellData(row, 3);

            User user = null;
            boolean present = false;
            
            //iterate over fields.
            for (Iterator iterator = users.iterator(); iterator.hasNext();)
            {
                user = (User) iterator.next();
                if (user.getFirstName().equals(userName))
                {
                    present = true;
                    break;
                }
            }

            if (!present)
            {
                user = addUser(userName);
            }

            Tweets tweet = addTweets(user, tweetBody, new Date(), m_iCurrentRow + "");
            assignVideosToTweet(tweet, videoId, videoName);

            m_iCurrentRow++;

            return user;
        }

    }

    /*
     *  Populates user instance for provided userName attribute.
     */
    private User addUser(String userName)
    {
        User user = new User();
        PersonalDetail personalDetail = new PersonalDetail(userName, userName + "Password", "Single");
        user.setUserId(userName);
        user.setEmailId(userName + "@" + userName + ".com");
        user.setFirstName(userName);
        user.setLastName(userName);
        user.setTweets(new HashSet<Tweets>());
        user.setPersonalDetail(personalDetail);
        return user;
    }

    /*
     *  Add user's tweets to provided user object. 
     */
    private static Tweets addTweets(User user, String body, Date tweetDate, String tweetId)
    {
        Tweets tweet = new Tweets();
        tweet.setTweetId(tweetId);
        tweet.setBody(body);
        tweet.setTweetDate(tweetDate);
        user.getTweets().add(tweet);
        return tweet;
    }

    /*
     *   Upload a collection of videos for a specific tweet.  
     */
    private static void assignVideosToTweet(Tweets tweet, String videoId, String videoName)
    {
        Set<Video> videos = new HashSet<Video>();
        Video video = new Video();
        video.setVideoId(videoId);
        video.setVideoName(videoName);
        videos.add(video);
        tweet.setVideos(videos);
    }

    /*
     * Read a cell for specific column.
     */
    private String extractCellData(Row row, int iCurrent) throws Exception
    {
        Cell cell = (Cell) row.getCell(iCurrent);
        if (cell == null)
        {
            return "";
        }
        else
        {
            switch (cell.getCellType())
            {
            case HSSFCell.CELL_TYPE_NUMERIC:
                double value = cell.getNumericCellValue();
                if (HSSFDateUtil.isCellDateFormatted(cell))

                {
                    if (HSSFDateUtil.isValidExcelDate(value))
                    {
                        Date date = HSSFDateUtil.getJavaDate(value);
                        SimpleDateFormat dateFormat = new SimpleDateFormat(JAVA_TOSTRING);
                        return dateFormat.format(date);
                    }
                    else
                    {
                        throw new Exception("Invalid Date value found at row number " + row.getRowNum()
                                + " and column number " + cell.getColumnIndex());
                    }
                }
                else
                {
                    return value + "";
                }
            case HSSFCell.CELL_TYPE_STRING:
                return cell.getStringCellValue();
            case HSSFCell.CELL_TYPE_BLANK:
                return null;
            default:
                return null;
            }
        }
    }

    
    /**
     * Reads and populate a collection of {@link User} from excel file available at dataFilePath.
     * 
     * @param dataFilePath
     * @return  collection of users.
     */
    static Set<User> brokeUserList(final String dataFilePath)
    {
        Set<User> users = new HashSet<User>();
        Workbook workBook = null;
        File file = new File(dataFilePath);
        InputStream excelDocumentStream = null;
        try
        {
            excelDocumentStream = new FileInputStream(file);
            POIFSFileSystem fsPOI = new POIFSFileSystem(new BufferedInputStream(excelDocumentStream));
            workBook = new HSSFWorkbook(fsPOI);
            UserBroker parser = new UserBroker(workBook.getSheetAt(0));
            User user;
            while ((user = parser.addUser(users)) != null)
            {
                users.add(user);
            }
            excelDocumentStream.close();

        }
        catch (Exception e)
        {
          throw new RuntimeException("Error while processing brokeUserList(), Caused by :",e);
        }
        
        return users;
    }

}