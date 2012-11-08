/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.benchmark;

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import jxl.CellView;
import jxl.Workbook;
import jxl.WorkbookSettings;
import jxl.format.UnderlineStyle;
import jxl.write.Label;
import jxl.write.Number;
import jxl.write.WritableCellFormat;
import jxl.write.WritableFont;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;
import jxl.write.WriteException;
import jxl.write.biff.RowsExceededException;

/**
 * WriteToExcelFile class responsible create a excel file and write content into
 * it.
 * 
 * @author Kuldeep Mishra
 * 
 */
public class WriteToExcelFile
{
    private WritableCellFormat timesBoldUnderline;

    private WritableCellFormat times;

    private String inputFile;

    private static final Log log = LogFactory.getLog(WriteToExcelFile.class);

    public void setOutputFile(String inputFile)
    {
        this.inputFile = inputFile;
    }

    /**
     * write into excel file.
     * 
     * @param map
     * @param testSummatyField
     * @throws IOException
     * @throws WriteException
     */
    public void write(Map<String, TestResult> map, String[] testSummatyField) throws IOException, WriteException
    {
        log.info("Writing file : " + inputFile);
        File file = new File(inputFile);
        WorkbookSettings wbSettings = new WorkbookSettings();

        wbSettings.setLocale(new Locale("en", "EN"));

        WritableWorkbook workbook = Workbook.createWorkbook(file, wbSettings);
        workbook.createSheet("PerformanceBenchmark", 0);
        WritableSheet excelSheet = workbook.getSheet(0);
        createLabel(excelSheet, testSummatyField);
        createContent(excelSheet, map);

        workbook.write();
        workbook.close();
    }

    /**
     * create column
     * 
     * @param sheet
     * @param testSummatyField
     * @throws WriteException
     */
    private void createLabel(WritableSheet sheet, String[] testSummatyField) throws WriteException
    {
        // Lets create a times font
        WritableFont times10pt = new WritableFont(WritableFont.TIMES, 10);
        // Define the cell format
        times = new WritableCellFormat(times10pt);
        // Lets automatically wrap the cells
        times.setWrap(true);

        // Create create a bold font with unterlines
        WritableFont times10ptBoldUnderline = new WritableFont(WritableFont.TIMES, 10, WritableFont.BOLD, false,
                UnderlineStyle.SINGLE);
        timesBoldUnderline = new WritableCellFormat(times10ptBoldUnderline);
        // Lets automatically wrap the cells
        timesBoldUnderline.setWrap(true);

        CellView cv = new CellView();
        cv.setFormat(times);
        cv.setFormat(timesBoldUnderline);
        // cv.setSize(111);

        // Write a few headers
        addCaption(sheet, 0, 0, testSummatyField[0]);
        addCaption(sheet, 1, 0, testSummatyField[1]);
        addCaption(sheet, 2, 0, testSummatyField[2]);
        addCaption(sheet, 3, 0, testSummatyField[3]);
        addCaption(sheet, 4, 0, testSummatyField[4]);

    }

    /**
     * put content
     * 
     * @param sheet
     * @param map
     * @throws WriteException
     * @throws RowsExceededException
     */
    private void createContent(WritableSheet sheet, Map<String, TestResult> map) throws WriteException,
            RowsExceededException
    {
        int i = 0;
        int j = 1;
        for (String s : map.keySet())
        {
            addLabel(sheet, i++, j, s);
            TestResult data = map.get(s);
            if (data != null)
            {
                addNumber(sheet, i++, j, data.getTotalTimeTaken());
                addNumber(sheet, i++, j, data.getNoOfInvocation());
                addNumber(sheet, i++, j, data.getAvgTime());

                addNumber(sheet, i++, j, data.getDelta() != null ? data.getDelta() : 0);
            }
            i = 0;
            j++;
        }
    }

    private void addCaption(WritableSheet sheet, int column, int row, String s) throws RowsExceededException,
            WriteException
    {
        Label label;
        label = new Label(column, row, s, timesBoldUnderline);
        sheet.addCell(label);
    }

    private void addNumber(WritableSheet sheet, int column, int row, Integer integer) throws WriteException,
            RowsExceededException
    {
        Number number;
        number = new Number(column, row, integer, times);
        sheet.addCell(number);
    }

    private void addNumber(WritableSheet sheet, int column, int row, Double doubleNumber) throws WriteException,
            RowsExceededException
    {
        Number number;
        number = new Number(column, row, doubleNumber, times);
        sheet.addCell(number);
    }

    private void addLabel(WritableSheet sheet, int column, int row, Object s) throws WriteException,
            RowsExceededException
    {
        Label label;
        label = new Label(column, row, s.toString(), times);
        sheet.addCell(label);
    }

    /**
     * Create a excel file and write into it.
     * 
     * @param map
     * @param fileName
     * @param testSummatyField
     * @throws WriteException
     * @throws IOException
     */
    public static void writeInXlsFile(Map<String, TestResult> map, String fileName, String[] testSummatyField)
            throws WriteException, IOException
    {
        WriteToExcelFile excelFile = new WriteToExcelFile();
        excelFile.setOutputFile(fileName);

        excelFile.write(map, testSummatyField);
        log.info("Please check the result file under " + testSummatyField);
    }
}