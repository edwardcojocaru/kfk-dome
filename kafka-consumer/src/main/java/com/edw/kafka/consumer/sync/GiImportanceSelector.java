package com.edw.kafka.consumer.sync;

import au.com.bytecode.opencsv.CSVParser;
import com.francetelecom.dome.utils.Constants;

import java.io.IOException;

/**
 * User: eduard.cojocaru
 * Date: 12/16/13
 */
public class GiImportanceSelector implements ImportanceSelector {

    public final static int REPORT_TIME_INDEX = 15;

    private CSVParser csvParser = new CSVParser(Constants.ELEMENT_SEPARATOR, Constants.CSV_QUOTECHAR);

    @Override
    public Long getImportance(String line) {

        try {
            String[] strings = csvParser.parseLine(line);
            return Long.parseLong(strings[REPORT_TIME_INDEX]);
        } catch (IOException | NumberFormatException | ArrayIndexOutOfBoundsException e) {
            return 0L;
        }
    }
}
