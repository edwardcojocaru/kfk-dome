package com.edw.kafka.consumer.sync;

import au.com.bytecode.opencsv.CSVParser;
import com.francetelecom.dome.utils.Constants;

import java.io.IOException;

/**
 * User: eduard.cojocaru
 * Date: 12/16/13
 */
public class GnImportanceSelector implements ImportanceSelector {

    public final static int TIME_TAG_INDEX = 0;

    private CSVParser csvParser = new CSVParser(Constants.ELEMENT_SEPARATOR, Constants.CSV_QUOTECHAR);

    @Override
    public Long getImportance(String line) {

        try {
            String[] strings = csvParser.parseLine(line);
            return Long.parseLong(strings[TIME_TAG_INDEX]);
        } catch (IOException | NumberFormatException e) {
            return 0L;
        }
    }
}
