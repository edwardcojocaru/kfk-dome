package com.francetelecom.dome.producer;

import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

/**
 * Created with IntelliJ IDEA.
 * User: ecojocaru
 * Date: 11/7/13
 * Time: 9:19 PM
 * To change this template use File | Settings | File Templates.
 */
public class WatchDirectoryTest {


    public static final String FILE_PATH = "/home/ecojocaru/workspace/kafka-dev/kfk-dome/kafka-producer/src/test/resource/iups_rab_tdr-201310141645542493_csv1.gz";

    @Test
    public void testDirectory() throws Exception {
        Path path = Paths.get(FILE_PATH);
        String type = Files.probeContentType(path);

        assertEquals("application/x-gzip", type);
    }
}
