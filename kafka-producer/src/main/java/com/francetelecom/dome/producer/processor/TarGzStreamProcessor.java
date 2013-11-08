package com.francetelecom.dome.producer.processor;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * User: Eduard.Cojocaru
 * Date: 11/8/13
 */
public class TarGzStreamProcessor implements IStreamProcessor {

    @Override
    public void process(Path filePath) {

        try {
            final TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(new BufferedInputStream(new GzipCompressorInputStream(Files.newInputStream(filePath))));

            TarArchiveEntry tarEntry = tarArchiveInputStream.getNextTarEntry();

            while(tarEntry != null) {
                final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(tarArchiveInputStream));
                System.out.println(tarEntry.getName());
                if (!tarEntry.isDirectory()) {

                    String line;
                    while ((line = bufferedReader.readLine()) != null) {
                        System.out.println(line);
                    }
                }
                BlockingQueue<String> queue = new ArrayBlockingQueue<String>(100);

                tarEntry = tarArchiveInputStream.getNextTarEntry();
            }
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public static void main(String[] args) {
        final String s = "z:\\Dome\\Raw_Data\\gi_transaction_usage_rdr-ba4042323000.20131014165001.tgz";
        final String s1 = "z:\\Dome\\Raw_Data\\Raw_Data.tar";
        final String s2 = "z:\\Dome\\Raw_Data\\gi_transaction_usage_rdr-ba4042323000.20131014164001.tar.gz";
        final String s3 = "z:\\Dome\\Raw_Data\\test_tar\\out.tar";
        final String s4 = "z:\\Dome\\Raw_Data\\test_tar\\out.tgz";
        final String s5 = "z:\\Dome\\Raw_Data\\test_tar\\out.gz.tar";

        new TarGzStreamProcessor().process(Paths.get(s5));
    }
}
