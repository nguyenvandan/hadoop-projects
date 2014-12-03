package com.lab.hadoop.twitter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

/**
 * Example application that load raw JSON forms from statuses/ directory and dump status texts.
 *
 * @author Yusuke Yamamoto - yusuke at mac.com
 */
public final class LoadRawJSON {
    /**
     * Usage: java twitter4j.examples.json.LoadRawJSON
     *
     * @param args String[]
     */
    public static void main(String[] args) {
        try {
            File[] files = new File("statuses").listFiles(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return name.endsWith(".json");
                }
            });
            for (File file : files) {
                String rawJSON = readFirstLine(file);
                Status status = TwitterObjectFactory.createStatus(rawJSON);
                System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
            }
            System.exit(0);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.out.println("Failed to store tweets: " + ioe.getMessage());
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to get timeline: " + te.getMessage());
            System.exit(-1);
        }
    }

    private static String readFirstLine(File fileName) throws IOException {
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        try {
            fis = new FileInputStream(fileName);
            isr = new InputStreamReader(fis, "UTF-8");
            br = new BufferedReader(isr);
            return br.readLine();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException ignore) {
                }
            }
            if (isr != null) {
                try {
                    isr.close();
                } catch (IOException ignore) {
                }
            }
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException ignore) {
                }
            }
        }
    }
}