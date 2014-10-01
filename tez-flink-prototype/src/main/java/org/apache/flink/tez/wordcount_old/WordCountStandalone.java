package org.apache.flink.tez.wordcount_old;


import org.apache.flink.tez.examples_old.WordCount;

import java.io.*;
import java.util.HashMap;
import java.util.Set;

public class WordCountStandalone {

    static String eol = System.getProperty("line.separator");

    public static void main (String [] args) {
        wordCount();
    }

    public static void tokenize () {
        try {
            FileInputStream fis = new FileInputStream(WordCount.INPUT_FILE);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
            FileOutputStream fos = new FileOutputStream("/tmp/standalone");
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos, "UTF-8"));
            String line = null;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.toLowerCase().split("\\W+");
                for (String token: tokens) {
                    if (token.length() > 0) {
                        writer.write("(" + token + ",1)");
                        writer.newLine();
                    }
                }
                writer.flush();
            }
            fis.close();
            fos.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void wordCount () {

        HashMap<String,Integer> counts = new HashMap<String, Integer>(1000);

        try {
            FileInputStream fis = new FileInputStream(WordCount.INPUT_FILE);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
            String line = null;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.toLowerCase().split("\\W+");
                for (String token: tokens) {
                    if (token.length() > 0) {
                        if (counts.containsKey(token)) {
                            int count = counts.get(token);
                            counts.put(token, count + 1);
                        } else {
                            counts.put(token, 1);
                        }
                    }
                }
            }
            fis.close();

            FileOutputStream fos = new FileOutputStream("/tmp/standalone");
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fos, "UTF-8"));
            Set<String> words = counts.keySet();
            for (String word: words) {
                writer.write("(" + word + "," + counts.get(word) + ")");
                writer.newLine();
                writer.flush();
            }
            fos.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }



    }

}
