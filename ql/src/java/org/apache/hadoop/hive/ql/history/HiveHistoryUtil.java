package org.apache.hadoop.hive.ql.history;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.history.HiveHistory.Listener;
import org.apache.hadoop.hive.ql.history.HiveHistory.RecordTypes;

public class HiveHistoryUtil {
  /**
  * Parses history file and calls call back functions. Also look at
  *  HiveHistoryViewer
  *
  * @param path
  * @param l
  * @throws IOException
  */
 public static void parseHiveHistory(String path, Listener l) throws IOException {
   FileInputStream fi = new FileInputStream(path);
   BufferedReader reader = new BufferedReader(new InputStreamReader(fi));
   try {
     String line = null;
     StringBuilder buf = new StringBuilder();
     while ((line = reader.readLine()) != null) {
       buf.append(line);
       // if it does not end with " then it is line continuation
       if (!line.trim().endsWith("\"")) {
         continue;
       }
       parseLine(buf.toString(), l);
       buf = new StringBuilder();
     }
   } finally {
     try {
       reader.close();
     } catch (IOException ex) {
     }
   }
 }


 private static final String KEY = "(\\w+)";
 private static final String VALUE = "[[^\"]?]+"; // anything but a " in ""
 private static final Pattern pattern = Pattern.compile(KEY + "=" + "\""
     + VALUE + "\"");

 // temp buffer for parsed dataa
 private static Map<String, String> parseBuffer = new HashMap<String, String>();

 /**
  * Parse a single line of history.
  *
  * @param line
  * @param l
  * @throws IOException
  */
 private static void parseLine(String line, Listener l) throws IOException {
   // extract the record type
   int idx = line.indexOf(' ');
   String recType = line.substring(0, idx);
   String data = line.substring(idx + 1, line.length());

   Matcher matcher = pattern.matcher(data);

   while (matcher.find()) {
     String tuple = matcher.group(0);
     String[] parts = tuple.split("=");

     parseBuffer.put(parts[0], parts[1].substring(1, parts[1].length() - 1));
   }

   l.handle(RecordTypes.valueOf(recType), parseBuffer);

   parseBuffer.clear();
 }

}
