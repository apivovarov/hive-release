/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton.tool;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestTempletonUtils {
  public static final String[] CONTROLLER_LINES = {
    "2011-12-15 18:12:21,758 [main] INFO  org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher - More information at: http://localhost:50030/jobdetails.jsp?jobid=job_201112140012_0047",
    "2011-12-15 18:12:46,907 [main] INFO  org.apache.pig.tools.pigstats.SimplePigStats - Script Statistics: "
  };
  public static final String testDataDir = System.getProperty("test.tmp.dir");
  File tmpFile;
  File usrFile;

  @Before
  public void setup() {
    try {
      tmpFile = new File(testDataDir, "tmp");
      tmpFile.createNewFile();
      usrFile = new File(testDataDir, "usr");
      usrFile.createNewFile();
    } catch (IOException ex) {
      Assert.fail(ex.getMessage());
    }
  }

  @After
  public void tearDown() {
    tmpFile.delete();
    usrFile.delete();
  }

  @Test
  public void testIssetString() {
    Assert.assertFalse(TempletonUtils.isset((String)null));
    Assert.assertFalse(TempletonUtils.isset(""));
    Assert.assertTrue(TempletonUtils.isset("hello"));
  }

  @Test
  public void testIssetTArray() {
    Assert.assertFalse(TempletonUtils.isset((Long[]) null));
    Assert.assertFalse(TempletonUtils.isset(new String[0]));
    String[] parts = new String("hello.world").split("\\.");
    Assert.assertTrue(TempletonUtils.isset(parts));
  }

  @Test
  public void testPrintTaggedJobID() {
    //JobID job = new JobID();
    // TODO -- capture System.out?
  }


  @Test
  public void testExtractPercentComplete() {
    Assert.assertNull(TempletonUtils.extractPercentComplete("fred"));
    for (String line : CONTROLLER_LINES) {
      Assert.assertNull(TempletonUtils.extractPercentComplete(line));
    }

    String fifty = "2011-12-15 18:12:36,333 [main] INFO  org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher - 50% complete";
    Assert.assertEquals("50% complete", TempletonUtils.extractPercentComplete(fifty));

    // MapRed jar run
    String sixty_six = "13/06/02 20:57:32 INFO mapred.JobClient:  map 66% reduce 0%";
    Assert.assertEquals("map 66% reduce 0%", TempletonUtils.extractPercentComplete(sixty_six));

    // Hive job run
    String hundred = "2013-06-02 20:52:57,331 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 13.31 sec";
    Assert.assertEquals("map 100% reduce 100%", TempletonUtils.extractPercentComplete(hundred));
  }

  @Test
  public void testExtractChildJobId() {
    Assert.assertNull(TempletonUtils.extractChildJobId("fred"));

    // MapRed jar run
    String jar_job = "12/01/02 20:57:11 INFO mapred.JobClient: Running job: job_201201221934_0798";
    Assert.assertEquals("job_201201221934_0798", TempletonUtils.extractChildJobId(jar_job));

    String hive_job = "Starting Job = job_201305221934_0798, Tracking URL = http://localhost:50030/jobdetails.jsp?jobid=job_201201221934_0798";
    Assert.assertEquals("job_201201221934_0798", TempletonUtils.extractChildJobId(hive_job));

  }

  @Test
  public void testEncodeArray() {
    Assert.assertEquals(null, TempletonUtils.encodeArray((String []) null));
    String[] tmp = new String[0];
    Assert.assertTrue(TempletonUtils.encodeArray(new String[0]).length() == 0);
    tmp = new String[3];
    tmp[0] = "fred";
    tmp[1] = null;
    tmp[2] = "peter,lisa,, barney";
    Assert.assertEquals("fred,,peter" +
           StringUtils.ESCAPE_CHAR + ",lisa" + StringUtils.ESCAPE_CHAR + "," +
           StringUtils.ESCAPE_CHAR + ", barney",
           TempletonUtils.encodeArray(tmp));
  }

  @Test
  public void testDecodeArray() {
    Assert.assertTrue(TempletonUtils.encodeArray((String[]) null) == null);
    String[] tmp = new String[3];
    tmp[0] = "fred";
    tmp[1] = null;
    tmp[2] = "peter,lisa,, barney";
    String[] tmp2 = TempletonUtils.decodeArray(TempletonUtils.encodeArray(tmp));
    try {
      for (int i=0; i< tmp.length; i++) {
        Assert.assertEquals((String) tmp[i], (String)tmp2[i]);
      }
    } catch (Exception e) {
      Assert.fail("Arrays were not equal" + e.getMessage());
    }
  }

  @Test
  public void testHadoopFsPath() {
    try {
      TempletonUtils.hadoopFsPath(null, null, null);
      TempletonUtils.hadoopFsPath(tmpFile.toURI().toString(), null, null);
      TempletonUtils.hadoopFsPath(tmpFile.toURI().toString(), new Configuration(), null);
    } catch (FileNotFoundException e) {
      Assert.fail("Couldn't find " + tmpFile.toURI().toString());
    } catch (Exception e) {
      // This is our problem -- it means the configuration was wrong.
      e.printStackTrace();
    }
    try {
      TempletonUtils.hadoopFsPath("/scoobydoo/teddybear",
                    new Configuration(), null);
      Assert.fail("Should not have found /scoobydoo/teddybear");
    } catch (FileNotFoundException e) {
      // Should go here.
    } catch (Exception e) {
      // This is our problem -- it means the configuration was wrong.
      e.printStackTrace();
    }
    try {
      TempletonUtils.hadoopFsPath("a", new Configuration(), "teddybear");
      Assert.fail("Should not have found /user/teddybear/a");
    } catch (FileNotFoundException e) {
      Assert.assertTrue(e.getMessage().contains("/user/teddybear/a"));
    } catch (Exception e) {
      // This is our problem -- it means the configuration was wrong.
      e.printStackTrace();
      Assert.fail("Get wrong exception: " + e.getMessage());
    }
  }

  @Test
  public void testHadoopFsFilename() {
    try {
      String tmpFileName1 = "/tmp/testHadoopFsListAsArray1";
      String tmpFileName2 = "/tmp/testHadoopFsListAsArray2";
      File tmpFile1 = new File(tmpFileName1);
      File tmpFile2 = new File(tmpFileName2);
      tmpFile1.createNewFile();
      tmpFile2.createNewFile();
      Assert.assertEquals(null, TempletonUtils.hadoopFsFilename(null, null, null));
      Assert.assertEquals(null,
        TempletonUtils.hadoopFsFilename(tmpFile.toURI().toString(), null, null));
      Assert.assertEquals(tmpFile.toURI().toString(),
        TempletonUtils.hadoopFsFilename(tmpFile.toURI().toString(),
          new Configuration(),
          null));
    } catch (FileNotFoundException e) {
      Assert.fail("Couldn't find name for /tmp");
      Assert.fail("Couldn't find name for " + tmpFile.toURI().toString());
    } catch (Exception e) {
      // Something else is wrong
      e.printStackTrace();
    }
    try {
      TempletonUtils.hadoopFsFilename("/scoobydoo/teddybear",
                      new Configuration(), null);
      Assert.fail("Should not have found /scoobydoo/teddybear");
    } catch (FileNotFoundException e) {
      // Should go here.
    } catch (Exception e) {
      // Something else is wrong.
      e.printStackTrace();
    }
  }

  @Test
  public void testHadoopFsListAsArray() {
    try {
      String tmpFileName1 = "/tmp/testHadoopFsListAsArray1";
      String tmpFileName2 = "/tmp/testHadoopFsListAsArray2";
      File tmpFile1 = new File(tmpFileName1);
      File tmpFile2 = new File(tmpFileName2);
      tmpFile1.createNewFile();
      tmpFile2.createNewFile();
      Assert.assertTrue(TempletonUtils.hadoopFsListAsArray(null, null, null) == null);
      Assert.assertTrue(TempletonUtils.hadoopFsListAsArray(tmpFileName1 + "," + tmpFileName2,
        null, null) == null);
      String[] tmp2
        = TempletonUtils.hadoopFsListAsArray(tmpFileName1 + "," + tmpFileName2,
                                             new Configuration(), null);
      Assert.assertEquals("file:" + tmpFileName1, tmp2[0]);
      Assert.assertEquals("file:" + tmpFileName2, tmp2[1]);
      tmpFile1.delete();
      tmpFile2.delete();
    } catch (FileNotFoundException e) {
      Assert.fail("Couldn't find name for " + tmpFile.toURI().toString());
    } catch (Exception e) {
      // Something else is wrong
      e.printStackTrace();
    }
    try {
      TempletonUtils.hadoopFsListAsArray("/scoobydoo/teddybear,joe",
                         new Configuration(),
                         null);
      Assert.fail("Should not have found /scoobydoo/teddybear");
    } catch (FileNotFoundException e) {
      // Should go here.
    } catch (Exception e) {
      // Something else is wrong.
      e.printStackTrace();
    }
  }

  @Test
  public void testHadoopFsListAsString() {
    try {
      String tmpFileName1 = "/tmp/testHadoopFsListAsString1";
      String tmpFileName2 = "/tmp/testHadoopFsListAsString2";
      File tmpFile1 = new File(tmpFileName1);
      File tmpFile2 = new File(tmpFileName2);
      tmpFile1.createNewFile();
      tmpFile2.createNewFile();
      Assert.assertTrue(TempletonUtils.hadoopFsListAsString(null, null, null) == null);
      Assert.assertTrue(TempletonUtils.hadoopFsListAsString("/tmp,/usr",
        null, null) == null);
      Assert.assertEquals("file:" + tmpFileName1 + ",file:" + tmpFileName2,
        TempletonUtils.hadoopFsListAsString
        (tmpFileName1 + "," + tmpFileName2, new Configuration(), null));
    } catch (FileNotFoundException e) {
      Assert.fail("Couldn't find name for " + tmpFile.toURI().toString());
    } catch (Exception e) {
      // Something else is wrong
      e.printStackTrace();
    }
    try {
      TempletonUtils.hadoopFsListAsString("/scoobydoo/teddybear,joe",
                        new Configuration(),
                        null);
      Assert.fail("Should not have found /scoobydoo/teddybear");
    } catch (FileNotFoundException e) {
      // Should go here.
    } catch (Exception e) {
      // Something else is wrong.
      e.printStackTrace();
    }
  }

  @Test
  public void testConstructingUserHomeDirectory() throws Exception {
    String[] sources = new String[] { "output+", "/user/hadoop/output",
      "hdfs://container", "hdfs://container/", "hdfs://container/path",
      "output#link", "hdfs://cointaner/output#link",
      "hdfs://container@acc/test" };
    String[] expectedResults = new String[] { "/user/webhcat/output+",
      "/user/hadoop/output", "hdfs://container/user/webhcat",
      "hdfs://container/", "hdfs://container/path",
      "/user/webhcat/output#link", "hdfs://cointaner/output#link",
      "hdfs://container@acc/test" };
    for (int i = 0; i < sources.length; i++) {
      String source = sources[i];
      String expectedResult = expectedResults[i];
      String result = TempletonUtils.addUserHomeDirectoryIfApplicable(source,
          "webhcat");
      Assert.assertEquals(result, expectedResult);
    }

    String badUri = "c:\\some\\path";
    try {
      TempletonUtils.addUserHomeDirectoryIfApplicable(badUri, "webhcat");
      Assert.fail("addUserHomeDirectoryIfApplicable should fail for bad URI: "
          + badUri);
    } catch (URISyntaxException ex) {
    }
  }

  @Test
  public void testPropertiesParsing() throws Exception {
    String[] props = {"hive.metastore.uris=thrift://localhost:9933\\,thrift://127.0.0.1:9933",
      "hive.metastore.sasl.enabled=false",
    "hive.some.fake.path=C:\\foo\\bar.txt\\"};
    StringBuilder input = new StringBuilder();
    for(String prop : props) {
      if(input.length() > 0) {
        input.append(',');
      }
      input.append(prop);
    }
    String[] newProps = StringUtils.split(input.toString());
    for(int i = 0; i < newProps.length; i++) {
      Assert.assertEquals("Pre/post split values don't match",
        StringUtils.unEscapeString(props[i]), StringUtils.unEscapeString(newProps[i]));
    }
  }
}
