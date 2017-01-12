/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.source;


public class MyTailConfigurationConstants {

  /**
    * Output queue size
    */
  public static final String CONFIG_QUEUE_SIZE = "queueSize";
  public static final int DEFAULT_QUEUE_SIZE = 1000;
 
  /**
    *Check file line fromat
    */
  public static final String CONFIG_LINE_CHECK = "lineCheck";
  public static final boolean DEFAULT_LINE_CHECK = false;

  /**
    * write file point flush timeout 
    */
  public static final String CONFIG_FLUSH_TIME = "flushTime";
  public static final long DEFAULT_FLUSH_TIME = 1000l;

  /**
    * file is roll type enable
    */
  public static final String CONFIG_FILE_ROLL = "fileRoll";
  public static final boolean DEFAULT_FILE_ROLL = false;

  /**
   * Number of lines to read at a time
   */
  public static final String CONFIG_BATCH_SIZE = "batchSize";
  public static final int DEFAULT_BATCH_SIZE = 20;

  /**
   * Amount of time to wait, if the buffer size was not reached, before 
   * to data is pushed downstream: : default 3000 ms
   */
  public static final String CONFIG_BATCH_TIME_OUT = "batchTimeout";
  public static final long DEFAULT_BATCH_TIME_OUT = 3000l;

  /**
   * Charset for reading input
   */
  public static final String CHARSET = "charset";
  public static final String DEFAULT_CHARSET = "UTF-8";

  /**
    * File output fromat enable
    */
  public static final String CONFIG_FILE_OUT_FROMAT_ENABLE = "fileOutFromatEnable";
  public static final boolean DEFALUT_FILE_OUT_FROMAT_ENABLE = false;

  /**
    * File output fromat regex
    */
  public static final String CONFIG_FILE_OUT_FROMAT = "fileOutFromat";
  public static final String DEFAULT_FILE_OUT_FROMAT = "(.*)";

  /**
    * File output delimit
    */
  public static final String CONFIG_FILE_OUT_DELIMIT = "fileOutDelimit";
  public static final int DEFAULT_FILE_OUT_DELIMIT = 1;
}
