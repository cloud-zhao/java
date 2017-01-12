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


public class MySpoolDirConfigurationConstants {

  /**
    * set Kafka sink event header
    */
  public static final String CONFIG_SET_KAFKA_HEADER = "setKafkaHeader";
  public static final boolean DEFAULT_SET_KAFKA_HEADER = true;

  /**
    * Output queue size
    */
  public static final String CONFIG_QUEUE_SIZE = "queueSize";
  public static final int DEFAULT_QUEUE_SIZE = 3000;

  /**
    * write file point flush timeout 
    */
  public static final String CONFIG_FLUSH_TIME = "flushTime";
  public static final long DEFAULT_FLUSH_TIME = 1000l;

  /**
    * scan history file timeout
    */
  public static final String CONFIG_HISTORY_TIME = "historyTime";
  public static final long DEFAULT_HISTORY_TIME = 60000l;

  /**
    * scan dir time interval
    */
  public static final String CONFIG_SCAN_TIME = "scanTime";
  public static final long DEFAULT_SCAN_TIME = 60000l;

  /**
   * Number of lines to read at a time
   */
  public static final String CONFIG_BATCH_SIZE = "batchSize";
  public static final int DEFAULT_BATCH_SIZE = 1;

  /**
   * Amount of time to wait, if the buffer size was not reached, before 
   * to data is pushed downstream: : default 3000 ms
   */
  public static final String CONFIG_BATCH_TIME_OUT = "batchTimeout";
  public static final long DEFAULT_BATCH_TIME_OUT = 1000l;

  /**
   * Charset for reading input
   */
  public static final String CHARSET = "charset";
  public static final String DEFAULT_CHARSET = "UTF-8";

}
