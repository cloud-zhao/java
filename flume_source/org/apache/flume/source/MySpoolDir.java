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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FileInputStream;
import java.io.RandomAccessFile;     
import java.io.FileOutputStream;     
import java.util.Properties;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.NoSuchElementException;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.Source;
import org.apache.flume.SystemClock;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.nio.charset.Charset;



public class MySpoolDir extends AbstractSource implements EventDrivenSource,
Configurable {

  private static final Logger logger = LoggerFactory.getLogger(MySpoolDir.class);

  private String rootDir;
  private long flushTimeout;
  private long scanTimeout;
  private long historyTimeout;
  private boolean setKafkaHeader;
  private SourceCounter sourceCounter;
  private ExecutorService executor;
  private Integer bufferCount;
  private long batchTimeout;
  private Charset charset;
  private Integer queueSize;
  private Future<?> future;
  private MySpoolDirCheck msdc;
  private ArrayBlockingQueue<String> mySystemOutPrint;

  @Override
  public void start() {
    logger.info("MySpoolDir source starting with");

    mySystemOutPrint = new ArrayBlockingQueue<String>(queueSize);
  
    executor = Executors.newSingleThreadExecutor();

    msdc = new MySpoolDirCheck( rootDir,
                                getChannelProcessor(),
                                sourceCounter,
                                scanTimeout,flushTimeout,historyTimeout,batchTimeout,
                                bufferCount,charset,
                                setKafkaHeader,mySystemOutPrint);

    // FIXME: Use a callback-like executor / future to signal us upon failure.
    future = executor.submit(msdc);

    /*
     * NB: This comes at the end rather than the beginning of the method because
     * it sets our state to running. We want to make sure the executor is alive
     * and well first.
     */
    sourceCounter.start();
    super.start();

    logger.debug("MySpoolDir source started");
  }

  @Override
  public void stop() {

    MySpoolDirTool.cancel(future);
    msdc.kill();
    MySpoolDirTool.shutdown(executor);

    sourceCounter.stop();
    super.stop();

    logger.debug("MySpoolDir stopped. Metrics:{}",sourceCounter);
  }

  @Override
  public void configure(Context context) {
    rootDir = context.getString("rootDir");

    Preconditions.checkState(rootDir != null,
        "The parameter rootDir must be specified");

    setKafkaHeader = context.getBoolean(MySpoolDirConfigurationConstants.CONFIG_SET_KAFKA_HEADER,
        MySpoolDirConfigurationConstants.DEFAULT_SET_KAFKA_HEADER);

    flushTimeout = context.getLong(MySpoolDirConfigurationConstants.CONFIG_FLUSH_TIME,
        MySpoolDirConfigurationConstants.DEFAULT_FLUSH_TIME);

    bufferCount = context.getInteger(MySpoolDirConfigurationConstants.CONFIG_BATCH_SIZE,
        MySpoolDirConfigurationConstants.DEFAULT_BATCH_SIZE);

    queueSize = context.getInteger(MySpoolDirConfigurationConstants.CONFIG_QUEUE_SIZE,
        MySpoolDirConfigurationConstants.DEFAULT_QUEUE_SIZE);

    batchTimeout = context.getLong(MySpoolDirConfigurationConstants.CONFIG_BATCH_TIME_OUT,
        MySpoolDirConfigurationConstants.DEFAULT_BATCH_TIME_OUT);

    historyTimeout = context.getLong(MySpoolDirConfigurationConstants.CONFIG_HISTORY_TIME,
        MySpoolDirConfigurationConstants.DEFAULT_HISTORY_TIME);

    scanTimeout = context.getLong(MySpoolDirConfigurationConstants.CONFIG_SCAN_TIME,
        MySpoolDirConfigurationConstants.DEFAULT_SCAN_TIME);

    charset = Charset.forName(context.getString(MySpoolDirConfigurationConstants.CHARSET,
        MySpoolDirConfigurationConstants.DEFAULT_CHARSET));

    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
  }



}
