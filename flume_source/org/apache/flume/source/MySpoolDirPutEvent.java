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
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
import java.util.UUID;


public class MySpoolDirPutEvent implements Runnable,MySpoolDirInterface{

  public MySpoolDirPutEvent(ChannelProcessor cp,SourceCounter sc,int bc,long bt,Charset cs,boolean sh,ArrayBlockingQueue<String> outq){
    this.channelProcessor=cp;
    this.sourceCounter=sc;
    this.bufferCount=bc;
    this.batchTimeout=bt;
    this.charset=cs;
    this.setHeader=sh;
    this.out=outq;
  }

  private final boolean setHeader;
  private final ChannelProcessor channelProcessor;
  private final SourceCounter sourceCounter;
  private final int bufferCount;
  private long batchTimeout;
  private final Charset charset;
  private SystemClock systemClock = new SystemClock();
  private Long lastPushToChannel = systemClock.currentTimeMillis();
  ScheduledExecutorService timedFlushService;
  ScheduledFuture<?> future;
  private boolean quit=false;
  private ArrayBlockingQueue<String> out;

  private static final Logger logger = LoggerFactory.getLogger(MySpoolDirPutEvent.class);

  @Override
  public void run() {
    String exitCode = "unknown";
    String line = null;
    final List<Event> eventList = new ArrayList<Event>();

    timedFlushService = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setNameFormat(
      "MySpoolDirPutEventTimedFlushService" +
      Thread.currentThread().getId() + "-%d").build());
    try{
      future = timedFlushService.scheduleWithFixedDelay(new Runnable() {
        @Override
        public void run() {
          try {
            synchronized (eventList) {
              if(!eventList.isEmpty() && timeout()) {
                flushEventBatch(eventList);
              }
            }
          } catch (Exception e) {
            logger.error("Exception occured when processing event batch", e);
            if(e instanceof InterruptedException) {
              Thread.currentThread().interrupt();
            }
          }
        }
      },
      batchTimeout, batchTimeout, TimeUnit.MILLISECONDS);

      while (! quit) {
          //line=out.take();
        line=out.poll(batchTimeout,TimeUnit.MILLISECONDS);
        if(line != null){
          synchronized (eventList) {
            Event e;
            if(setHeader){
              Map<String,String> header=new HashMap<String,String>();
              header.put("topic",null);
              header.put("key",getUUID());
              e=EventBuilder.withBody(line.getBytes(charset),header);
            }else{
              e=EventBuilder.withBody(line.getBytes(charset));
            }
            sourceCounter.incrementEventReceivedCount();
            eventList.add(e);
            if(eventList.size() >= bufferCount || timeout()) {
              flushEventBatch(eventList);
            }
          }
        }
      }

      synchronized (eventList) {
        if(!eventList.isEmpty()) {
          flushEventBatch(eventList);
        }
      }
    } catch (Exception e) {
      logger.error("MySpoolDirPutEvent ",e);
      if(e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
    } finally {
      exitCode = String.valueOf(kill());
    }
  }

  private void flushEventBatch(List<Event> eventList){
    channelProcessor.processEventBatch(eventList);
    sourceCounter.addToEventAcceptedCount(eventList.size());
    eventList.clear();
    lastPushToChannel = systemClock.currentTimeMillis();
  }

  private boolean timeout(){
    return (systemClock.currentTimeMillis() - lastPushToChannel) >= batchTimeout;
  }

  private String getUUID(){
    String str=UUID.randomUUID().toString();
    return str.replace("-", "");
  }

  public int kill(){
    this.quit=true;

    // Stop the Thread that flushes periodically
    MySpoolDirTool.cancel(future);
    MySpoolDirTool.shutdown(timedFlushService);
    
    return 0;
  }
}

