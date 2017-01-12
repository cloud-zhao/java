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
import java.util.Comparator;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Iterator;


public class MySpoolDirCheck implements Runnable,MySpoolDirInterface{
  private String rootDir=null;
  private File rootFile;
  private boolean quit=false,sh;
  private String dirRegex=".+_(?:interface|bussiness)$";
  private long scanTimeout,ft,cht,bt;
  private int bc;
  private Charset cs;
  private ChannelProcessor cp;
  private SourceCounter sc;
  private ArrayBlockingQueue<String> outq;
  private ExecutorService executor;
  private ScheduledExecutorService timedFlushService;
  private ConcurrentHashMap<String,MySpoolDirPath> mpHash=new ConcurrentHashMap<String,MySpoolDirPath>();
  private ConcurrentHashMap<String,Future<?>> createHash=new ConcurrentHashMap<String,Future<?>>();
  private ConcurrentHashMap<String,MySpoolDirInterface> objectHash=new ConcurrentHashMap<String,MySpoolDirInterface>();
  private ConcurrentHashMap<String,Future<?>> scheduledHash=new ConcurrentHashMap<String,Future<?>>();

  private static final Logger logger = LoggerFactory.getLogger(MySpoolDirCheck.class);

  public MySpoolDirCheck( String rd,
                          ChannelProcessor cp,
                          SourceCounter sc,
                          long st,long ft,long cht,long bt,
                          int bc,Charset cs,
                          boolean sh,ArrayBlockingQueue<String> outq){
    this.rootDir=rd;
    this.rootFile=new File(rootDir);
    this.scanTimeout=st;
    this.ft=ft;
    this.cht=cht;
    this.bt=bt;
    this.bc=bc;
    this.cs=cs;
    this.sc=sc;
    this.cp=cp;
    this.sh=sh;
    this.outq=outq;
  }

  public int kill(){
    this.quit=true;

    futureMapCancel(scheduledHash);

    MySpoolDirTool.shutdown(timedFlushService);

    for(String key : objectHash.keySet()){
      objectHash.get(key).kill();
    }

    futureMapCancel(createHash);

    MySpoolDirTool.shutdown(executor);

    return 0;
  }

  private void futureMapCancel(ConcurrentHashMap<String,Future<?>> map){
    for(String k : map.keySet()){
      MySpoolDirTool.cancel(map.get(k));
    }
  }

  public void run(){
    logger.info("Start Directory showFlieList , createRun , checkRun...");

    logger.debug("create Executors.newCachedThreadPool()");
    executor=Executors.newCachedThreadPool();

    logger.debug("create newScheduledThreadPool(5)");
    timedFlushService = Executors.newScheduledThreadPool(2,
              new ThreadFactoryBuilder().setNameFormat(
              "dirShowCreateCheck"+Thread.currentThread().getId() + "-%d").build());

    logger.debug("put showFlieList() to dirShowCreateCheck");
    scheduledHash.put("showFlieList",timedFlushService.scheduleWithFixedDelay(new Runnable(){
        public void run(){
          try{
            showFlieList();
          }catch(Exception e){
            logger.error("showFlieList() ", e);
            if(e instanceof InterruptedException) {
              Thread.currentThread().interrupt();
            }
          }
        }
      },
      1000l,scanTimeout,TimeUnit.MILLISECONDS));

    logger.debug("put createRun() to dirShowCreateCheck");
    scheduledHash.put("createRun",timedFlushService.scheduleWithFixedDelay(new Runnable(){
        public void run(){
          try{
            createRun();
          }catch(Exception e){
            logger.error("createRun() ", e);
            if(e instanceof InterruptedException) {
              Thread.currentThread().interrupt();
            }
          }
        }
      },
      5000l,scanTimeout,TimeUnit.MILLISECONDS));

    logger.debug("put createWatch() to executor");
    createWatch();

    logger.debug("put createPutEvent() to executor");
    createPutEvent();

    logger.debug("run checkRun()");
    while(!quit){
      try{
          MySpoolDirTool.mySleep(scanTimeout);
          checkRun();
        }catch(Exception e){
          logger.error("checkRun() ", e);
          if(e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }
  
  private void showFlieList(){
    for(File logDirFile : rootFile.listFiles()){
      if(logDirFile.isDirectory() && MySpoolDirTool.checkFileName(dirRegex,logDirFile.getName()) && !mpHash.containsKey(logDirFile.getAbsolutePath())){
        MySpoolDirPath mp=null;
        File[] logFiles=logDirFile.listFiles();
        Arrays.sort(logFiles,new CompratorByLastModified());
        String[] strs=logDirFile.getName().split("_");
        String fileRegex=MySpoolDirTool.join("_",Arrays.asList(Arrays.copyOfRange(strs, 0, (strs.length-1))));
        for(File logFile : logFiles){
          if(logFile.isFile() && MySpoolDirTool.checkFileName("^"+fileRegex+"-\\d+\\.log$",logFile.getName())){
            mp=new MySpoolDirPath(logDirFile.getAbsolutePath(),logFile.getName());
            break;
          }
        }
        if(mp==null){
          mp=new MySpoolDirPath(logDirFile.getAbsolutePath(),null);
        }else{
          mpHash.put(logDirFile.getAbsolutePath(),mp);
        }
      }
    }
  }

  private void createWatch(){
    MySpoolDirWatch myw=new MySpoolDirWatch(mpHash);
    Future<?> fu=executor.submit(myw);
    createHash.put("MySpoolDirWatch",fu);
    objectHash.put("MySpoolDirWatch",myw);
  }

  private void createPutEvent(){
    MySpoolDirPutEvent mype=new MySpoolDirPutEvent(cp,sc,bc,bt,cs,sh,outq);
    Future<?> fu=executor.submit(mype);
    createHash.put("MySpoolDirPutEvent",fu);
    objectHash.put("MySpoolDirPutEvent",mype);
  }

  private void createRun(){
    for(String cmp : mpHash.keySet()){
      if(!createHash.containsKey(cmp)){
        MySpoolDirRun msdr=new MySpoolDirRun(mpHash.get(cmp),ft,cht,outq,executor);
        Future<?> fu=executor.submit(msdr);
        createHash.put(cmp,fu);
        objectHash.put(cmp,msdr);
      }
    }
  }

  private void checkRun(){
    for(Iterator<String> keys=createHash.keySet().iterator();keys.hasNext();){
      String cmp=keys.next();
      Future<?> fu=createHash.get(cmp);
      if((fu != null) && fu.isDone()){
        createHash.remove(cmp);
        objectHash.remove(cmp);
      }
    }
  }

  private static class CompratorByLastModified implements Comparator<File> {  
        
        public int compare(File f1, File f2) {  
            long diff = f1.lastModified() - f2.lastModified();  
            if (diff > 0) {  
                   return -1;  
            } else if (diff == 0) {  
                   return 0;  
            } else {  
                  return 1;  
            }  
        }  
  }

}

