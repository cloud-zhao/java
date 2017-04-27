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


public class MySpoolDirTool{

  private static final Logger logger = LoggerFactory.getLogger(MySpoolDirTool.class);

  public static void mySleep(long ms){
    try{
      Thread.currentThread().sleep(ms);
    }catch(InterruptedException e){
      logger.debug("Sleep {}ms InterruptedException ",ms,e);
    }
  }

  public static boolean checkFileName(String regex,String fileName){
      Pattern p=Pattern.compile(regex);
      Matcher matcher=p.matcher(fileName);
      boolean res=matcher.matches();
      logger.debug("checkFileName Regex: "+regex+" FileName: "+fileName+" Relust: "+res);
      return res;
  }

  public static String join(String flag,List<String> list){
    boolean f=false;
    String str="";
    for(String s : list){
      if(f)
        str=str+flag;
      else
        f=true;
      str=str+s;
    }
    return str;
  }

  public static void shutdown(ExecutorService es){
    if(es != null){
      es.shutdown();
      while (!es.isTerminated()) {
        logger.debug("Waiting for exec executor service to stop");
        try {
          es.awaitTermination(500, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          logger.debug("Interrupted while waiting for exec executor service "
            + "to stop. Just exiting.");
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  public static void cancel(Future<?> fu){
    if(fu != null){
      fu.cancel(true);
    }
  }

  public static void writeSingle(String f,String line,boolean append){
    try{
      FileWriter fw = new FileWriter(f,append);
      fw.write(line);
      fw.flush();
      fw.close();
    }catch(IOException e){
      logger.error("file "+f+" writer failed",e);
    }
  }

  public static String fromatOut(String regex,String flag,String input){
    Pattern p=Pattern.compile(regex);
    Matcher m=p.matcher(input);
    String output=null;
    List<String> outputs=new ArrayList<String>();
    if(m.find()){
      for(int i=1;i<=m.groupCount();i++){
        outputs.add(m.group(i));
      }
      output=join(flag,outputs);
    }
    return output;
  }

  public  static void MyReaderClose(Object re){
    if(re != null){
      try{
        if(re instanceof RandomAccessFile){
          ((RandomAccessFile)re).close();
        }else if(re instanceof BufferedReader){
          ((BufferedReader)re).close();
        }
      }catch(IOException e1){
        logger.debug("BufferedReader close IOException ",e1);
      }
    }
  }
}


 
