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


public class MySpoolDirRun implements Runnable,MySpoolDirInterface{
  private MySpoolDirPath mp=null;
  private ArrayBlockingQueue<String> outq=null;
  private ExecutorService exec=null;
  private long flushTimeout;
  private long checkHistoryTimeout;
  private boolean quit=false;
  private String checkPointFileName=null;
  private RandomAccessFile reader = null;
  private ScheduledExecutorService timedFlushService;
  private ScheduledFuture[] future=new ScheduledFuture[2];

  private static final Logger logger = LoggerFactory.getLogger(MySpoolDirRun.class);

  public MySpoolDirRun( MySpoolDirPath mp,
                        long ft,long cht,
                        ArrayBlockingQueue<String> out,ExecutorService e){
    this.mp=mp;
    this.flushTimeout=ft;
    this.checkHistoryTimeout=cht;
    this.outq=out;
    this.exec=e;
    this.checkPointFileName=mp.get_dir()+"/"+".check_point_file";
  }

  private void checkPoint(){
    logger.debug("checkPoint file ...");

    File checkPointFile = new File(checkPointFileName);
    BufferedReader creader = null;
    if(checkPointFile.canRead()){
      logger.debug("checkPoint read file...");
      try{
        creader = new BufferedReader(new FileReader(checkPointFile));
        String line = creader.readLine();
        if(line != null){
          String[] file_info=line.split("\\|");
          mp.set_file(file_info[0]);
          try{
            reader = new RandomAccessFile(mp.get_dir()+"/"+file_info[0],"r");
            reader.seek(Long.parseLong(file_info[1]));
          }catch(IOException e2){
            logger.error("checkPoint new RandomAccessFile error",e2);
            MySpoolDirTool.MyReaderClose(reader);
            reader = null;
          }
        }
      }catch(IOException e1){
        logger.debug("checkPointFileName not reader line",e1);
      }finally{
        MySpoolDirTool.MyReaderClose(creader);
      }
    }else{
      logger.debug("checkPoint wait file...");
      for(;;){
        if(mp.get_file() == null){
            String newfile=mp.get_newfile();
            if(newfile == null){
              logger.debug("wait new MySpoolDirPath set file");
              MySpoolDirTool.mySleep(3000);
            }else{
              mp.set_file(newfile);
              break;
            }
        }else{
          break;
        }
      }
    }
  }

  private void flushCheckPoint(){
    String point = null;
    try{
      point = Long.toString(reader.getFilePointer());
    }catch(IOException e){
      logger.error("get file pointer Exception ",e);
      point = Long.toString(0l); 
    }
    String timeStamp =  Integer.toString((int)(System.currentTimeMillis()/1000));
    MySpoolDirTool.writeSingle(checkPointFileName,mp.get_file()+"|"+point+"|"+timeStamp+"\n",false);
  }

  public int kill(){
    this.quit=true;

    for(int i=0;i<future.length;i++){
      MySpoolDirTool.cancel(future[i]);
    }

    MySpoolDirTool.shutdown(timedFlushService);

    return 0;
  }

  private void readHistroyFile(){
    String history_file=null;
    String dir=mp.get_dir();
    while((history_file=mp.get_history()) != null){
      logger.debug("init histroy log thread "+dir+"/"+history_file);
      exec.execute(new MySpoolDirReadline(dir+"/"+history_file,outq));
    }
  }
  
  public void run(){
    logger.info("Start MySpoolDirRun.....");

    checkPoint();

    timedFlushService = Executors.newScheduledThreadPool(2,
            new ThreadFactoryBuilder().setNameFormat(
            "MySpoolDirRunTimedFlushService"+Thread.currentThread().getId() + "-%d").build());

    future[0] = timedFlushService.scheduleWithFixedDelay(new Runnable(){
      public void run(){
        try{
          flushCheckPoint();
        }catch(Exception e){
          logger.error("Exception flushCheckPoint ", e);
          if(e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
          }
        }
      }
    },
    flushTimeout,flushTimeout,TimeUnit.MILLISECONDS);

    future[1] = timedFlushService.scheduleWithFixedDelay(new Runnable(){
      public void run(){
        try{
          readHistroyFile();
        }catch(Exception e){
          logger.error("Exception flushCheckPoint ", e);
          if(e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
          }
        }
      }
    },
    checkHistoryTimeout,checkHistoryTimeout,TimeUnit.MILLISECONDS);
   
    File file=new File(mp.get_absfile());
    if(reader == null){
      try{
        reader = new RandomAccessFile(file,"r");
      }catch(IOException e){
        logger.error("reader create failed ",e);
        System.exit(1);
      }
    }

    logger.info("start readline file loop");
    try{
      outLoop: for(;true;){
        String line=null;
        while(true){
          line=reader.readLine();
          if(line != null){
            try{
              outq.put(line);
            }catch(InterruptedException e){
              logger.error("file line "+file,e);
            }
          }else{
            if(quit)
              break outLoop;
            String mp_file=mp.get_newfile();
            if(mp_file != null){
              if(mp_file.equals("MV")){
                mp_file=file.getAbsolutePath();
              }else{
                mp.set_file(mp_file);
              }
              logger.debug("read new log file");
              file=new File(mp.get_absfile());
              logger.debug("mp_file: "+mp_file);
              MySpoolDirTool.MyReaderClose(reader);
              break;
            }else{
              logger.debug("sleep 1000");
              MySpoolDirTool.mySleep(1000);
            }
          }
        }
        reader=new RandomAccessFile(file,"r");
      }
    }catch(IOException e){
      e.printStackTrace();
    }finally{
      MySpoolDirTool.MyReaderClose(reader);
    }
  }
}



class MySpoolDirReadline implements Runnable{
  private String file_name=null;
  private ArrayBlockingQueue<String> outq=null;

  private static final Logger logger = LoggerFactory.getLogger(MySpoolDirReadline.class);

  public MySpoolDirReadline(String name,ArrayBlockingQueue<String> out){
    this.file_name=name;
    this.outq=out;
  }

  public void run(){
    File file=new File(file_name);
    RandomAccessFile reader=null;
    try{
      reader=new RandomAccessFile(file,"r");
      String line=null;
      while((line=reader.readLine()) != null){
        try{
          outq.put(line);
        }catch(InterruptedException e){
          logger.error("Myreadline read file line "+line,e);
        }
      }
      file.delete();
    }catch(IOException e){
      logger.debug("readline file "+file_name+" IOException ",e);
    }finally{
      MySpoolDirTool.MyReaderClose(reader);
    }
  }
}

 
