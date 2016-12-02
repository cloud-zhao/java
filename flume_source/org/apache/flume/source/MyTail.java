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
import java.io.FileInputStream;     
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



public class MyTail extends AbstractSource implements EventDrivenSource,
Configurable {

  private static final Logger logger = LoggerFactory.getLogger(MyTail.class);

  private String fileName;
  private String fileNameRegex;
  private String fileDir;
  private SourceCounter sourceCounter;
  private ExecutorService executor;
  private Future<?> myTailRunFuture;
  private Future<?> myTailFuture;
  private Future<?> myWatchFuture;
  private String fileOutFromat;
  private String fileOutDelimit;
  private Integer bufferCount;
  private long batchTimeout;
  private MyTailRunnable myTailRun;
  private Mytail myTail;
  private Mywatch myWatch;
  private Mypath myPath;
  private Charset charset;
  private Integer queueSize;
  private ArrayBlockingQueue<String> mySystemOutPrint;

  @Override
  public void start() {
    logger.info("Exec source starting with");

    mySystemOutPrint = new ArrayBlockingQueue<String>(queueSize);
    myPath = new Mypath(fileDir,fileName);

    executor = Executors.newFixedThreadPool(6);

    myTailRun = new MyTailRunnable(getChannelProcessor(),sourceCounter,bufferCount,batchTimeout,charset,mySystemOutPrint);
    myTail = new Mytail(myPath,fileOutFromat,fileOutDelimit,mySystemOutPrint,executor);
    myWatch = new Mywatch(myPath,fileNameRegex);

    // FIXME: Use a callback-like executor / future to signal us upon failure.
    myTailRunFuture = executor.submit(myTailRun);
    myTailFuture = executor.submit(myTail);
    myWatchFuture = executor.submit(myWatch);


    /*
     * NB: This comes at the end rather than the beginning of the method because
     * it sets our state to running. We want to make sure the executor is alive
     * and well first.
     */
    sourceCounter.start();
    super.start();

    logger.debug("Exec source started");
  }

  @Override
  public void stop() {
    logger.debug("Stop myTail....");
    if(myTail != null){
      myTail.kill();
    }
    logger.debug("cancel exec myTail");
    if (myTailFuture != null) {
      logger.debug("Stopping exec myTail");
      myTailFuture.cancel(true);
      logger.debug("Exec myTail stopped");
    }

    logger.info("Stop myWatch....");
    if(myWatch != null) {
      myWatch.kill();
    }
    logger.info("cancel exec myWatch");
    if (myWatchFuture != null) {
      logger.debug("Stopping exec myWatch");
      myWatchFuture.cancel(true);
      logger.debug("Exec myWatch stopped");
    }

    logger.info("Stop myTailRun....");
    if(myTailRun != null) {
      myTailRun.kill();
    }
    logger.info("cancel exec myTailRun");
    if (myTailRunFuture != null) {
      logger.debug("Stopping exec myTailRun");
      myTailRunFuture.cancel(true);
      logger.debug("Exec myTailRun stopped");
    }

    executor.shutdown();

    while (!executor.isTerminated()) {
      logger.debug("Waiting for exec executor service to stop");
      try {
        executor.awaitTermination(500, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        logger.debug("Interrupted while waiting for exec executor service "
            + "to stop. Just exiting.");
        Thread.currentThread().interrupt();
      }
    }

    sourceCounter.stop();
    super.stop();

    logger.debug("mytail stopped. Metrics:{}",sourceCounter);
  }

  @Override
  public void configure(Context context) {
    fileName = context.getString("fileName");
    fileNameRegex = context.getString("fileNameRegex");
    fileDir = context.getString("fileDir");

    Preconditions.checkState(fileName != null,
        "The parameter fileName must be specified");
    Preconditions.checkState(fileNameRegex != null,
        "The parameter fileNameRegex must be specified");
    Preconditions.checkState(fileDir != null,
        "The parameter fileDir must be specified");

    bufferCount = context.getInteger(MyTailConfigurationConstants.CONFIG_BATCH_SIZE,
        MyTailConfigurationConstants.DEFAULT_BATCH_SIZE);

    queueSize = context.getInteger(MyTailConfigurationConstants.CONFIG_QUEUE_SIZE,
        MyTailConfigurationConstants.DEFAULT_QUEUE_SIZE);

    batchTimeout = context.getLong(MyTailConfigurationConstants.CONFIG_BATCH_TIME_OUT,
        MyTailConfigurationConstants.DEFAULT_BATCH_TIME_OUT);

    fileOutFromat = context.getString(MyTailConfigurationConstants.CONFIG_FILE_OUT_FROMAT,
        MyTailConfigurationConstants.DEFAULT_FILE_OUT_FROMAT);

    fileOutDelimit = context.getString(MyTailConfigurationConstants.CONFIG_FILE_OUT_DELIMIT,
        String.valueOf((char)MyTailConfigurationConstants.DEFAULT_FILE_OUT_DELIMIT));

    charset = Charset.forName(context.getString(MyTailConfigurationConstants.CHARSET,
        MyTailConfigurationConstants.DEFAULT_CHARSET));

    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
  }

  private static class MyTailRunnable implements Runnable {

    public MyTailRunnable(ChannelProcessor cp,SourceCounter sc,int bc,long bt,Charset cs,ArrayBlockingQueue<String> outq){
      this.channelProcessor=cp;
      this.sourceCounter=sc;
      this.bufferCount=bc;
      this.batchTimeout=bt;
      this.charset=cs;
      this.out=outq;
    }


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

    @Override
    public void run() {
        String exitCode = "unknown";
        String line = null;
        final List<Event> eventList = new ArrayList<Event>();

        timedFlushService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat(
                "timedFlushExecService" +
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
              sourceCounter.incrementEventReceivedCount();
              eventList.add(EventBuilder.withBody(line.getBytes(charset)));
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
        logger.error("MyTailRunnable ",e);
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

    public int kill(){
      this.quit=true;

      // Stop the Thread that flushes periodically
      if (future != null) {
        future.cancel(true);
      }

      if (timedFlushService != null) {
        timedFlushService.shutdown();
        while (!timedFlushService.isTerminated()) {
          try {
            timedFlushService.awaitTermination(500, TimeUnit.MILLISECONDS);
          }catch(InterruptedException e) {
            logger.debug("Interrupted while waiting for exec executor service "
              + "to stop. Just exiting.");
            Thread.currentThread().interrupt();
          }
        }
      }
      return 0;
    }
  }

  private static class Mytail implements Runnable{
    private Mypath mp=null;
    private String FILE_OUT_FROMAT=null;
    private String FILE_OUT_DELIMIT=null;
    private ArrayBlockingQueue<String> outq=null;
    private ExecutorService exec=null;
    private boolean quit=false;

    public Mytail(Mypath mp,String fof,String fod,ArrayBlockingQueue<String> out,ExecutorService e){
        this.mp=mp;
        this.FILE_OUT_FROMAT=fof;
        this.FILE_OUT_DELIMIT=fod;
        this.outq=out;
        this.exec=e;
    }

    public int kill(){
      this.quit=true;
      return 0;
    }
  
    public void run(){
      logger.info("Start Mytail.....");
   
      File file=new File(mp.get_absfile());
      String dir=mp.get_dir();
      BufferedReader reader=null;

      logger.info("start readline file loop");
      try{
        for(;true;){
          reader=new BufferedReader(new FileReader(file));
          String line=null;
          while(true){
            line=reader.readLine();
            if(line != null){
              try{
                outq.put(Mytools.fromatOut(FILE_OUT_FROMAT,FILE_OUT_DELIMIT,line));
              }catch(InterruptedException e){
                logger.error("file line "+file,e);
              }
            }else{
              if(quit)
                break;
              String abs_file=file.getAbsolutePath();
              String mp_file=mp.get_absfile();
              String history_file=null;
              while((history_file=mp.get_history()) != null){
                logger.debug("init histroy log thread "+dir+"/"+history_file);
                exec.execute(new Myreadline(dir+"/"+history_file,FILE_OUT_FROMAT,FILE_OUT_DELIMIT,outq));
              }
              if((! abs_file.equals(mp_file)) || mp.get_flag().equals("MV")){
                if(mp.get_flag().equals("MV"))
                  mp.set_flag("");
                  logger.debug("read new log file");
                  file=new File(mp_file);
                  logger.debug("Abs: "+abs_file+" mp_file: "+mp_file);
                  Mytools.MyReaderClose(reader);
                  break;
                }
              logger.debug("sleep 1000");
              Mytools.MySleep(1000);
            }
          }
        }
      }catch(IOException e){
        e.printStackTrace();
      }finally{
        Mytools.MyReaderClose(reader);
      }
    }
  }

  private static class Mytools{
    public static void MySleep(int ms){
      try{
        Thread.currentThread().sleep(ms);
      }catch(InterruptedException e){
        logger.debug("Sleep {}ms InterruptedException ",ms,e);
      }
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

    public static void MyReaderClose(BufferedReader re){
      if(re != null){
        try{
          re.close();
        }catch(IOException e1){
          logger.debug("BufferedReader close IOException ",e1);
        }
      }
    }


  } 

  private static class Mywatch implements Runnable{
	 private Mypath mypath=null;
	 private WatchService ws=null;
	 private Path path=null;
	 private String regex=null;
   private boolean quit=false;

	 public Mywatch(Mypath myp,String regex){
	 	 try{
			 this.regex=regex;
			 this.mypath=myp;
			 this.ws=FileSystems.getDefault().newWatchService();
			 this.path=Paths.get(myp.get_dir());
		  }catch(IOException e){
			 logger.debug("Mywatch IO: ",e);
		  }
	 }

    private boolean checkFileName(String regex,String fileName){
      Pattern p=Pattern.compile(regex);
      Matcher matcher=p.matcher(fileName);
      boolean res=matcher.matches();
      logger.debug("checkFileName Regex: "+regex+" FileName: "+fileName+" "+res);
      return res;
    }

    public int kill(){
      this.quit=true;
      return 0;
    }

	  public void run(){
		  try{
			 path.register(ws,StandardWatchEventKinds.ENTRY_CREATE,StandardWatchEventKinds.ENTRY_MODIFY);

			 while(true){
				  WatchKey wkey=ws.take();
				  for(WatchEvent<?> event : wkey.pollEvents()){
					 if(event.kind()==StandardWatchEventKinds.ENTRY_CREATE){
						  String file_name=event.context().toString();

						  logger.debug("Event File: "+file_name);

						  if(file_name.equals(mypath.get_file()))
							 mypath.set_flag("MV");
						  else if(checkFileName("^"+regex+"$",file_name))
							 mypath.set_file(file_name);
						  else if(checkFileName("^history_"+regex+"$",file_name))
							 mypath.set_history(file_name);
					 }else{
              if(quit)
                return;
           }
				  }
				  if(! wkey.reset()){
					 logger.debug("wkey unregisterede");
				  }
			 }
		  }catch(InterruptedException e){
			 logger.debug("WatchEvent Interrupted: ",e);
		  }catch(IOException e1){
			 logger.debug("WatchEvent IOException: ",e1);
		  }
	  }
  }

  private static class Myreadline implements Runnable{
    private String file_name=null;
    private String fof;
    private String fod;
    private ArrayBlockingQueue<String> outq=null;

    protected Myreadline(String name,String fof,String fod,ArrayBlockingQueue<String> out){
      this.file_name=name;
      this.fof=fof;
      this.fod=fod;
      this.outq=out;
    }

    public void run(){
      File file=new File(file_name);
      BufferedReader reader=null;
      try{
        reader=new BufferedReader(new FileReader(file));
        String line=null;
        while((line=reader.readLine()) != null){
          try{
            outq.put(Mytools.fromatOut(fof,fod,line));
          }catch(InterruptedException e){
            logger.error("Myreadline read file line "+line,e);
          }
        }
      }catch(IOException e){
        logger.debug("readline file "+file_name+" IOException ",e);
      }finally{
        Mytools.MyReaderClose(reader);
      }
    }
  }

  private static class Mypath{
	   private String dir=null;
	   private String file=null;
	   private String flag="";
	   private ArrayBlockingQueue<String> history=new ArrayBlockingQueue<String>(30);

	   public Mypath(String path,String file){
	 	   this.dir=path;
		    this.file=file;
	   }

	   public String get_dir(){
	     return dir.charAt(dir.length()-1) == '/' ?
			 dir.substring(0,dir.length()-1) : 
			 dir;
	   }
	   public String get_absfile(){
		    return dir.charAt(dir.length()-1) == '/' ?
			 dir+file : 
			 dir+"/"+file;
	   }
	   public String get_file(){
		  return file;
	   }
	   public void set_file(String file_name){
		  this.file=file_name;
	   }
	   public synchronized void set_flag(String flag){
		  this.flag=flag;
	   }
	   public String get_flag(){
		  return flag;
	   }
	   public String get_history(){
		  try{
		    return history.remove();
		  }catch(NoSuchElementException e){
			 return null;
		  }
	   }
	   public void set_history(String h){
		  try{
        this.history.put(h);
	    }catch(InterruptedException e){
        logger.error("Mypath  history put failed ",e);
      }
     }
  } 
}
