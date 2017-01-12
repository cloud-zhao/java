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



public class MyTail extends AbstractSource implements EventDrivenSource,
Configurable {

  private static final Logger logger = LoggerFactory.getLogger(MyTail.class);

  private String fileName;
  private String fileNameRegex;
  private String fileDir;
  private boolean fileRoll;
  private long flushTimeout;
  private boolean lineCheck;
  private SourceCounter sourceCounter;
  private ExecutorService executor;
  private Future<?> myTailRunFuture;
  private Future<?> myTailFuture;
  private Future<?> myWatchFuture;
  private String fileOutFromat;
  private boolean fileOutFromatEnable;
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
    logger.info("MyTail source starting with");

    mySystemOutPrint = new ArrayBlockingQueue<String>(queueSize);
    myPath = new Mypath(fileDir,fileName);

    executor = Executors.newFixedThreadPool(6);

    myTailRun = new MyTailRunnable(getChannelProcessor(),sourceCounter,bufferCount,batchTimeout,charset,mySystemOutPrint);
    myTail = new Mytail(myPath,lineCheck,fileRoll,flushTimeout,fileOutFromat,fileOutDelimit,fileOutFromatEnable,mySystemOutPrint,executor);
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

    logger.debug("MyTail source started");
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

    lineCheck = context.getBoolean(MyTailConfigurationConstants.CONFIG_LINE_CHECK,
        MyTailConfigurationConstants.DEFAULT_LINE_CHECK);

    fileRoll = context.getBoolean(MyTailConfigurationConstants.CONFIG_FILE_ROLL,
        MyTailConfigurationConstants.DEFAULT_FILE_ROLL);

    flushTimeout = context.getLong(MyTailConfigurationConstants.CONFIG_FLUSH_TIME,
        MyTailConfigurationConstants.DEFAULT_FLUSH_TIME);

    bufferCount = context.getInteger(MyTailConfigurationConstants.CONFIG_BATCH_SIZE,
        MyTailConfigurationConstants.DEFAULT_BATCH_SIZE);

    queueSize = context.getInteger(MyTailConfigurationConstants.CONFIG_QUEUE_SIZE,
        MyTailConfigurationConstants.DEFAULT_QUEUE_SIZE);

    batchTimeout = context.getLong(MyTailConfigurationConstants.CONFIG_BATCH_TIME_OUT,
        MyTailConfigurationConstants.DEFAULT_BATCH_TIME_OUT);

    fileOutFromat = context.getString(MyTailConfigurationConstants.CONFIG_FILE_OUT_FROMAT,
        MyTailConfigurationConstants.DEFAULT_FILE_OUT_FROMAT);

    fileOutFromatEnable = context.getBoolean(MyTailConfigurationConstants.CONFIG_FILE_OUT_FROMAT_ENABLE,
        MyTailConfigurationConstants.DEFALUT_FILE_OUT_FROMAT_ENABLE);

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
    private boolean fofe;
    private ArrayBlockingQueue<String> outq=null;
    private ExecutorService exec=null;
    private long flushTimeout;
    private boolean quit=false;
    private boolean fileRoll;
    private boolean lineCheck;
    private String checkPointFileName=null;
    private RandomAccessFile reader = null;
    ScheduledExecutorService timedFlushService;
    ScheduledFuture<?> future;

    public Mytail(Mypath mp,boolean lc,boolean fr,long ft,String fof,String fod,boolean fofe,ArrayBlockingQueue<String> out,ExecutorService e){
        this.mp=mp;
        this.lineCheck=lc;
        this.fileRoll=fr;
        this.flushTimeout=ft;
        this.FILE_OUT_FROMAT=fof;
        this.FILE_OUT_DELIMIT=fod;
        this.fofe=fofe;
        this.outq=out;
        this.exec=e;
        this.checkPointFileName=mp.get_dir()+"/"+".check_point_file";
    }

    private void checkPoint(){
      File checkPointFile = new File(checkPointFileName);
      BufferedReader creader = null;
      if(checkPointFile.canRead()){
        try{
          creader = new BufferedReader(new FileReader(checkPointFile));
          String line = creader.readLine();
          if(line != null){
            String[] file_info=line.split("\\|");
            if(!fileRoll){
              if((System.currentTimeMillis()/1000 - Integer.parseInt(file_info[2])) < (24*3600)){
                try{ 
                  reader = new RandomAccessFile(mp.get_dir()+"/"+file_info[0],"r");
                  reader.seek(Long.parseLong(file_info[1]));
                }catch(IOException e){
                  logger.error("checkPoint new RandomAccessFile error ",e);
                  Mytools.MyReaderClose(reader);
                  reader = null;
                }
              }
            }else{
              mp.set_file(file_info[0]);
              try{
                reader = new RandomAccessFile(mp.get_dir()+"/"+file_info[0],"r");
                reader.seek(Long.parseLong(file_info[1]));
              }catch(IOException e2){
                logger.error("checkPoint new RandomAccessFile error",e2);
                Mytools.MyReaderClose(reader);
                reader = null;
              }
            }
          }
        }catch(IOException e1){
          logger.debug("checkPointFileName not reader line",e1);
        }finally{
          Mytools.MyReaderClose(creader);
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
      Mytools.writeSingle(checkPointFileName,mp.get_file()+"|"+point+"|"+timeStamp+"\n",false);
    }

    public int kill(){
      this.quit=true;

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
  
    public void run(){
      logger.info("Start Mytail.....");
      logger.error("start read file...");

      checkPoint();

      /*
      timedFlushService = Executors.newScheduledThreadPool(2,
              new ThreadFactoryBuilder().setNameFormat(
              "timedFlushExecService" +
              Thread.currentThread().getId() + "-%d").build());
      */

      timedFlushService = Executors.newSingleThreadScheduledExecutor(
              new ThreadFactoryBuilder().setNameFormat(
              "timedFlushCheckPointExecService" +
              Thread.currentThread().getId() + "-%d").build());

      future = timedFlushService.scheduleWithFixedDelay(new Runnable(){
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
   
      File file=new File(mp.get_absfile());
      String dir=mp.get_dir();
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
                if(lineCheck){
                  if(line.indexOf(String.valueOf((char)1)) != -1){
                    outq.put(Mytools.fromatOut(FILE_OUT_FROMAT,FILE_OUT_DELIMIT,line,fofe));
                  }
                }else{
                  outq.put(Mytools.fromatOut(FILE_OUT_FROMAT,FILE_OUT_DELIMIT,line,fofe));
                }
                //outq.put(line);
              }catch(InterruptedException e){
                logger.error("file line "+file,e);
              }
            }else{
              logger.error("read file ok");
              if(quit)
                break outLoop;
              String mp_file=mp.get_newfile();
              String history_file=null;
              while((history_file=mp.get_history()) != null){
                logger.debug("init histroy log thread "+dir+"/"+history_file);
                exec.execute(new Myreadline(dir+"/"+history_file,FILE_OUT_FROMAT,FILE_OUT_DELIMIT,fofe,outq));
              }
              if(mp_file != null){
                if(mp_file.equals("MV")){
                  mp_file=file.getAbsolutePath();
                }else{
                  mp.set_file(mp_file);
                }
                logger.debug("read new log file");
                file=new File(mp.get_absfile());
                logger.debug("mp_file: "+mp_file);
                Mytools.MyReaderClose(reader);
                break;
              }else{
                logger.debug("sleep 1000");
                Mytools.MySleep(1000);
              }
            }
          }
          reader=new RandomAccessFile(file,"r");
        }
      }catch(IOException e){
        logger.debug("Mytail readline ",e);
      }finally{
        Mytools.MyReaderClose(reader);
      }
    }
  }

  private static class Mytools{
    protected static void MySleep(int ms){
      try{
        Thread.currentThread().sleep(ms);
      }catch(InterruptedException e){
        logger.debug("Sleep {}ms InterruptedException ",ms,e);
      }
    }

    protected static String join(String flag,List<String> list){
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

    protected static void writeSingle(String f,String line,boolean append){
      try{
        FileWriter fw = new FileWriter(f,append);
        fw.write(line);
        fw.flush();
        fw.close();
      }catch(IOException e){
        logger.error("file "+f+" writer failed",e);
      }
    }

    protected static String fromatOut(String regex,String flag,String input,boolean fofe){
      if(!fofe){
        return input;
      }
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

    protected static void MyReaderClose(Object re){
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
              //Path path=(Path)wkey.watchable();
						  String file=event.context().toString();

						  logger.debug("Event File: "+file);

						  if(file.equals(mypath.get_file())){
							 mypath.set_newfile("MV");
              }else if(checkFileName("^"+regex+"$",file))
							 mypath.set_newfile(file);
						  else if(checkFileName("^history_"+regex+"$",file))
							 mypath.set_history(file);
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
    private boolean fofe;
    private ArrayBlockingQueue<String> outq=null;

    protected Myreadline(String name,String fof,String fod,boolean fofe,ArrayBlockingQueue<String> out){
      this.file_name=name;
      this.fof=fof;
      this.fofe=fofe;
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
            //outq.put(Mytools.fromatOut(fof,fod,line,fofe));
            outq.put(line);
          }catch(InterruptedException e){
            logger.error("Myreadline read file line "+line,e);
          }
        }
        file.delete();
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
     private ArrayBlockingQueue<String> newFile=new ArrayBlockingQueue<String>(3);
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
     public void set_file(String file){
      this.file=file;
     }
     public String get_file(){
      return file;
     }
     public String get_absfile(){
      return get_dir()+"/"+file;
     }
     public void set_newfile(String f){
      try{
        this.newFile.put(f);
      }catch(InterruptedException e){
        logger.error("Mypath new file put failed ",e);
      }
     }
     public String get_newfile(){
      try{
        return newFile.remove();
      }catch(NoSuchElementException e){
        return null;
      }
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
