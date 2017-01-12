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
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class MySpoolDirWatch implements Runnable,MySpoolDirInterface{
  private WatchService ws=null;
  private boolean quit=false;
  private ConcurrentHashMap<String,MySpoolDirPath> dict=null;
  private ConcurrentHashMap<String,MySpoolDirPath> watch=new ConcurrentHashMap<String,MySpoolDirPath>();

  private static final Logger logger = LoggerFactory.getLogger(MySpoolDirWatch.class);

  public MySpoolDirWatch(ConcurrentHashMap<String,MySpoolDirPath> mps){
    try{
      this.ws=FileSystems.getDefault().newWatchService();
      this.dict=mps;
    }catch(IOException e){
      logger.debug("Mywatch IO: ",e);
    }
  }

  public int kill(){
    this.quit=true;
    return 0; 
  }

  public void run(){
    try{
      	while(true){
      		for(String logDir : dict.keySet()){
      			try{
      	  			if(!watch.containsKey(logDir)){
						Path path=Paths.get(logDir);
      	  				path.register(ws,StandardWatchEventKinds.ENTRY_CREATE,StandardWatchEventKinds.ENTRY_MODIFY);
      	  				watch.put(logDir,dict.get(logDir));
     	  			}
      			}catch(IOException e){
      				logger.error("MySpoolDirWatch watch path ",e);
      			}
	  		}

      		WatchKey wkey=ws.take();
      		for(WatchEvent<?> event : wkey.pollEvents()){
        		if(event.kind()==StandardWatchEventKinds.ENTRY_CREATE){
        	    	Path filepath=(Path)wkey.watchable();
        	    	Path filename=(Path)event.context();

            		String[] strs=filepath.toFile().getName().split("_");
            		String regex=MySpoolDirTool.join("_",Arrays.asList(Arrays.copyOfRange(strs, 0, (strs.length-1))));
            		MySpoolDirPath mp=watch.get(filepath.toString());

            		logger.debug("Event File: "+filepath.toString()+"/"+filename.toString());

            		if(filename.equals(mp.get_file())){
		    			mp.set_newfile("MV");
            		}else if(MySpoolDirTool.checkFileName("\\^logHistoryCollect_"+regex+"-\\d+\\.log$",filename.toString())){
		    			mp.set_history(filename.toString());
            		}else if(MySpoolDirTool.checkFileName("\\^"+regex+"-\\d+\\.log$",filename.toString())){
		    			mp.set_newfile(filename.toString());
            		}
		  		}else{
					if(quit)
						return;
		  		}
        		if(! wkey.reset()){
        			logger.error("wkey unregisterede");
        		}
        	}
      	}
   }catch(InterruptedException e){
      logger.debug("WatchEvent Interrupted: ",e);
   }
  }
}

