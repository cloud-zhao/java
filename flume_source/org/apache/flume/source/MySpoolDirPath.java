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


 public class MySpoolDirPath{
	private String dir=null;
	private String file=null;
	private String flag="";
  private ArrayBlockingQueue<String> newFile=new ArrayBlockingQueue<String>(5);
	private ArrayBlockingQueue<String> history=new ArrayBlockingQueue<String>(30);

	private static final Logger logger = LoggerFactory.getLogger(MySpoolDirPath.class);

	 public MySpoolDirPath(String path,String file){
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
