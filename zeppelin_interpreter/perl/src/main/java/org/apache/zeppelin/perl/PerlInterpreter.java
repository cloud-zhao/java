/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*  http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.zeppelin.perl;

import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterPropertyBuilder;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * Perl interpreter for Zeppelin.
 */
public class PerlInterpreter extends Interpreter {
  Logger logger = LoggerFactory.getLogger(PerlInterpreter.class);

  public static final String ZEPPELIN_PERL = "zeppelin.perl";
  public static final String DEFAULT_ZEPPELIN_PERL = "/usr/bin/perl";

  private Integer port;
  private long perlPid;
  private InterpreterContext context;


  static {
    Interpreter.register(
        "perl",
        "perl",
        PerlInterpreter.class.getName(),
        new InterpreterPropertyBuilder()
            .add(ZEPPELIN_PERL, DEFAULT_ZEPPELIN_PERL,
                "Perl directory. Default : perl (assume perl is in your $PATH)")
            .build()
    );
  }

  public PerlInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void open() {
    logger.info("Starting Perl interpreter .....");
    logger.info("Perl path is set to:" + property.getProperty(ZEPPELIN_PERL));
  }

  @Override
  public void close() {
    logger.info("closing Perl interpreter .....");
  }

  @Override
  public InterpreterResult interpret(String cmd, InterpreterContext contextInterpreter) {
    this.context = contextInterpreter;
    if (cmd == null || cmd.isEmpty()) {
      return new InterpreterResult(Code.SUCCESS, "");
    }
    String output = sendCommandToPerl(cmd);

    // TODO(zjffdu), we should not do string replacement operation in the result, as it is
    // possible that the output contains the kind of pattern itself, e.g. print("...")
    return new InterpreterResult(Code.SUCCESS, output.replaceAll("\\.\\.\\.", ""));
  }

  @Override
  public void cancel(InterpreterContext context) {
      logger.error("Can't interrupt the perl interpreter");
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public List<InterpreterCompletion> completion(String buf, int cursor) {
    return null;
  }

  @Override
  public FormType getFormType() {
    return FormType.NATIVE;
  }

  private String sendCommandToPerl(String cmd) {
    String output = "";
    logger.info("Sending : \n" + (cmd.length() > 200 ? cmd.substring(0, 200) + "..." : cmd));
    try {
      logger.info("Waite process");
      output = PerlProcess.sendAndGetResult(property.getProperty(ZEPPELIN_PERL),cmd);
    } catch (IOException e) {
      logger.error("Error when sending commands to perl process", e);
    }
    logger.info("Got : \n" + output);
    return output;
  }

}
