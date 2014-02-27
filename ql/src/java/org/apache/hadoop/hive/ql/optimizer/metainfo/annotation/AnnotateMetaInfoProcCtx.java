package org.apache.hadoop.hive.ql.optimizer.metainfo.annotation;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;

public class AnnotateMetaInfoProcCtx implements NodeProcessorCtx {

  ParseContext parseContext;
  HiveConf conf;
  
  public AnnotateMetaInfoProcCtx(ParseContext parseContext) {
    this.setParseContext(parseContext);
    if(parseContext != null) {
      this.setConf(parseContext.getConf());
    } else {
      this.setConf(null);
    }
  }

  public HiveConf getConf() {
    return conf;
  }

  public void setConf(HiveConf conf) {
    this.conf = conf;
  }

  public ParseContext getParseContext() {
    return parseContext;
  }

  public void setParseContext(ParseContext parseContext) {
    this.parseContext = parseContext;
  }

}
