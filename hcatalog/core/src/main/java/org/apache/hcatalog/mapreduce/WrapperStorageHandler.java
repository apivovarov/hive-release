/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hcatalog.mapreduce;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hcatalog.common.HCatConstants;

/**
 *  This class is used to encapsulate an underlying HiveStorageHandler and do
 *  o.a.mapred->o.a.mapreduce conversions so HCatalog can use them.
 */
public class WrapperStorageHandler extends HCatStorageHandler {

    private final Class<? extends HiveStorageHandler> handlerClass;
    private final HiveStorageHandler handler;
    private boolean isHiveOutputFormat = true;
    private Class<? extends OutputFormat> outputFormatClass = null;

    public WrapperStorageHandler(Class<? extends HiveStorageHandler> handlerClass,
            Configuration conf) throws Exception{
        this.handlerClass = handlerClass;
        this.handler = handlerClass.newInstance();
        if (conf != null){
            this.handler.setConf(conf);
        }
    }

    public WrapperStorageHandler(String handlerClassName) throws Exception {
        this((Class<? extends HiveStorageHandler>) Class.forName(handlerClassName),null);
    }

    public HiveStorageHandler getUnderlyingHiveStorageHandler() {
        return this.handler;
    }

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return handler.getInputFormatClass();
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        if (outputFormatClass == null){
            // determines if outputformat is a HiveOutputFormat -
            // if it isn't, we find the appropriate substitute
            outputFormatClass = this.handler.getOutputFormatClass();
            if (!HiveOutputFormat.class.isAssignableFrom(outputFormatClass)){
                isHiveOutputFormat = false;
                outputFormatClass = HiveFileFormatUtils.getOutputFormatSubstitute(outputFormatClass);
            }
        }
        return outputFormatClass;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return handler.getSerDeClass();
    }

    @Override
    public HiveMetaHook getMetaHook() {
        return handler.getMetaHook();
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc,
                                            Map<String, String> jobProperties) {
        String hcatInputInfo = tableDesc.getJobProperties().get(HCatConstants.HCAT_KEY_JOB_INFO);
        handler.configureInputJobProperties(tableDesc, jobProperties);
        if (!jobProperties.containsKey(HCatConstants.HCAT_KEY_JOB_INFO)){
            // If the underlying storage handler didn't copy the hcat input job info, we do
            jobProperties.put(HCatConstants.HCAT_KEY_JOB_INFO, hcatInputInfo);
        }
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc,
                                             Map<String, String> jobProperties) {
        String hcatOutputInfo = tableDesc.getJobProperties().get(HCatConstants.HCAT_KEY_OUTPUT_INFO);
        handler.configureOutputJobProperties(tableDesc, jobProperties);
        if (!jobProperties.containsKey(HCatConstants.HCAT_KEY_OUTPUT_INFO)){
            // If the underlying storage handler didn't copy the hcat output job info, we do
            jobProperties.put(HCatConstants.HCAT_KEY_OUTPUT_INFO, hcatOutputInfo);
        }
    }

    @Override
    OutputFormatContainer getOutputFormatContainer(
        org.apache.hadoop.mapred.OutputFormat outputFormat) {
        return new DefaultOutputFormatContainer(outputFormat);
    }

    @Override
    public Configuration getConf() {
        return handler.getConf();
    }

    @Override
    public void setConf(Configuration conf) {
        handler.setConf(conf);
    }

    @Override
    public HiveAuthorizationProvider getAuthorizationProvider()
        throws HiveException {
        return handler.getAuthorizationProvider();
    }

}
