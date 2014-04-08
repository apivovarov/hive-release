### Licensed to the Apache Software Foundation (ASF) under one or more
### contributor license agreements.  See the NOTICE file distributed with
### this work for additional information regarding copyright ownership.
### The ASF licenses this file to You under the Apache License, Version 2.0
### (the "License"); you may not use this file except in compliance with
### the License.  You may obtain a copy of the License at
###
###     http://www.apache.org/licenses/LICENSE-2.0
###
### Unless required by applicable law or agreed to in writing, software
### distributed under the License is distributed on an "AS IS" BASIS,
### WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
### See the License for the specific language governing permissions and
### limitations under the License.

###
### Install script that can be used to install Hive as a Single-Node cluster.
### To invoke the scipt, run the following command from PowerShell:
###   install.ps1 -username <username> -password <password> or
###   install.ps1 -credentialFilePath <credentialFilePath>
###
### where:
###   <username> and <password> represent account credentials used to run
###   Hive services as Windows services.
###   <credentialFilePath> encripted credentials file path
###
### By default, Hadoop is installed to "C:\Hadoop". To change this set
### HADOOP_NODE_INSTALL_ROOT environment variable to a location were
### you'd like Hadoop installed.
###
### Script pre-requisites:
###   JAVA_HOME must be set to point to a valid Java location.
###   HADOOP_HOME must be set to point to a valid Hadoop install location.
###
### To uninstall previously installed Single-Node cluster run:
###   uninstall.ps1
###
### NOTE: Notice @version@ strings throughout the file. First compile
### winpkg with "ant winpkg", that will replace the version string.
###

param(
    [String]
    [Parameter( ParameterSetName='UsernamePassword', Position=0, Mandatory=$true )]
    [Parameter( ParameterSetName='UsernamePasswordBase64', Position=0, Mandatory=$true )]
    $username,
    [String]
    [Parameter( ParameterSetName='UsernamePassword', Position=1, Mandatory=$true )]
    $password,
    [String]
    [Parameter( ParameterSetName='UsernamePasswordBase64', Position=1, Mandatory=$true )]
    $passwordBase64,
    [Parameter( ParameterSetName='CredentialFilePath', Mandatory=$true )]
    $credentialFilePath,
    [String]
    $hiveRoles = "hiveclient"
    )

    ####"hiveclient hwi derbyserver hiveserver2 metastore"
function Main( $scriptDir )
{
    $FinalName = "@final.name@"
    if ( -not (Test-Path ENV:WINPKG_LOG))
    {
        $ENV:WINPKG_LOG = "$FinalName.winpkg.log"
    }

    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"
    $nodeInstallRoot = "$ENV:HADOOP_NODE_INSTALL_ROOT"
    $hiveInstallToDir = Join-Path "$nodeInstallRoot" "$FinalName"

    ###
    ### Create the Credential object from the given username and password or the provided credentials file
    ###
    $serviceCredential = Get-HadoopUserCredentials -credentialsHash @{"username" = $username; "password" = $password; `
        "passwordBase64" = $passwordBase64; "credentialFilePath" = $credentialFilePath}
    $username = $serviceCredential.UserName
    Write-Log "Username: $username"
    Write-Log "CredentialFilePath: $credentialFilePath"


    if ("$ENV:IS_HIVE_SERVER" -eq "yes" ) {
      $hiveRoles = "$hiveRoles"+" "+"hiveclient hwi hiveserver2 metastore"
    }
    if ("$ENV:IS_DERBY_SERVER" -eq "yes" ) {
      $hiveRoles = "$hiveRoles"+" "+"derbyserver"
    }

    ###
    ### Install and Configure Hive
    ###
    if ((Test-Path ENV:IS_TEZ) -and ($ENV:IS_TEZ -ieq "yes"))
    {
      $locations = 'HKLM:\SYSTEM\CurrentControlSet\Control\Session Manager\Environment',
                   'HKCU:\Environment'

      $locations | ForEach-Object {
        $k = Get-Item $_
        $k.GetValueNames() | ForEach-Object {
          $name  = $_
          $value = $k.GetValue($_)
          if ($name -ieq 'TEZ_CLASSPATH') {
            Set-Item -Path Env:\$name -Value $value
          }
        }
      }
      Write-Log "Setting HIVE_CLASSPATH to $ENV:TEZ_CLASSPATH at machine scope"
      [Environment]::SetEnvironmentVariable( "HIVE_CLASSPATH", "$ENV:TEZ_CLASSPATH", [EnvironmentVariableTarget]::Machine )
      $ENV:HIVE_CLASSPATH = "$ENV:TEZ_CLASSPATH"
    }
    else
    {
      Write-Log "Not Setting HIVE_CLASSPATH to TEZ_CLASSPATH since TEZ is not enabled."
    }

    Install "hive" $nodeInstallRoot $serviceCredential $hiveRoles

    Write-Log "Installation of Hive completed successfully"
    ###
    ### Install Hcatalog/webhcat
    ###
    Write-Log "Installing Hcatalog"

    $FinalName = "hcatalog"
    $ENV:HCAT_HOME = Join-Path $env:HIVE_HOME "$FinalName"
    ###
    ### Install webhcat
    ###
    $ENV:TEMPLETON_HOME = "$ENV:HCAT_HOME"
    $ENV:TEMPLETON_LOG_DIR="$ENV:HDP_LOG_DIR\webhcat"
    Write-Log "Setting TEMPLETON_LOG_DIR to $ENV:TEMPLETON_LOG_DIR at machine scope"
    [Environment]::SetEnvironmentVariable( "TEMPLETON_LOG_DIR", "$ENV:TEMPLETON_LOG_DIR", [EnvironmentVariableTarget]::Machine )

    if ( -not (Test-Path ENV:WINPKG_LOG))
    {
        $ENV:WINPKG_LOG = "$FinalName.winpkg.log"
    }
    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"
    $nodeInstallRoot = $ENV:HADOOP_NODE_INSTALL_ROOT
    $templetonInstallToDir = Join-Path $env:HIVE_HOME "$FinalName"

    Write-Log "Installing Apache Hcatalog $FinalName to $nodeInstallRoot"
    Write-Log "Installing Apache Templeton $FinalName to $templetonInstallToDir"

    ###
    ### Create the Credential object from the given username and password or the provided credentials file
    ###
    $serviceCredential = Get-HadoopUserCredentials -credentialsHash @{"username" = $username; "password" = $password; `
        "passwordBase64" = $passwordBase64; "credentialFilePath" = $credentialFilePath}
    $username = $serviceCredential.UserName
    Write-Log "Username: $username"
    Write-Log "CredentialFilePath: $credentialFilePath"

    ###
    ### Stop templeton services before proceeding with the install step, otherwise
    ### files will be in-use and installation can fail
    ###
    Write-Log "Stopping Templeton services if already running before proceeding with install"
    StopService "hcatalog" "templeton"

    if ( "$ENV:IS_WEBHCAT" -eq "yes" ) {
    $templetonRole="templeton"
    }
    Install "hcatalog" $nodeInstallRoot $serviceCredential $templetonRole

    Write-Log "Done installing Hcatalog"
    ###
    ### Configure a separate MapReduce capacity-scheduler queue for Templeton.
    ### This is required to avoid deadlock, where all map slots are currently
    ### taken by the Templeton map-only jobs.
    ### For single-node we set cap on the templeton queue capacity to 50% and
    ### leave 50% of map slots available for jobs to complete.
    ###

    $xmlFile = Join-Path $ENV:HADOOP_HOME "etc\hadoop\capacity-scheduler.xml"
    UpdateXmlConfig $xmlFile @{
        "yarn.scheduler.capacity.root.queues" = "default,joblauncher";
        "yarn.scheduler.capacity.root.default.capacity" = "95";
        "yarn.scheduler.capacity.root.default.user-limit-factor" = "10";
        "yarn.scheduler.capacity.root.joblauncher.capacity" = "5";
        "yarn.scheduler.capacity.root.joblauncher.maximum-capacity" = "50";
        "yarn.scheduler.capacity.root.joblauncher.user-limit-factor" = "10" }

    ###
    ### Configure Templeton to use a separate "joblauncher" queue
    ###
    Configure "hcatalog" $NodeInstallRoot $ServiceCredential @{
        "templeton.hadoop.queue.name" = "joblauncher" }
    Write-Log "Configuring the Templeton configurations"
    $pythonpath = Which python
    Write-Log "Using python from $pythonpath"
    $xmlFile = Join-Path $ENV:TEMPLETON_HOME "etc\webhcat\webhcat-site.xml"

    $zookeeperNodes = $env:ZOOKEEPER_HOSTS.Replace(",",":2181,")
    $zookeeperNodes = $zookeeperNodes + ":2181"

    UpdateXmlConfig $xmlFile @{
        "templeton.hive.properties" = "hive.metastore.local=false,hive.metastore.uris=thrift://${ENV:HIVE_SERVER_HOST}:9083";
        "templeton.python" = "$pythonpath";
        "templeton.storage.class" = "org.apache.hive.hcatalog.templeton.tool.ZooKeeperStorage";
        "templeton.zookeeper.hosts" = "$zookeeperNodes"}
    Write-Log "Finished installing Apache Templeton"
}

try
{
    $scriptDir = Resolve-Path (Split-Path $MyInvocation.MyCommand.Path)
    $utilsModule = Import-Module -Name "$scriptDir\..\resources\Winpkg.Utils.psm1" -ArgumentList ("HIVE") -PassThru
    $apiModule = Import-Module -Name "$scriptDir\InstallApi.psm1" -PassThru
    Main $scriptDir
}
catch
{
	Write-Log $_.Exception.Message "Failure" $_
	exit 1
}
finally
{
    if( $apiModule -ne $null )
    {
        Remove-Module $apiModule
    }

    if( $utilsModule -ne $null )
    {
        Remove-Module $utilsModule
    }
}
