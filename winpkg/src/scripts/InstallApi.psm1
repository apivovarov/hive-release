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
### A set of basic PowerShell routines that can be used to install and
### manage Hadoop services on a single node. For use-case see install.ps1.
###

###
### Global variables
###
$ScriptDir = Resolve-Path (Split-Path $MyInvocation.MyCommand.Path)
$FinalName = "@final.name@"
$DefaultRoles = @("hiveclient", "Derbyserver", "hiveserver2", "metastore")
$WaitingTime = 10000

###############################################################################
###
### Installs Hive.
###
### Arguments:
###     component: Component to be installed, it can be "core, "hdfs" or "mapreduce"
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     role: Space separated list of roles that should be installed.
###           (for example, "jobtracker historyserver" for mapreduce)
###
###############################################################################
function Install(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=2, Mandatory=$true )]
    $serviceCredential,
    [String]
    [Parameter( Position=3, Mandatory=$false )]
    $roles
    )
{
    if ( $component -eq "hive" )
    {
        $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"

        ### $hiveInstallDir: the directory that contains the appliation, after unzipping
        $hiveInstallToDir = Join-Path "$nodeInstallRoot" "$FinalName"
        $hiveInstallToBin = Join-Path "$hiveInstallToDir" "bin"
        Write-Log "hiveInstallToDir: $hiveInstallToDir"

        CheckRole $roles $DefaultRoles

        Write-Log "Checking the Hive Installation before copying the hive bits"
        if( -not (Test-Path $ENV:HIVE_HOME\bin\hive.cmd))
        {
            InstallBinaries $nodeInstallRoot $serviceCredential
        }

        ###
        ### Create Hive Windows Services and grant user ACLS to start/stop
        ###

        Write-Log "Node Hive Role Services: $roles"
        $allServices = $roles

        Write-Log "Installing services $allServices"

        foreach( $service in empty-null $allServices.Split(' '))
        {
            if ( $service -eq "hiveclient" )
            {
                continue
            }

            CreateAndConfigureHadoopService $service $HDP_RESOURCES_DIR $hiveInstallToBin $serviceCredential

            Write-Log "Creating service config ${hiveInstallToBin}\$service.xml"
            if( $service -ne "derbyserver" )
            {
                $cmd = "$hiveInstallToBin\hive.cmd --service $service catservicexml > `"$hiveInstallToBin`"\$service.xml"
            }
            else
            {
                $cmd = "$hiveInstallToBin\derbyserver.cmd catservicexml > `"$hiveInstallToBin`"\derbyserver.xml"
            }

            Invoke-CmdChk $cmd
        }

        ### Configure the default log locations
        $hivelogsdir = "$hiveInstallToDir\logs"
        if (Test-Path ENV:HIVE_LOG_DIR)
        {
            $hivelogsdir = "$ENV:HIVE_LOG_DIR"
        }
        Configure "hive" $nodeInstallRoot $serviceCredential @{
        "hive.log.dir" = "$hivelogsdir";
        "hive.querylog.location" = "$hivelogsdir\history"}
    }
    elseif ( $component -eq "hcatalog" )
	{
        $FinalName = "hcatalog"
        $WebHCatVersion = "@version@"
        $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"
        Write-Log "Checking the JAVA Installation."
        if( -not (Test-Path $ENV:JAVA_HOME\bin\java.exe))
        {
            Write-Log "JAVA_HOME not set properly; $ENV:JAVA_HOME\bin\java.exe does not exist" "Failure"
            throw "Install: JAVA_HOME not set properly; $ENV:JAVA_HOME\bin\java.exe does not exist."
        }

        Write-Log "Checking the Hadoop Installation."
        if( -not (Test-Path $ENV:HADOOP_HOME\bin\hadoop.cmd))
        {
          Write-Log "HADOOP_HOME not set properly; $ENV:HADOOP_HOME\bin\hadoop.cmd does not exist" "Failure"
          throw "Install: HADOOP_HOME not set properly; $ENV:HADOOP_HOME\bin\hadoop.cmd does not exist."
        }

        ### $hcatInstallPath: the name of the folder containing the application, after unzipping
        $hcatInstallPath = Join-Path $env:HIVE_HOME "$FinalName"

        Write-Log "Installing Apache Hcatalog $FinalName to $hcatInstallPath"

        ### Create Node Install Root directory
        if( -not (Test-Path "$nodeInstallRoot"))
        {
            Write-Log "Creating Node Install Root directory: `"$nodeInstallRoot`""
            $cmd = "mkdir `"$nodeInstallRoot`""
            Invoke-CmdChk $cmd
        }
        ### $installToDir: the directory that contains the appliation, after unzipping
        $installToDir = Join-Path $env:HIVE_HOME "$FinalName"
        $installToBin = Join-Path "$installToDir" "bin"


        [Environment]::SetEnvironmentVariable( "TEMPLETON_HOME", "$installToDir", [EnvironmentVariableTarget]::Machine )
        $ENV:TEMPLETON_HOME = "$installToDir"

        $webhcatConfDir = Join-Path "$installToDir" "etc/webhcat"
        [Environment]::SetEnvironmentVariable( "WEBHCAT_CONF_DIR", "$webhcatConfDir", [EnvironmentVariableTarget]::Machine )

        ###
        ### Set HCAT_HOME environment variable
        ###
        Write-Log "Setting the HCAT_HOME environment variable at machine scope to `"$hcatInstallPath`""
        [Environment]::SetEnvironmentVariable("HCAT_HOME", $hcatInstallPath, [EnvironmentVariableTarget]::Machine)
        $ENV:HCAT_HOME = "$hcatInstallPath"
        Write-Log "Copying template files"
        $xcopy_cmd = "xcopy /EIYF `"$HDP_INSTALL_PATH\..\template\conf\webhcat-*`" `"$installToDir\etc\webhcat`""
        Invoke-CmdChk $xcopy_cmd

        ###
        ###  Setup default configuration
        ###
        $streamingJarUriPath = $ENV:HADOOP_HOME + "/share/hadoop/tools/lib/hadoop-streaming-@hadoop.version@.jar"
        $streamingJarUriPath = ConvertAbsolutePathToUri $streamingJarUriPath
        $templetonLibJarsPath = $ENV:HIVE_HOME + "/lib/zookeeper-@zookeeper.version@.jar"
        $templetonLibJarsPath = ConvertAbsolutePathToUri $templetonLibJarsPath
        $templetonJar = "$ENV:TEMPLETON_HOME\share\webhcat\svr\lib\hive-webhcat-$WebHCatVersion.jar"

        Configure "hcatalog" $NodeInstallRoot $serviceCredential @{
            "templeton.streaming.jar" = $streamingJarUriPath;
            "templeton.libjars" = $templetonLibJarsPath;
            "templeton.jar" = $templetonJar;}

        $pythonExePath = $env:SystemDrive + "\python27\python.exe"
        if ( Test-Path "$pythonExePath" )
        {
            Write-Log "Using python.exe from '$pythonExePath'"
            Configure "hcatalog" $NodeInstallRoot $serviceCredential @{
                "templeton.python" = $pythonExePath;}
        }

        ###
        ### Grant Hadoop user full access to $hadoopInstallToDir
        ###
        Write-Log "Granting hadoop user full permissions on $installToDir"
        $username = $serviceCredential.UserName
        GiveFullPermissions $installToDir $username

        ###
        ### Create the log directory
        ###
        $ttlogsdir = "$installToDir\logs"
        if (Test-Path ENV:TEMPLETON_LOG_DIR)
        {
            $ttlogsdir = "$ENV:TEMPLETON_LOG_DIR"
        }
        if ( -not (Test-Path "$ttlogsdir"))
        {
            Write-Log "Creating Templeton logs folder"
            New-Item -Path "$ttlogsdir" -type directory | Out-Null
        }
        GiveFullPermissions "$ttlogsdir" "Users"
        Write-Log "Finished installing Apache Hcatalog"

        if ($roles){
            CheckRole $roles @("templeton")
            ###
            ### Create Templeton Server service and grant user ACLS to start/stop
            ###
            Write-Log "Templeton Role Services: $roles"
            $allServices = $roles

            Write-Log "Installing services $allServices"

            foreach( $service in empty-null $allServices.Split(' '))
            {
              CreateAndConfigureHadoopService $service $HDP_RESOURCES_DIR $installToBin $serviceCredential

              Write-Log "Creating service config ${installToBin}\$service.xml"
              $cmd = "$installToBin\templeton.cmd --service $service > `"$installToBin\$service.xml`""
              Invoke-CmdChk $cmd
            }
        }

    }
    else
    {
        throw "Install: Unsupported compoment argument."
    }
}

###############################################################################
###
### Uninstalls Hadoop component.
###
### Arguments:
###     component: Component to be uninstalled, it can be "core, "hdfs" or "mapreduce"
###     nodeInstallRoot: Install folder (for example "C:\Hadoop")
###
###############################################################################
function Uninstall(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot
    )
{
    if ( $component -eq "hive" )
    {

        $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"

        ### $hiveInstallDir: the directory that contains the appliation, after unzipping
        $hiveInstallToDir = Join-Path "$nodeInstallRoot" "$FinalName"
        Write-Log "hiveInstallToDir: $hiveInstallToDir"

        ###
        ### Stop and delete servicess (no need to stop/delete the "hiveclient")
        ###
        foreach( $service in $DefaultRoles)
        {
            StopAndDeleteHadoopService $service
        }

        ###
        ### Delete the Hive directory
        ###
        Write-Log "Deleting $hiveInstallToDir"
        $cmd = "rd /s /q `"$hiveInstallToDir`""
        Invoke-Cmd $cmd

        ###
        ### Removing HIVE_HOME environment variable
        ###
        Write-Log "Removing ENV:HIVE_HOME, ENV:HIVE_OPTS at machine scope"
        [Environment]::SetEnvironmentVariable( "HIVE_HOME", $null, [EnvironmentVariableTarget]::Machine )
        [Environment]::SetEnvironmentVariable( "HIVE_OPTS", $null, [EnvironmentVariableTarget]::Machine )
        [Environment]::SetEnvironmentVariable( "HIVE_LIB_DIR", $null, [EnvironmentVariableTarget]::Machine )
	    [Environment]::SetEnvironmentVariable( "HIVE_CONF_DIR", $null, [EnvironmentVariableTarget]::Machine )
    }
    elseif ( $component -eq "hcatalog" )
    {
        $FinalName = "hcatalog"
        $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"

        Write-Log "Uninstalling Apache hcatalog $FinalName"
        ### $installToDir: the directory that contains the application, after unzipping
        $installToDir = Join-Path "$nodeInstallRoot" "$FinalName"
        Write-Log "installToDir: $installToDir"

        ###
        ### Stop and delete services
        ###
        foreach( $service in ("templeton"))
        {
            StopAndDeleteHadoopService $service
        }

        ###
        ### Delete install dir
        ###
        Write-Log "Deleting $installToDir"
        $cmd = "rd /s /q `"$installToDir`""
        Invoke-Cmd $cmd

        ### Removing HCAT_HOME environment variable
        Write-Log "Removing the HCAT_HOME environment variable"
        [Environment]::SetEnvironmentVariable( "HCAT_HOME", $null, [EnvironmentVariableTarget]::Machine )

        Write-Log "Successfully uninstalled Hcatalog"

        ###
        ### Removing Templeton environment variables
        ###
        Write-Log "Removing Templeton environment variables at machine scope"
        [Environment]::SetEnvironmentVariable( "TEMPLETON_HOME", $null, [EnvironmentVariableTarget]::Machine )
        [Environment]::SetEnvironmentVariable( "WEBHCAT_CONF_DIR", $null, [EnvironmentVariableTarget]::Machine )
    }
    else
    {
        throw "Uninstall: Unsupported compoment argument."
    }
}

###############################################################################
###
### Alters the configuration of the component.
###
### Arguments:
###     component: Component to be configured, e.g "core, "hdfs" or "mapreduce"
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     configs: Configuration that should be applied.
###              For example, @{"fs.checkpoint.edits.dir" = "C:\Hadoop\hdfs\2nne"}
###              Some configuration parameter are aliased, see ProcessAliasConfigOptions
###              for details.
###     aclAllFolders: If true, all folders defined in config file will be ACLed
###                    If false, only the folders listed in $configs will be ACLed.
###
###############################################################################
function Configure(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=2, Mandatory=$true )]
    $serviceCredential,
    [hashtable]
    [parameter( Position=3 )]
    $configs = @{},
    [bool]
    [parameter( Position=4 )]
    $aclAllFolders = $True
    )
{
    if ( $component -eq "hive" )
    {
        ### Process alias config options first
        $configs = ProcessAliasConfigOptions $component $configs

        $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"

        ### $hiveInstallDir: the directory that contains the appliation, after unzipping
        $hiveInstallToDir = Join-Path "$nodeInstallRoot" "$FinalName"
        $hiveInstallToBin = Join-Path "$hiveInstallToDir" "bin"
        Write-Log "hiveInstallToDir: $hiveInstallToDir"

        if( -not (Test-Path $hiveInstallToDir ))
        {
            throw "ConfigureHive: Install the hive before configuring it"
        }

        ###
        ### Apply configuration changes to hive-site.xml
        ###
        $xmlFile = Join-Path $hiveInstallToDir "conf\hive-site.xml"
        UpdateXmlConfig $xmlFile $configs
    }
    elseif ( $component -eq "hcatalog" )
    {
        $FinalName = "hcatalog"
        $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"

        ### $installToDir: the directory that contains the application, after unzipping
        $installToDir = Join-Path $env:HIVE_HOME "$FinalName"
        $installToBin = Join-Path "$installToDir" "bin"
        Write-Log "installToDir: $installToDir"

        if( -not (Test-Path $installToDir ))
        {
            throw "Configure: Install Templeton before configuring it"
        }

        ###
        ### Apply configuration changes to templeton-site.xml
        ###
        $xmlFile = Join-Path $installToDir "etc\webhcat\webhcat-site.xml"
        UpdateXmlConfig $xmlFile $configs
    }
    else
    {
        throw "Configure: Unsupported compoment argument."
    }
}

###############################################################################
###
### Start component services.
###
### Arguments:
###     component: Component name
###     roles: List of space separated service to start
###
###############################################################################
function StartService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $roles
    )
{
    Write-Log "Starting `"$component`" `"$roles`" services"

    if ( $component -eq "hive" )
    {
        ### Verify that roles are in the supported set
        CheckRole $roles $DefaultRoles

        foreach ( $role in $roles.Split(" ") )
        {
            if ( $role -eq "hiveclient" )
            {
                Write-Log "Hive client does not have any services"
            }
            else
            {
                Write-Log "Starting $role service"
                Start-Service $role
            }
        }
    }
    else
    {
        throw "StartService: Unsupported compoment argument."
    }
}

###############################################################################
###
### Stop component services.
###
### Arguments:
###     component: Component name
###     roles: List of space separated service to stop
###
###############################################################################
function StopService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $roles
    )
{
    Write-Log "Stopping `"$component`" `"$roles`" services"

    if ( $component -eq "hive" )
    {
        ### Verify that roles are in the supported set
        CheckRole $roles $DefaultRoles

        foreach ( $role in $roles.Split(" ") )
        {
            if ( $role -eq "hiveclient" )
            {
                Write-Log "Hive client does not have any services"
            }
            else
            {
                try
                {
                        Write-Log "Stopping $role "
                        if (Get-Service "$role" -ErrorAction SilentlyContinue)
                        {
                            Write-Log "Service $role exists, stopping it"
                            Stop-Service $role
                        }
                        else
                        {
                            Write-Log "Service $role does not exist, moving to next"
                        }
                }
                catch [Exception]
                {
                    Write-Host "Can't stop service $role"
                }
            }
        }
    }
    elseif ( $component -eq "hcatalog" )
    {
        ### Verify that roles are in the supported set
        CheckRole $roles @("templeton")
        foreach ( $role in $roles.Split(" ") )
        {
            try
            {
                Write-Log "Stopping $role "
                if (Get-Service "$role" -ErrorAction SilentlyContinue)
                {
                    Write-Log "Service $role exists, stopping it"
                    Stop-Service $role
                }
                else
                {
                    Write-Log "Service $role does not exist, moving to next"
                }
            }
            catch [Exception]
            {
                Write-Host "Can't stop service $role"
            }
        }
    }
    else
    {
        throw "StartService: Unsupported compoment argument."
    }
}

###############################################################################
###
### Installs Hive binaries.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###
###############################################################################
function InstallBinaries(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=2, Mandatory=$true )]
    $serviceCredential
    )
{
    $username = $serviceCredential.UserName

    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"

    ### $hiveInstallDir: the directory that contains the appliation, after unzipping
    $hiveInstallToDir = Join-Path "$nodeInstallRoot" "$FinalName"
    $hiveLogsDir = Join-Path "$hiveInstallToDir" "logs"
    if (Test-Path ENV:HIVE_LOG_DIR)
    {
        $hiveLogsDir = "$ENV:HIVE_LOG_DIR"
    }
    Write-Log "hiveLogsDir: $hiveLogsDir"


    Write-Log "Checking the JAVA Installation."
    if( -not (Test-Path $ENV:JAVA_HOME\bin\java.exe))
    {
      Write-Log "JAVA_HOME not set properly; $ENV:JAVA_HOME\bin\java.exe does not exist" "Failure"
      throw "Install: JAVA_HOME not set properly; $ENV:JAVA_HOME\bin\java.exe does not exist."
    }

    Write-Log "Checking the Hadoop Installation."
    if( -not (Test-Path $ENV:HADOOP_HOME\bin\winutils.exe))
    {
      Write-Log "HADOOP_HOME not set properly; $ENV:HADOOP_HOME\bin\winutils.exe does not exist" "Failure"
      throw "Install: HADOOP_HOME not set properly; $ENV:HADOOP_HOME\bin\winutils.exe does not exist."
    }

    ###
    ### Set HIVE_HOME environment variable
    ###
    Write-Log "Setting the HIVE_HOME environment variable at machine scope to `"$hiveInstallToDir`""
    [Environment]::SetEnvironmentVariable("HIVE_HOME", $hiveInstallToDir, [EnvironmentVariableTarget]::Machine)
    $ENV:HIVE_HOME = $hiveInstallToDir

    ### Hive Binaries must be installed before creating the services
    ###
    ### Begin install
    ###
    Write-Log "Installing Apache Hive $FinalName to $nodeInstallRoot"

    ### Create Node Install Root directory
    if( -not (Test-Path "$nodeInstallRoot"))
    {
        Write-Log "Creating Node Install Root directory: `"$nodeInstallRoot`""
        New-Item -Path "$nodeInstallRoot" -type directory | Out-Null
    }
    #Rename zip file
    Rename-Item "$HDP_RESOURCES_DIR\$FinalName-bin.zip" "$HDP_RESOURCES_DIR\$FinalName.zip"

    ###
    ###  Unzip Hadoop distribution from compressed archive
    ###
    Write-Log "Extracting Hive archive into $hiveInstallToDir"
    if ( Test-Path ENV:UNZIP_CMD )
    {
        ### Use external unzip command if given
        $unzipExpr = $ENV:UNZIP_CMD.Replace("@SRC", "`"$HDP_RESOURCES_DIR\$FinalName.zip`"")
        $unzipExpr = $unzipExpr.Replace("@DEST", "`"$nodeInstallRoot`"")
        ### We ignore the error code of the unzip command for now to be
        ### consistent with prior behavior.
        Invoke-Ps $unzipExpr
    }
    else
    {
        $shellApplication = new-object -com shell.application
        $zipPackage = $shellApplication.NameSpace("$HDP_RESOURCES_DIR\$FinalName.zip")
        $destinationFolder = $shellApplication.NameSpace($nodeInstallRoot)
        $destinationFolder.CopyHere($zipPackage.Items(), 20)
    }

    #Rename apache-$FinalName-bin to $FinalName
    Move-Item -Path "$nodeInstallRoot\$FinalName-bin" -destination "$nodeInstallRoot\$FinalName"

    ###
    ###  Copy template config files
    ###
    Write-Log "Copying template files"
    $xcopy_cmd = "xcopy /EIYF `"$HDP_INSTALL_PATH\..\template\bin\*`" `"$hiveInstallToDir\bin`""
    Invoke-Cmd $xcopy_cmd
    $xcopy_cmd = "xcopy /EIYF `"$HDP_INSTALL_PATH\..\template\conf\hive-*`" `"$hiveInstallToDir\conf`""
    Invoke-Cmd $xcopy_cmd

    ###
    ### Grant Hadoop user access to $hiveInstallToDir
    ###
    GiveFullPermissions $hiveInstallToDir $username

    ###
    ### ACL Hive logs directory such that machine users can write to it
    ###
    if( -not (Test-Path "$hiveLogsDir"))
    {
        Write-Log "Creating Hive logs folder"
        New-Item -Path "$hiveLogsDir" -type directory | Out-Null
    }

    GiveFullPermissions "$hiveLogsDir" "Users"

    $hadooplogdir = "$ENV:HADOOP_HOME\logs"
    if (Test-Path ENV:HADOOP_LOG_DIR)
    {
        $hadooplogdir = "$ENV:HADOOP_LOG_DIR"
    }
    $ENV:HADOOP_OPTS += " -Dfile.encoding=UTF-8"
    $ENV:HIVE_OPTS += " -hiveconf hive.querylog.location=$hiveLogsDir\history"
	$ENV:HIVE_OPTS += " -hiveconf hive.log.dir=$hiveLogsDir"

	[Environment]::SetEnvironmentVariable( "HIVE_OPTS", "$ENV:HIVE_OPTS", [EnvironmentVariableTarget]::Machine )
    [Environment]::SetEnvironmentVariable( "HIVE_LIB_DIR", "$ENV:HIVE_HOME\lib", [EnvironmentVariableTarget]::Machine )
	[Environment]::SetEnvironmentVariable( "HIVE_CONF_DIR", "$ENV:HIVE_HOME\conf", [EnvironmentVariableTarget]::Machine )

    Write-Log "Installation of Apache Hive binaries completed"
}


### Helper routing that converts a $null object to nothing. Otherwise, iterating over
### a $null object with foreach results in a loop with one $null element.
function empty-null($obj)
{
   if ($obj -ne $null) { $obj }
}

### Gives full permissions on the folder to the given user
function GiveFullPermissions(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $folder,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $username)
{
    Write-Log "Giving user/group `"$username`" full permissions to `"$folder`""
    $cmd = "icacls `"$folder`" /grant ${username}:(OI)(CI)F"
    Invoke-CmdChk $cmd
}

### Checks if the given space separated roles are in the given array of
### supported roles.
function CheckRole(
    [string]
    [parameter( Position=0, Mandatory=$true )]
    $roles,
    [array]
    [parameter( Position=1, Mandatory=$true )]
    $supportedRoles
    )
{
    foreach ( $role in $roles.Split(" ") )
    {
        if ( -not ( $supportedRoles -contains $role ) )
        {
            throw "CheckRole: Passed in role `"$role`" is outside of the supported set `"$supportedRoles`""
        }
    }
}

### Creates and configures the service.
function CreateAndConfigureHadoopService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $service,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $hdpResourcesDir,
    [String]
    [Parameter( Position=2, Mandatory=$true )]
    $serviceBinDir,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=3, Mandatory=$true )]
    $serviceCredential
)
{
    if ( -not ( Get-Service "$service" -ErrorAction SilentlyContinue ) )
    {
        Write-Log "Creating service `"$service`" as $serviceBinDir\$service.exe"
        $xcopyServiceHost_cmd = "copy /Y `"$hdpResourcesDir\serviceHost.exe`" `"$serviceBinDir\$service.exe`""
        Invoke-CmdChk $xcopyServiceHost_cmd

        #HadoopServiceHost.exe will write to this log but does not create it
        #Creating the event log needs to be done from an elevated process, so we do it here
        if( -not ([Diagnostics.EventLog]::SourceExists( "$service" )))
        {
            [Diagnostics.EventLog]::CreateEventSource( "$service", "" )
        }

        Write-Log "Adding service $service"
        $s = New-Service -Name "$service" -BinaryPathName "$serviceBinDir\$service.exe" -Credential $serviceCredential -DisplayName "Apache Hadoop $service"
        if ( $s -eq $null )
        {
            throw "CreateAndConfigureHadoopService: Service `"$service`" creation failed"
        }

        $cmd="$ENV:WINDIR\system32\sc.exe failure $service reset= 30 actions= restart/5000"
        Invoke-CmdChk $cmd

        $cmd="$ENV:WINDIR\system32\sc.exe config $service start= demand"
        Invoke-CmdChk $cmd

        Set-ServiceAcl $service
    }
    else
    {
        Write-Log "Service `"$service`" already exists, Removing `"$service`""
        StopAndDeleteHadoopService $service
        CreateAndConfigureHadoopService $service $hdpResourcesDir $serviceBinDir $serviceCredential
    }
}

### Forces a service to stop
function ForceStopService(
    [ServiceProcess.ServiceController]
    [Parameter( Position=0, Mandatory=$true )]
    $s
)
{
    Stop-Service -InputObject $s -Force
    $ServiceProc = Get-Process -Id (Get-WmiObject win32_Service | Where {$_.Name -eq $s.Name}).ProcessId -ErrorAction SilentlyContinue
    if( $ServiceProc.Id -ne 0 )
    {
        if( $ServiceProc.WaitForExit($WaitingTime) -eq $false )
        {
            Write-Log "Process $ServiceProc cannot be stopped. Trying to kill the process"
            Stop-Process $ServiceProc -Force  -ErrorAction Continue
        }
     }
}

### Stops and deletes the Hadoop service.
function StopAndDeleteHadoopService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $service
)
{
    Write-Log "Stopping $service"
    $s = Get-Service $service -ErrorAction SilentlyContinue
    if( $s -ne $null )
    {
        try
        {
            ForceStopService $s
        }
        catch
        {
            Write-Log "ForceStopService: Failed with exception: $($_.Exception.ToString())"
        }
        $cmd = "sc.exe delete $service"
        Invoke-Cmd $cmd
    }
}

### Runs the given configs thru the alias transformation and returns back
### the new list of configs with all alias dependent options resolved.
###
### Supported aliases:
###  core-site:
###     hive_derbyserver_host -> (javax.jdo.option.ConnectionURL, jdbc:derby://localhost:1527/metastore_db;create=true)
###
function ProcessAliasConfigOptions(
    [String]
    [parameter( Position=0 )]
    $component,
    [hashtable]
    [parameter( Position=1 )]
    $configs)
{
    $result = @{}
    Write-Log "ProcessAliasConfigOptions: Resolving `"$component`" configs"
    if ( $component -eq "hive" )
    {
        foreach( $key in empty-null $configs.Keys )
        {
            if ( $key -eq "hive_derbyserver_host" )
            {
                if($false -eq $configs.ContainsKey("javax.jdo.option.ConnectionURL"))
                {
                    $result.Add("javax.jdo.option.ConnectionURL",  "jdbc:derby://localhost:1527/metastore_db;create=true".Replace("localhost", $configs[$key]))
                }
            }
            elseif ( $key -eq "hive_hiveserver_host" )
            {
                if($false -eq $configs.ContainsKey("hive.metastore.uris"))
                {
                    $result.Add("hive.metastore.uris",  "thrift://localhost:10000".Replace("localhost", $configs[$key]))
                }
            }
            else
            {
                $result.Add($key, $configs[$key])
            }
        }
    }
    else
    {
        throw "ProcessAliasConfigOptions: Unknown component name `"$component`""
    }

    return $result
}
### Simple routine that converts a Windows absolute path to an URI
### For example: c:\some\path will be converted to file:///c:/some/path
function ConvertAbsolutePathToUri( $path )
{
    $path = $path.Replace("\", "/")
    $path = "file:///" + $path
    $path
}
### Returns the value of the given propertyName from the given xml file.
###
### Arguments:
###     xmlFileName: Xml file full path
###     propertyName: Name of the property to retrieve
function FindXmlPropertyValue(
    [string]
    [parameter( Position=0, Mandatory=$true )]
    $xmlFileName,
    [string]
    [parameter( Position=1, Mandatory=$true )]
    $propertyName)
{
    $value = $null

    if ( Test-Path $xmlFileName )
    {
        $xml = [xml] (Get-Content $xmlFileName)
        $xml.SelectNodes('/configuration/property') | ? { $_.name -eq $propertyName } | % { $value = $_.value }
        $xml.ReleasePath
    }

    $value
}

### Helper routine that updates the given fileName XML file with the given
### key/value configuration values. The XML file is expected to be in the
### Hadoop format. For example:
### <configuration>
###   <property>
###     <name.../><value.../>
###   </property>
### </configuration>
function UpdateXmlConfig(
    [string]
    [parameter( Position=0, Mandatory=$true )]
    $fileName,
    [hashtable]
    [parameter( Position=1 )]
    $config = @{} )
{
    $xml = New-Object System.Xml.XmlDocument
    $xml.PreserveWhitespace = $true
    $xml.Load($fileName)

    foreach( $key in empty-null $config.Keys )
    {
        $value = $config[$key]
        $found = $False
        $xml.SelectNodes('/configuration/property') | ? { $_.name -eq $key } | % { $_.value = $value; $found = $True }
        if ( -not $found )
        {
            $xml["configuration"].AppendChild($xml.CreateWhitespace("`r`n  ")) | Out-Null
            $newItem = $xml.CreateElement("property")
            $newItem.AppendChild($xml.CreateWhitespace("`r`n    ")) | Out-Null
            $newItem.AppendChild($xml.CreateElement("name")) | Out-Null
            $newItem.AppendChild($xml.CreateWhitespace("`r`n    ")) | Out-Null
            $newItem.AppendChild($xml.CreateElement("value")) | Out-Null
            $newItem.AppendChild($xml.CreateWhitespace("`r`n  ")) | Out-Null
            $newItem.name = $key
            $newItem.value = $value
            $xml["configuration"].AppendChild($newItem) | Out-Null
            $xml["configuration"].AppendChild($xml.CreateWhitespace("`r`n")) | Out-Null
        }
    }

    $xml.Save($fileName)
    $xml.ReleasePath
}

### Helper routine to emulate which
function Which($command)
{
    (Get-Command $command | Select-Object -first 1).Path
}

### Helper routine that ACLs the folders defined in folderList properties.
### The routine will look for the property value in the given xml config file
### and give full permissions on that folder to the given username.
###
### Dev Note: All folders that need to be ACLed must be defined in *-site.xml
### files.
function AclFoldersForUser(
    [string]
    [parameter( Position=0, Mandatory=$true )]
    $xmlFileName,
    [string]
    [parameter( Position=1, Mandatory=$true )]
    $username,
    [array]
    [parameter( Position=2, Mandatory=$true )]
    $folderList )
{
    $xml = [xml] (Get-Content $xmlFileName)

    foreach( $key in empty-null $folderList )
    {
        $folderName = $null
        $xml.SelectNodes('/configuration/property') | ? { $_.name -eq $key } | % { $folderName = $_.value }
        if ( $folderName -eq $null )
        {
            throw "AclFoldersForUser: Trying to ACLs the folder $key which is not defined in $xmlFileName"
        }

        ### TODO: Support for JBOD and NN Replication
        $folderParent = Split-Path $folderName -parent

        if( -not (Test-Path $folderParent))
        {
            Write-Log "AclFoldersForUser: Creating Directory `"$folderParent`" for ACLing"
            mkdir $folderParent

            ### TODO: ACL only if the folder does not exist. Otherwise, assume that
            ### it is ACLed properly.
            GiveFullPermissions $folderParent $username
        }
    }

    $xml.ReleasePath
}

###
### Public API
###
Export-ModuleMember -Function Install
Export-ModuleMember -Function Uninstall
Export-ModuleMember -Function Configure
Export-ModuleMember -Function StartService
Export-ModuleMember -Function StopService
Export-ModuleMember -Function UpdateXmlConfig
Export-ModuleMember -Function Which
