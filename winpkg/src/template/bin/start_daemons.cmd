@echo off
@rem Licensed to the Apache Software Foundation (ASF) under one or more
@rem contributor license agreements.  See the NOTICE file distributed with
@rem this work for additional information regarding copyright ownership.
@rem The ASF licenses this file to You under the Apache License, Version 2.0
@rem (the "License"); you may not use this file except in compliance with
@rem the License.  You may obtain a copy of the License at
@rem
@rem     http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.

@rem set the hwi jar and war files
pushd %HIVE_HOME%\lib
for /f %%a IN ('dir /b hive-hwi-*') do (
	call :ProcessFileName %%a
)
popd

setlocal enabledelayedexpansion

echo Starting Hive services

if not defined HIVE_SERVICES (
  @rem TODO: Revisit the default
  set HIVE_SERVICES= hwi derbyserver metastore  hiveserver2
)

@rem
@rem  Start services
@rem
for %%i in (%HIVE_SERVICES%) do (
  echo Starting %%i
  "%windir%\system32\net.exe" start %%i
    if %%i==metastore (
    echo Wait 10s for metastore db setup
    ping 1.1.1.1 -n 1 -w 10000 > NUL
  )
)
goto :EOF
endlocal

@rem process the hwi files
:ProcessFileName
	set temp=%1
	set temp=%temp:~-3%

	if %temp%==jar set HWI_JAR_FILE=lib\%1

	if %temp%==war set HWI_WAR_FILE=lib\%1
goto :EOF
