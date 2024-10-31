@echo off

set kvsWorkers=1

set flameWorkers=1

taskkill /im cmd.exe
javac --source-path src -d bin src/flame/*.java
javac --source-path src -d bin src/kvs/*.java
javac --source-path src -d bin src/generic/*.java
javac --source-path src -d bin src/jobs/*.java
javac --source-path src -d bin src/webserver/*.java
javac --source-path src -d bin src/tools/*.java


REM Package jar files

jar cvf crawler.jar -C bin jobs/Crawler.class

jar cvf pagerank.jar -C bin jobs/PageRank.class

jar cvf indexer.jar -C bin jobs/Indexer.class

REM Launch kvs coordinator	

(

 echo cd %cd%

 echo java -cp bin; kvs.Coordinator 8000

) > kvscoordinator.bat

REM Verify kvs coordinator batch file content

type kvscoordinator.bat

REM Launch kvs coordinator

start cmd.exe /k kvscoordinator.bat

REM Enable delayed expansion

setlocal enabledelayedexpansion

REM Launch kvs workers

for /l %%i in (1,1,%kvsWorkers%) do (

 set dir=worker%%i
 
REM if exist !dir! (
REM        rd /s /q !dir!
REM   )

 if not exist !dir! mkdir !dir!

(

 echo cd %cd%\!dir!

 echo java -cp ../bin; kvs.Worker 800%%i !dir! localhost:8000

 ) > kvsworker%%i.bat

 start cmd.exe /k kvsworker%%i.bat

)

REM Launch flame coordinator

(


 echo java -cp bin; flame.Coordinator 9000 localhost:8000

) > flamecoordinator.bat

start cmd.exe /k flamecoordinator.bat

REM Launch flame workers

for /l %%i in (1,1,%flameWorkers%) do (

 (



 echo java -cp bin; flame.Worker 900%%i localhost:9000

 ) > flameworker%%i.bat

 start cmd.exe /k flameworker%%i.bat

)

@REM REM Submit the job using FlameSubmit simple crawl test

@REM java -cp bin; flame.FlameSubmit localhost:9000 crawler.jar jobs.Crawler

@REM (

@REM echo cd %cd%

@REM echo java -cp bin;lib\webserver.jar;lib\kvs.jar;lib\flame.jar cis5550.flame.FlameSubmit localhost:9000 crawler.jar cis5550.jobs.Crawler http://simple.crawltest.cis5550.net/
@REM ) > flamesubmit.bat

@REM start cmd.exe /k flamesubmit.bat

@REM REM Submit the job using FlameSubmit advanced crawl test

@REM (

@REM echo cd %cd%

@REM echo java -cp bin;lib\webserver.jar;lib\kvs.jar;lib\flame.jar cis5550.flame.FlameSubmit localhost:9000 crawler.jar cis5550.jobs.Crawler http://advanced.crawltest.cis5550.net
@REM ) > flamesubmit.bat

@REM start cmd.exe /k flamesubmit.bat