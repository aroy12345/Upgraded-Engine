@REM REM Submit the job using FlameSubmit simple crawl test
(

echo cd %cd%

@REM echo java -cp bin; flame.FlameSubmit localhost:9000 crawler.jar jobs.Crawler https://en.wikipedia.org/wiki/Capybara
echo java -cp bin; flame.FlameSubmit localhost:9000 crawler.jar jobs.Crawler http://simple.crawltest.cis5550.net/
 ) > flamesubmit.bat

start cmd.exe /k flamesubmit.bat