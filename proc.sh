echo "Clean up old directories"
hdfs dfs -rm -R /home/cloudera/daily/
sleep 5
echo "Running Sqoop Import"
sqoop job --exec job4daily
yes '' | sed 5q
echo "################"
echo "Finished Sqoop Import. Wait for 5 secs"
sleep 5

hdfs dfs -ls /home/cloudera/daily
hdfs dfs -rm -R /home/cloudera/daily/derived
echo
yes '' | sed 5q
echo "################"
echo "Look for Process Message"
sleep 5

python process.py
echo "Finished Spark Data Processing....Yeah."
echo
yes '' | sed 5q
echo "################"
sleep 5
echo "Starting Sqoop Export"
sqoop job --exec job4dailyExport
