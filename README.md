# U12264017---MIT805-Exam
Commands used:
• Command to run the scripts using hadoop:
python MRMostVehicleMake.py -r hadoop –hadoop-streaming-jar /usr/hdp/current/hadoopmapreduce-
client/hadoop-streaming.jar inputfile.csv outputfile.csv
• Command to run and check errors on the scripts:
python violations.py inputfile.csv outputfile.csv
• To copy files from localhost to the sandbox VM:
pscp -P 2222 inputfile.csv maria dev@127.0.0.1:/home/maria dev
