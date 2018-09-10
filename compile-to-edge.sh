#scp start-edge.sh pi@192.168.0.101:~
#scp start-edge.sh pi@192.168.0.102:~
#scp start-edge.sh pi@192.168.0.103:~
#scp start-edge.sh pi@192.168.0.104:~
mvn package
cd target
scp queryengine-1.0-SNAPSHOT.jar pi@192.168.0.101:~
#scp -r dependency-jars pi@192.168.0.101:~
scp queryengine-1.0-SNAPSHOT.jar pi@192.168.0.102:~
#scp -r dependency-jars pi@192.168.0.102:~
scp queryengine-1.0-SNAPSHOT.jar pi@192.168.0.103:~
#scp -r dependency-jars pi@192.168.0.103:~
scp queryengine-1.0-SNAPSHOT.jar pi@192.168.0.104:~
#scp -r dependency-jars pi@192.168.0.104:~
