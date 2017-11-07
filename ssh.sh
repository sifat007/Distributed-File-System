machine=$1
port=$2

ssh tarequl@$machine.cs.colostate.edu -t "cd /s/chopin/b/grad/tarequl/Desktop/CS555/HW4; echo $machine:; java -cp ./bin ChunkServer $port; bash --login"
