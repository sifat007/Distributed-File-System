

while read line ; do
	A="$(cut -d':' -f1 <<<"$line")"
	B="$(cut -d':' -f2 <<<"$line")"
	echo $A
	gnome-terminal -e "bash -c 'ssh tarequl@$A -t \"cd /s/chopin/b/grad/tarequl/Desktop/CS555/HW4; echo $A:; java -cp ./bin Controller; bash --login\"';bash"
	
	sleep 2
done < controller_node.txt

while read line ; do
	A="$(cut -d' ' -f1 <<<"$line")"
	B="$(cut -d' ' -f2 <<<"$line")"
	if [ $A = "#" ]; then 
		break 
	fi
	echo $A
	gnome-terminal -e "bash -c './ssh.sh $A $B';bash"
	sleep 2
done < config.txt



