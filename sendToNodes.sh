#!/bin/bash
sshpass -p "raspberry" scp -r pi@192.168.0.101:~/ /storage-node.py
sshpass -p "raspberry" scp -r pi@192.168.0.102:~/ /storage-node.py
sshpass -p "raspberry" scp -r pi@192.168.0.103:~/ /storage-node.py
sshpass -p "raspberry" scp -r pi@192.168.0.104:~/ /storage-node.py
