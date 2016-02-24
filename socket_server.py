#!/usr/bin/env python
#-*- coding:utf-8 -*-

# Written by Vamei
# Server side
import socket

# Address
HOST = ''
PORT = 8000

reply = 'Yes'

# Configure socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((HOST, PORT))

# passively wait, 3: maximum number of connections in the queue
s.listen(3)
# accept and establish connection
conn, addr = s.accept()
# receive message
request = conn.recv(1024)
print 'request is: ',request
print 'Connected by', addr
# send message
conn.sendall(reply)
# close connection
conn.close()
