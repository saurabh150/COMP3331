#Python 3
#Usage: python3 UDPClient3.py localhost 12000
#coding: utf-8
from socket import *
import sys
import threading
import time

#Server would be running on the same host as Client
serverName = sys.argv[1]
serverPort = int(sys.argv[2])

clientSocket = socket(AF_INET, SOCK_DGRAM)
def send():
    global serverName, serverPort, clientSocket
    while (True):
        message = input("Please type Subscribe\n")

        clientSocket.sendto(message.encode(),(serverName, serverPort))
        ##if message == 'Unsubscribe':
          #  clientSocket.close()
#wait for the reply from the server
def get():
    global clientSocket
    receivedMessage, serverAddress = clientSocket.recvfrom(2048)
    print (serverAddress)
    if (receivedMessage.decode()=='Subscription successfull'):
        #Wait for 10 back to back messages from server
        for _ in range(10):
            receivedMessage, serverAddress = clientSocket.recvfrom(2048)
            print(receivedMessage.decode())
        message = 'Unsubscribe'    
        clientSocket.sendto(message.encode(),(serverName, serverPort))
#prepare to exit. Send Unsubscribe message to server
'''
message='Unsubscribe'
clientSocket.sendto(message.encode(),(serverName, serverPort))
clientSocket.close()
# Close the socket
'''

recv_thread=threading.Thread(name="send", target=send)
recv_thread.daemon = True
recv_thread.start()

send_thread=threading.Thread(name="get",target=get)
send_thread.daemon = True
send_thread.start()

#this is the main thread
while True:
    time.sleep(0.1)