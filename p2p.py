# Python3

from socket import *
import threading
import time
import datetime as dt
import sys

# Class node is where all the data of the Peer will be stored
class node:
    def __init__(self, peer, firstSuccessor, secondSuccessor, pingInterval):
        self.peer = peer
        self.port = peer + 12000
        self.firstSuccessor = firstSuccessor
        self.secondSuccessor = secondSuccessor
        self.firsPredecessor = -1
        self.secondPredecessor = -1
        self.pingInterval = pingInterval
        self.firstSuccessorNAK = 0
        self.secondSuccessorNAK = 0
        self.files = []

# Global access to the Peer's data which will be an object of class node    
Peer = None  

# Initialising the Peer
def init(peer, firstSuccessor, secondSuccessor, pingInterval):
    newNode = node(peer, firstSuccessor, secondSuccessor, pingInterval)
    global Peer
    Peer = newNode

# Joing of Peer through a knownPeer
def join(peer, knownPeer, pingInterval):
    global Peer
    Peer = node(peer,-1,-1,pingInterval)

    msg = f'join/{peer}.{knownPeer}.{pingInterval}'
    TCPClient(knownPeer, msg)

# Pinging successors to check if they still alive
def ping():
    global Peer
    UDPServer_thread=threading.Thread(name="UDPServer", target=UDPServer)
    UDPServer_thread.daemon=True
    UDPServer_thread.start()

    while True:
        time.sleep(Peer.pingInterval)
        UDPClient()

# Peer quiting with notice, informing its predecessor of it leaving
def depart_graceful():
    global Peer
    msg1 = f'depart/{Peer.peer}.{Peer.firstSuccessor}.{Peer.secondSuccessor}'
    msg2 = f'depart/{Peer.peer}.{Peer.firsPredecessor}.{Peer.firstSuccessor}'
    peer1 = Peer.firsPredecessor
    peer2 = Peer.secondPredecessor
    TCPClient(peer1, msg1)
    TCPClient(peer2, msg2)

# Inserting a file into the DHT
def data_insert(fileNumber, fileHash):
    global Peer
    msg = f'store/{fileNumber}.{fileHash}'
    TCPClient(Peer.peer, msg)

# Retriving the file from the DHT
def data_retrive(fileNumber, fileHash):
    fileNumber, fileHash
    msg = f'retrive/{fileNumber}.{fileHash}.{Peer.peer}'
    print(f'File request for {fileNumber} has been sent to my successor')
    TCPClient(Peer.firstSuccessor, msg)

# ******************************** Servers and Clients ********************************

# TCPServer which runs all the time - handles everything except Pinging and abrupt quit
def TCPServer():
    global Peer

    # Creating a server socket
    server = socket(AF_INET, SOCK_STREAM)
    # Waiting until Peer has been initialised
    while (Peer == None):
        time.sleep(0.1) 
        
    server.bind(('localhost', Peer.port))
    server.listen(5)
    while True:
        clientSocket, addr = server.accept()
        data = clientSocket.recv(4096).decode()
        # Exploiting data structure : data = 'task/args' where args = 'Variable1.Variable2.Variable3'
        task = data.split('/')[0]
        args = data.split('/')[1]

        # If task sent is for join - checks if peer should be added here or sends it to its
        # successor
        if task == 'join':
            peer = int(args.split('.')[0])
            knownPeer = int(args.split('.')[1])
            pingInterval = float(args.split('.')[2])
            # DHT = 2 -> 4 -> 6 -> 8 -> 2 ...
            # 1st If: add Peer 3 (between 2 and 4)
            # 2nd If: add Peer 1 (between 8 and 2)
            # 3rd If: add Peer 9 (between 8 and 2) 
            if (peer > Peer.peer and peer < Peer.firstSuccessor)\
                or (peer < Peer.peer and peer < Peer.firstSuccessor and Peer.peer > Peer.firstSuccessor) \
                or (peer > Peer.peer and peer > Peer.firstSuccessor and Peer.peer > Peer.firstSuccessor):
                # get current first and second successor to become 'peer's new first and second
                # set own first as new second and 'peer' and new first
                # send first predecessor to update its secondSuccessor to be 'peer'
                
                print(f'Peer {peer} Join request received')
                
                # send to OG TCPServer of 'peer' with data f'init/{peer}.{firstSuccessor}.{secondSuccessor}.{pingInterval}
                firstSuccessor = Peer.firstSuccessor
                secondSuccessor = Peer.secondSuccessor
                tempMsg = f'init/{peer}.{firstSuccessor}.{secondSuccessor}.{pingInterval}'
                TCPClient(peer, tempMsg)
                
                Peer.secondSuccessor = Peer.firstSuccessor
                Peer.firstSuccessor = peer
                print(f'My new first successor is Peer {Peer.firstSuccessor}')
                print(f'My new second successor is Peer {Peer.secondSuccessor}')

                # send to firstPredecessor to update its secondSuccessor to peer
                # f'update/{peer}'
                tempMsg = f'update/{peer}'
                TCPClient(Peer.firsPredecessor, tempMsg)

            else : 
                # Forwarding peer to its successor
                print(f'Peer {peer} Join request forwarded to my successor')
                # send to firstSuccessor
                TCPClient(Peer.firstSuccessor,data)
                pass
        # This is to update the predecessor of the Peer which adds the newPeer after it
        elif task == 'update':
            print('Successor Change request received')
            Peer.secondSuccessor = int(args[0])
            print(f'My new first successor is Peer {Peer.firstSuccessor}')
            print(f'My new second successor is Peer {Peer.secondSuccessor}')
        # Here the peer which joins through knownPeer is made to initialize
        elif task == 'init':
            peer = int(args.split('.')[0])
            firstSuccessor = int(args.split('.')[1])
            secondSuccessor = int(args.split('.')[2])
            pingInterval = float(args.split('.')[3])
            init(peer, firstSuccessor, secondSuccessor, pingInterval)
        # Here the predecessor of the Peer quiting is updated    
        elif task == 'depart':
            peerDeparting = int(args.split('.')[0])
            firstSuccessor = int(args.split('.')[1])
            secondSuccessor = int(args.split('.')[2])

            print(f'Peer {peerDeparting} will depart from the network')
            Peer.firstSuccessor = firstSuccessor
            Peer.secondSuccessor = secondSuccessor
            print(f'My new first successor is Peer {Peer.firstSuccessor}')
            print(f'My new second successor is Peer {Peer.secondSuccessor}')
        # This is used to sent to the respective first/second successor to get its
        # successor to be sent to the Peer whose respective successor died
        elif task == 'abrupt':
            peer = int(args.split('.')[0])
            location = int(args.split('.')[1])
            msg = f'abruptUpdate/{peer}.{location}.{Peer.firstSuccessor}.{Peer.peer}'
            TCPClient(peer, msg)
        # The update request sent to update its respective Peer which died
        elif task == 'abruptUpdate':
            newSecondSuccessor = int(args.split('.')[2])
            if (newSecondSuccessor == Peer.secondSuccessor):
                time.sleep(0.5)
                msg = f'abrupt/{Peer.peer}.2'
                TCPClient(int(args.split('.')[3]), msg)
            else:
                Peer.secondSuccessorNAK = 0
                Peer.secondSuccessor = newSecondSuccessor

                print(f'My new first successor is Peer {Peer.firstSuccessor}')
                print(f'My new second successor is Peer {Peer.secondSuccessor}')
        # Stores the file sent here: checks if this is the currect Peer to store - else 
        # forwardes it to it's peer
        elif task == 'store': 
            fileNumber = int(args.split('.')[0])
            fileHash = int(args.split('.')[1])
            if Peer.peer >= fileHash:
                print(f'Store {fileNumber} request accpeted')
                Peer.files.append(fileNumber)
                fileName = '' + str(fileNumber) + '.pdf'
                f = open(fileName, 'w+')
                f.write('a' * 1024)
                f.close()
            else:
                print(f'Store {fileNumber} request forwarded to my successor')
                TCPClient(Peer.firstSuccessor, data)
        # File retrive function - checks where the file is and send its content back 
        # to tge peer that requested the file 
        elif task == 'retrive':
            fileNumber = int(args.split('.')[0])
            fileHash = int(args.split('.')[1])
            peerRequested = int(args.split('.')[2])
            
            if Peer.peer >= fileHash:
                print(f'File {fileNumber} is stored here')
                fileName = '' + str(fileNumber) + '.pdf'
                
                f = open(fileName, 'r')
                flag = 0
                print(f'Sendinf file {fileName} to Peer {peerRequested}')
                read = f.read(100)
                while read:
                    msg = f'read/{fileNumber}.{Peer.peer}.{read}.{flag}'
                    TCPClient(peerRequested, msg)
                    time.sleep(0.1)
                    flag += 1
                    read = f.read(1024)
                print('The file has been sent')
                flag = -1
                msg = f'read/{fileNumber}.{Peer.peer}..{flag}'
                TCPClient(peerRequested, msg)
                f.close()
            else:
                print(f'Request for File {fileNumber} has been received, but the file is not stored here')
                TCPClient(Peer.firstSuccessor, data)
        # Read function for the Peer that requested the file to be read - sent here from the 
        # location where the file is actually saved
        elif task == 'read':
            fileNumber = int(args.split('.')[0])
            peerSent = int(args.split('.')[1])
            read = args.split('.')[2]
            flag = int(args.split('.')[3])
            if flag == 0:
                print(f'Receiving File {fileNumber} from Peer {peerSent}')
            
            if (flag >= 0):
                fileName = 'received_' + str(fileNumber) + '.pdf'
                f = open(fileName, 'a')
                f.write(read)
                f.close()

            if flag == -1:
                print(f'File {fileNumber} received')
        # Closing the socket
        clientSocket.close()
       
# Used to send all TCP msgs from the Peer to its known successor/predecessor
def TCPClient(peer, msg):
    port = peer + 12000
    client = socket(AF_INET, SOCK_STREAM)
    client.connect(('localHost', port))
    client.send(msg.encode())
    client.close()

# Server which is used to respond to the ping requests w
def UDPServer():
    #Server will run on this port
    global Peer

    serverPort = Peer.port

    def recv_handler():
        while(1):
            message, clientAddress = serverSocket.recvfrom(2048)
            #received data from the client, now we know who we are talking with
            message = message.decode()
            msgToPrint = message.split('/')[0]
            
            if int(message.split('/')[1]) == 1:
                    Peer.firsPredecessor = int(msgToPrint.split(' ')[-1])
            if int(message.split('/')[1]) == 2:
                    Peer.secondPredecessor = int(msgToPrint.split(' ')[-1])

            print(msgToPrint)
            msg = f'Ping response received from Peer {Peer.peer}'
            serverSocket.sendto(msg.encode(), clientAddress)

    serverSocket = socket(AF_INET, SOCK_DGRAM)
    serverSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    serverSocket.bind(('localhost', serverPort))

    recv_thread=threading.Thread(name="RecvHandler", target=recv_handler)
    recv_thread.daemon=True
    recv_thread.start()


    #this is the main thread
    while True:
        time.sleep(0.1)

# Here the ping requests are sent out, ack for response is received also 
# takes care of NAKs - abrupt quit
UDPClientSuccess = 0
def UDPClient():
    global Peer
    global UDPClientSuccess
    UDPClientSuccess = 0
    serverName = 'localhost'
    serverPort1 = Peer.firstSuccessor + 12000
    serverPort2 = Peer.secondSuccessor + 12000
    
    clientSocket1 = socket(AF_INET, SOCK_DGRAM)
    clientSocket2 = socket(AF_INET, SOCK_DGRAM)
    # Sending ping requests
    def send():
        global UDPClientSuccess
        print(f"Ping request sent to Peer {Peer.firstSuccessor} and {Peer.secondSuccessor}")
        message1 = f"Ping request message received from Peer {Peer.peer}/1"
        message2 = f"Ping request message received from Peer {Peer.peer}/2"
        clientSocket1.sendto(message1.encode(),(serverName, serverPort1))
        clientSocket2.sendto(message2.encode(),(serverName, serverPort2))

        UDPClientSuccess += 1
    # Receiving ping response - also checking for successors abrupt exit
    def get():
        global UDPClientSuccess
        # Getting ping from first successor 
        try:
            clientSocket1.settimeout(0.5)
            receivedMessage1, serverAddress1 = clientSocket1.recvfrom(2048)
            print(receivedMessage1.decode())
            Peer.firstSuccessorNAK = 0
            UDPClientSuccess += 1
        except:
            UDPClientSuccess +=1
            Peer.firstSuccessorNAK += 1

        # Getting ping from second successor
        try:            
            clientSocket2.settimeout(0.5)
            receivedMessage2, serverAddress2 = clientSocket2.recvfrom(2048)
            print(receivedMessage2.decode())
            Peer.secondSuccessorNAK = 0
            UDPClientSuccess += 1
        except:
            UDPClientSuccess += 1
            Peer.secondSuccessorNAK += 1

    # Checking if 2 consecutive responses have been missed
    if Peer.firstSuccessorNAK >= 2:
        print(f'Peer {Peer.firstSuccessor} is no longer alive')
        msg = f'abrupt/{Peer.peer}.1'
        Peer.firstSuccessorNAK = Peer.secondSuccessorNAK
        Peer.firstSuccessor = Peer.secondSuccessor
        TCPClient(Peer.secondSuccessor, msg)
        time.sleep(0.1)
    # Checking if 2 consecutive responses have been missed
    elif Peer.secondSuccessorNAK >= 2:
        time.sleep(1) # Just to get the peer's firstSuccessor can update first
        print(f'Peer {Peer.secondSuccessor} is no longer alive')
        msg = f'abrupt/{Peer.peer}.2'
        TCPClient(Peer.firstSuccessor, msg)
    

    get_thread=threading.Thread(name="get",target=get)
    get_thread.daemon=True
    get_thread.start()

    send_thread=threading.Thread(name="send",target=send)
    send_thread.daemon=True
    send_thread.start()
    
    while UDPClientSuccess < 3:  
        time.sleep(0.5)

# ***************************** main *********************************
if __name__ == "__main__":
    # Starting the TCP Server
    TCPServer_thread=threading.Thread(name="TCPServer", target=TCPServer)
    TCPServer_thread.daemon=True
    TCPServer_thread.start()
    
    if sys.argv[1] == 'init':
        peer = int(sys.argv[2])
        firstSuccessor = int(sys.argv[3])
        secondSuccessor = int(sys.argv[4])
        pingInterval = float(sys.argv[5])

        init(peer,firstSuccessor, secondSuccessor, pingInterval)

    if sys.argv[1] == 'join':
        peer = int(sys.argv[2])
        knownPeer = int(sys.argv[3])
        pingInterval = float(sys.argv[4])
        
        join(peer, knownPeer, pingInterval)
        # Wait some time to get the peer online
        time.sleep(2)

    ping_thread=threading.Thread(name="ping", target=ping)
    ping_thread.daemon=True
    ping_thread.start()
    
    while (True):
        case = (input(''))
        if case == 'quit':
            depart_graceful()
            exit(0)
        if case.split(' ')[0] == 'store':
            fileNumber = int(case.split(' ')[1])
            fileHash = fileNumber % 256
            data_insert(fileNumber, fileHash)
        if case.split(' ')[0] == 'request':
            fileNumber = int(case.split(' ')[1])
            fileHash = fileNumber % 256
            data_retrive(fileNumber, fileHash)
