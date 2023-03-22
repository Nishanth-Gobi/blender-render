# 
# Master
# 

import getopt
import sys
import socket
import threading
import pickle
from math import floor
from queue import Queue
import blend_render_info

#---------------------------------------------------------------------
# MACROs
#---------------------------------------------------------------------

SOURCE_FILE = ""
SOURCE_URL = ""
RENDER_BLOCK_SIZE = 10

MAX_CONNECTIONS = 5
# SERVER_ADDRESS = "127.0.0.1"
SERVER_ADDRESS = "10.1.82.126"
SERVER_PORT = 5555


#---------------------------------------------------------------------
# Command line arguments
#---------------------------------------------------------------------

argumentList = sys.argv[1:]

options = "hm:s:u:b:"
long_options = ["help", "max-connections=", "source=", "source-url=", "block-size="]

try:
    arguments, values = getopt.getopt(argumentList, options, long_options)
    
    for currentArgument, currentValue in arguments:

        if currentArgument in ("-h", "--help"):
            print("Displaying help") 
            exit(0)

        elif currentArgument in ("-m", "--max-connections"):
            MAX_CONNECTIONS = int(currentValue)
            print(f"Max-connection limit: {MAX_CONNECTIONS}")

        elif currentArgument in ("-s", "--source"):
            SOURCE_FILE = currentValue
            print(f"Source File: {SOURCE_FILE}")

        elif currentArgument in ("-u", "--source-url"):
            SOURCE_URL = currentValue
            print(f"Source URL: {SOURCE_URL}")

        elif currentArgument in ("-b", "--block-size"):
            RENDER_BLOCK_SIZE = int(currentValue)
            print(f"Render block size: {RENDER_BLOCK_SIZE}")

except getopt.error as err:
    print(str(err))


# Create a socket object and bind it to the server address and port
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((SERVER_ADDRESS, SERVER_PORT))

# Create the message queue
message_queue = Queue()


#---------------------------------------------------------------------
# Function that handles comms with each client
#---------------------------------------------------------------------
def handle_client(client_socket, client_address):

    # TODO: authenticate client?

    counter = 1
    while not message_queue.empty():

        # get message from queue
        message = message_queue.get()

        print('-'*50)
        print(f"{client_address}: \n Action no: {counter} \nStart frame: {message['start_frame']} \nEnd frame: {message['end_frame']}")
        print('-'*50)

        # send message to client
        client_socket.send(pickle.dumps(message))
        
        # get a response 
        data = client_socket.recv(1024)
        response = pickle.loads(data)

        # stop if the client want's to stop
        if not response['get_file']:
            print("{client_address}: Client want's to stop")
            break

    else:
        print(f"{client_address}: Message queue empty")
                
    
    print(f"{client_address}: Closing connection")
    # client_socket.shutdown(socket.SHUT_RDWR)
    # client_socket.close()
    return
        

#---------------------------------------------------------------------
# Function that accepts new clients
#---------------------------------------------------------------------
def listen_for_clients():
    server_socket.listen(MAX_CONNECTIONS)
    print(f"Server listening on {SERVER_ADDRESS}:{SERVER_PORT}...")

    while True:
        # Accept the client connection
        client_socket, client_address = server_socket.accept()
        print(f"New client connected: {client_address}")

        # Start a new thread to handle the client connection
        client_thread = threading.Thread(target=handle_client, args=(client_socket, client_address))
        client_thread.start()


#---------------------------------------------------------------------
# Function to get the number of frames in the source .blend file
#---------------------------------------------------------------------
def get_num_frames() -> int:

    frame_start, frame_end, scene = blend_render_info.read_blend_rend_chunk(SOURCE_FILE)[0]

    return frame_end - frame_start + 1


#---------------------------------------------------------------------
# Function to build the queue
#---------------------------------------------------------------------
def build_queue():

    num_frames = get_num_frames()
    # print(f"num_frames: {num_frames}")

    start_frame = 0
    for frame_block in range(floor(num_frames/10)):

        message_queue.put({
            'src': SOURCE_URL,
            'start_frame': start_frame,
            'end_frame': start_frame + RENDER_BLOCK_SIZE
        })

        start_frame += RENDER_BLOCK_SIZE + 1

    print(f"Message queue size: {message_queue.qsize()}")
    # print(*list(message_queue.queue), sep='\n\n')



# Start the thread for splitting the file
master_thread = threading.Thread(target=build_queue)
master_thread.start()

# Start the thread that listens for client connections
# listen_thread = threading.Thread(target=listen_for_clients)
# listen_thread.start()
