#
#   Worker
#

import pickle
import socket
import subprocess
import gdown
import platform
import argparse
from tqdm import tqdm_gui

#---------------------------------------------------------------------
# Global defaults
#---------------------------------------------------------------------

SOURCE_FILE = "blend-file.blend"
SOURCE_URL = ""
BLEND_SOURCE = -1

BLENDER_COMMAND_UTILITY = None
BLENDER_SHELL_FLAG = None

SERVER_ADDRESS = "127.0.0.1"
SERVER_PORT = 5555

NO_OF_TRIALS = 3


#---------------------------------------------------------------------
# Command line arguments
#---------------------------------------------------------------------
parser = argparse.ArgumentParser()

parser.add_argument('-o', '--host', type=str, default=SERVER_ADDRESS, help='Set the server IP to connect to', required=True)

parser.add_argument('-p', '--port', type=int, default=SERVER_PORT, help='Set the server PORT to connect to')

parser.add_argument('-l', '--local-source-name', type=str, default=SOURCE_FILE, help='Set the name for the local copy of the .blend file')

args = parser.parse_args()


#---------------------------------------------------------------------
# Set the blender cli command based on user's platform
#---------------------------------------------------------------------
system = platform.system()

if system == 'Darwin':
    BLENDER_COMMAND_UTILITY = 'Blender'
    BLENDER_SHELL_FLAG = False        
elif system == 'Linux':
    BLENDER_COMMAND_UTILITY = 'blender'
    BLENDER_SHELL_FLAG = False        
elif system == 'Windows':
    BLENDER_COMMAND_UTILITY = 'blender'
    BLENDER_SHELL_FLAG = True        
else:
    raise ValueError(f"Unsupported platform: {system}")


#---------------------------------------------------------------------
# Create client socket and connect to the server
#---------------------------------------------------------------------
client_socket = socket.socket()

SERVER_ADDRESS = args.host
SERVER_PORT = args.port

client_socket.connect((SERVER_ADDRESS, SERVER_PORT))


#---------------------------------------------------------------------
# Function to run blender cli 
#---------------------------------------------------------------------
def render(source: str, start_frame: int, end_frame: int) -> int:
    
    print(f"{BLENDER_COMMAND_UTILITY} -b {source} -f {start_frame}..{end_frame}")
    print(f"Rendering file...")

    try:
        out = subprocess.Popen(
            [BLENDER_COMMAND_UTILITY, '-b', source, '-o', './outputs/frame_#####', '-s', str(start_frame), '-e', str(end_frame),'-a'],                  
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=BLENDER_SHELL_FLAG
        )

        stdout, stderr = out.communicate()  
        print(stdout)

    except:
        print("ERROR: Blender executable not found. This could be because,\n1. Blender is not installed in the system\n2. The PATH variable to the blender executable is not set.") 

        raise ValueError(f"Unsupported platform: {system}")

    return 0


#---------------------------------------------------------------------
# Function to download blender file from google drive 
#---------------------------------------------------------------------
def download(url: str):
    gdown.download(url=url, output=SOURCE_FILE, quiet=False, fuzzy=True)
    return 0


#---------------------------------------------------------------------
# Function to get source file from server
#---------------------------------------------------------------------
def get_source_file(client_socket, file_size):
    
    try:
        with open(SOURCE_FILE, 'wb') as file:
            writes = (file_size + 1023)/1024
            writes = int(writes)

            for counter in range(writes):
                
                recv_size = min(file_size - counter * 1024, 1024)

                data = client_socket.recv(recv_size)
                file.write(data)


        file.close()
        print("...Received file successfully")

        message = {
            'received': True
        }
        client_socket.send(pickle.dumps(message))
        return 0

    except:
        print("...Error receiving source file")
        return 1


#---------------------------------------------------------------------
# Get source
#---------------------------------------------------------------------
file_received_flag = False

data = client_socket.recv(1024)
response = pickle.loads(data)
print(f"received: {response}")

SOURCE_FILE = response['file_name']
file_size = response['file_size']


for trial in range(NO_OF_TRIALS):

    print(f"Getting the source file... trial: {trial} ")

    out = get_source_file(client_socket, file_size)

    if out == 0:
        file_received_flag = True
        break
    else:
        print("Retrying to receive source file...")


if not file_received_flag:

    data = client_socket.recv(1024)
    response = pickle.loads(data)    

    SOURCE_URL = response['src']
    download(SOURCE_URL)


#---------------------------------------------------------------------
# Driver loop
#---------------------------------------------------------------------
while True:

    # TODO: Introduce some error in status
    
    data = client_socket.recv(1024)
    message = pickle.loads(data)

    print(f"Received reply: {message}")

    try:
        res = render(SOURCE_FILE, message['start_frame'], message['end_frame'])
    
        request = {
            "status": True
        }
        client_socket.send(pickle.dumps(request))
        print("Getting next render command from server")

    except:
        request = {
            "status": False
        }
    
        client_socket.send(pickle.dumps(request))
        
        print("Stopping client...")    
        break
