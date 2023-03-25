#
#   Worker
#

import pickle
import socket
import subprocess
import gdown
from os.path import exists
import platform
import argparse


#---------------------------------------------------------------------
# Global variables
#---------------------------------------------------------------------
LOCAL_FILE_NAME = "blend-file.blend"
BLENDER_COMMAND_UTILITY = None
SERVER_ADDRESS = "127.0.0.1"
SERVER_PORT = 5555


#---------------------------------------------------------------------
# Command line arguments
#---------------------------------------------------------------------
parser = argparse.ArgumentParser()

parser.add_argument('-o', '--host', type=str, default=SERVER_ADDRESS, help='Set the server IP to connect to', required=True)

parser.add_argument('-p', '--port', type=int, default=SERVER_PORT, help='Set the server PORT to connect to')

parser.add_argument('-l', '--local-source-name', type=str, default=LOCAL_FILE_NAME, help='Set the name for the local copy of the .blend file')

args = parser.parse_args()


#---------------------------------------------------------------------
# Set the blender cli command based on user's platform
#---------------------------------------------------------------------
system = platform.system()

if system == 'Darwin':
    BLENDER_COMMAND_UTILITY = 'Blender'        
elif system == 'Linux':
    BLENDER_COMMAND_UTILITY = 'blender'
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
    
    print(f"blender -b {source} -f {start_frame}..{end_frame}")
    print(f"Rendering file...")

    try:
        out = subprocess.Popen(
            [BLENDER_COMMAND_UTILITY, '-b', source, '-o', './outputs/frame_#####', '-s', str(start_frame), '-e', str(end_frame),'-a'],                  
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=True
        )
    except:
        print("ERROR: Blender executable not found. This could be because,\n1. Blender is not installed in the system\n2. The PATH variable to the blender executable is not set.")

        return 1

    stdout, stderr = out.communicate()  
    print(stdout)

    return 0


#---------------------------------------------------------------------
# Function to run blender cli 
#---------------------------------------------------------------------
def download(url: str):
    output = "blend-file.blend"
    gdown.download(url=url, output=output, quiet=False, fuzzy=True)
    return output


#---------------------------------------------------------------------
# Driver loop
#---------------------------------------------------------------------
while True:

    request = {
        "status": "online", 
        "get_file": True
    }
    
    client_socket.send(pickle.dumps(request))
    
    data = client_socket.recv(1024)
    message = pickle.loads(data)
    print(f"Received reply: {message}")

    local_path = "blend-file.blend"

    if not exists(LOCAL_FILE_NAME):
        local_path = download(message['src'])

    res = render(local_path, message['start_frame'], message['end_frame'])

    if res==1:
        print("\nStopping client...")    
        break
