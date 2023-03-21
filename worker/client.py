#
#   Client
#

import pickle
import socket
import subprocess
import gdown
from os.path import exists
import platform


LOCAL_FILE_NAME = "blend-file.blend"
BLENDER_COMMAND_UTILITY = 'blender'

match platform.system():
    case 'Darwin':
        BLEND_COMMAND_UTILITY = 'Blender'        


# Create a socket object
client_socket = socket.socket()

# Define host and port
# host = "127.0.0.1"
host = "10.1.82.126"
port = 5555

# Connect to the server
client_socket.connect((host, port))


#---------------------------------------------------------------------
# Function to run blender cli 
#---------------------------------------------------------------------
def render(source: str, start_frame: int, end_frame: int):
    
    print(f"blender -b {source} -f {start_frame}..{end_frame}")
    print(f"Rendering file...")

    out = subprocess.Popen(
        [BLENDER_COMMAND_UTILITY, '-b', source, '-o', './outputs/frame_#####', '-s', str(start_frame), '-e', str(end_frame),'-a'],                  
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=True
    )

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

    render(local_path, message['start_frame'], message['end_frame'])


# if __name__ == '__main__':
#     render("blend-file.blend", 1, 3)
