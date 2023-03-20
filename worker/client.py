#
#   Client
#

import pickle
import socket
import subprocess
import gdown

# Create a socket object
client_socket = socket.socket()

# Define host and port
host = "127.0.0.1"
# host = "10.1.82.126"
port = 5555

# Connect to the server
client_socket.connect((host, port))

file_downloaded = True # TODO: set to False

#---------------------------------------------------------------------
# Function to run blender cli 
#---------------------------------------------------------------------
def render(source: str, start_frame: int, end_frame: int):
    
    print(f"blender -b {source} -f {start_frame}..{end_frame}")
    print(f"Rendering file...")
    out = subprocess.Popen(
        ['blender', '-b', source, '-o', './outputs/frame_#####', '-s', str(start_frame), '-e', str(end_frame),'-a'],                  
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
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

    if not file_downloaded:
        local_path = download(message['src'])
    else:
        local_path = "blend-file.blend"

    render(local_path, message['start_frame'], message['end_frame'])

    print(client_socket.recv(1024).decode("utf-8"))

    break   


# if __name__ == '__main__':
#     render("blend-file.blend", 1, 3)
