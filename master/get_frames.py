#
# Usage
# blender -b /path/to/file.blend -P get_frames.py
#

import bpy

scene = bpy.context.scene
print("Scene %r frames: %d..%d = %d" % (scene.name, scene.frame_start, scene.frame_end, scene.frame_end - scene.frame_start + 1)) # frame_end is included

if __name__ == '__main__':
    print("ERR: File not meant to be run directly!")


#
# REFS
#   https://blender.stackexchange.com/questions/3141/get-number-of-frames-in-scene-from-the-command-line
#   https://blender.stackexchange.com/questions/5732/getting-started-no-module-called-bpy-outside-of-blender
#
