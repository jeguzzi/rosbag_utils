import argparse
import json
import subprocess
import tempfile
from typing import Any, Iterator, List, Tuple

import numpy as np
import libmedia_codec

from .reader import BagReader, header_stamp, sanitize



def make_video(bag: BagReader, topic: str, out: str, use_header_stamps: bool = True) -> List[int]:
    video_decoder = libmedia_codec.H264Decoder()
    packets: List[np.ndarray] = []
    stamps: List[int] = []
    for _, msg, stamp in bag.get_messages(topics=[topic]):
        frames = video_decoder.decode(msg.data.tobytes())
        images = [np.frombuffer(frame, dtype=np.ubyte, count=len(frame)).reshape((height, width, 3))
                  for frame, width, height, ls in frames]
        if use_header_stamps:
            stamp = header_stamp(msg)
        for _ in images:
            stamps.append(stamp)
        if images:
            # only include packets that have images in them
            packets.append(np.frombuffer(msg.data, np.uint8))
    data = np.concatenate(packets)

    with tempfile.NamedTemporaryFile() as f:
        f.write(data.tobytes())
        subprocess.call(f'ffmpeg -v debug -y -i {f.name} -vcodec copy {out}', shell=True)
    metadata = json.loads(subprocess.check_output(f'ffprobe -print_format json -loglevel fatal -show_streams -count_frames -i {out}', shell=True))
    frame_count = int(metadata['streams'][0]['nb_frames'])
    return stamps[-frame_count:]


def main(args: Any = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('bag_file', help='Bag file')
    parser.add_argument('topic', help='Topic name')
    parser.add_argument('--out', help='video out file', default=".mp4")
    arg = parser.parse_args(args)
    bag_name = os.path.basename(os.path.normpath(arg.bag_file))
    out = f'{bag_name}__{sanitize(arg.topic)}{arg.out}'
    bag = BagReader(arg.bag_file)
    make_video(bag, arg.topic, out)
