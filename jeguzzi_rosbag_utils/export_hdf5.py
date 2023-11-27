import argparse
import os
from typing import Any, Dict, Callable, Optional, Collection

import numpy as np
import h5py

import nav_msgs.msg
import geometry_msgs.msg
import sensor_msgs.msg

try:
    import robomaster_msgs.msg
    H264Packet = robomaster_msgs.msg.H264Packet
except ImportError:
    H264Packet = None

try:
    from h264_msgs.msg import Packet
except:
    Packet = None

from .h264_video import make_video
from .reader import BagReader, header_stamp, sanitize


_readers: Dict[Any, Callable[[Any], np.ndarray]] = {}


def reader(t: Any) -> Callable[[Any], Callable[[Any], Optional[np.ndarray]]]:
    def g(f: Callable[[Any], np.ndarray]) -> Callable[[Any], Optional[np.ndarray]]:
        setattr(t, 'reader', f)
        return f
    return g


@reader(nav_msgs.msg.Odometry)
def odom(msg: nav_msgs.msg.Odometry) -> np.ndarray:
    ps = msg.pose.pose.position
    qs = msg.pose.pose.orientation
    vs = msg.twist.twist.linear
    ws = msg.twist.twist.angular
    return np.array([ps.x, ps.y, ps.z, qs.x, qs.y, qs.z, qs.w, vs.x, vs.y, vs.z, ws.x, ws.y, ws.z])


@reader(geometry_msgs.msg.PoseStamped)
def pose(msg: geometry_msgs.msg.PoseStamped) -> np.ndarray:
    ps = msg.pose.position
    qs = msg.pose.orientation
    return np.array([ps.x, ps.y, ps.z, qs.x, qs.y, qs.z, qs.w])


@reader(robomaster_msgs.msg.AudioData)
def audio(msg: robomaster_msgs.msg.AudioData) -> np.ndarray:
    return np.asarray(msg.data)


@reader(sensor_msgs.msg.JointState)
def joint_state(msg: sensor_msgs.msg.JointState) -> np.ndarray:
    return np.asarray(msg.position)


def import_topic(bag: BagReader, topic: str, msg_type: Any, store: h5py.File,
                 use_header_stamps: bool = True
                 ) -> bool:
    if not hasattr(msg_type, 'reader'):
        bag.logger.warning(f'Cannot import messages of type {msg_type}')
        return False
    datas = []
    stamps = []
    for _, msg, stamp in bag.get_messages(topics=[topic]):
        try:
            data = msg.reader()
            if data is not None:
                datas.append(data)
        except Exception as e:  # noqa
            bag.logger.warning(f'Cannot import message: {e}')
            return False
        if use_header_stamps:
            try:
                stamp = header_stamp(msg)
            except AttributeError:
                pass
        stamps.append(stamp)
    if datas:
        try:
            store.create_dataset(f"{sanitize(topic)}:data", data=datas)
        except ValueError:
            bag.logger.warning(f'Failed to import topic {topic}')
            return False
    if stamps:
        store.create_dataset(f"{sanitize(topic)}:stamp", data=stamps)
    return len(datas) > 0 or len(stamps) > 0


def export_bag(bag_file: str, topics: Collection[str] = [], exclude: Collection[str] = [],
               use_header_stamps: bool = True, should_make_video: bool = False,
               video_format: str = 'mp4') -> None:
    bag = BagReader(bag_file)
    bag_name = os.path.basename(os.path.normpath(bag_file))
    if not topics:
        topics = bag.type_map.keys()
    if exclude:
        topics = set(topics) - set(exclude)
    with h5py.File(f'{bag_name}.h5', 'w') as store:
        for topic in topics:
            bag.logger.info(f'Will try to import {topic}')
            msg_type = bag.get_message_type(topic)
            if msg_type in (H264Packet, Packet):
                if should_make_video:
                    out = f'{bag_name}__{sanitize(topic)}.{video_format}'
                    t = make_video(bag, topic, out, use_header_stamps)
                    if t:
                        store.create_dataset(f"{sanitize(topic)}:stamp", data=stamps)
            else:
                t = import_topic(bag, topic, msg_type, store, use_header_stamps)
            if t:
                bag.logger.info(f'imported {topic}')


def main(args: Any = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('bag_file', help='Bag file')
    parser.add_argument('--topics', help='topics', type=str, nargs='+', default="")
    parser.add_argument('--exclude', help='exclude topics', type=str, nargs='+', default="")
    parser.add_argument('--use_header_stamps', help='use stamps from headers', type=bool,
                        default=True)
    parser.add_argument('--make_video', help='make video', type=bool, default=False)
    parser.add_argument('--video_format', help='video format', type=str, default='mp4')
    arg = parser.parse_args(args)
    export_bag(arg.bag_file, arg.topics, arg.exclude, arg.use_header_stamps, arg.make_video,
               arg.video_format)
