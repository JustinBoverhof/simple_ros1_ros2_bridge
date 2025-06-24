#!/usr/bin/env python
"""
Simplified Auto-Discovery ROS1 <-> ZeroMQ Bridge
Maintains all features with cleaner, more concise code
"""

import rospy
from std_msgs.msg import String, Int32, Float64, Bool
from geometry_msgs.msg import Twist, Point, Pose, PoseStamped
from sensor_msgs.msg import LaserScan
from nav_msgs.msg import Odometry, Path
import zmq
import json
import threading
import fnmatch

class ROS1ZMQBridge:
    def __init__(self):
        rospy.init_node('ros1_zmq_bridge')
        
        # ZeroMQ setup
        self.context = zmq.Context()
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.bind("tcp://*:5555")
        self.sub_socket.setsockopt(zmq.SUBSCRIBE, b"ros2")
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.bind("tcp://*:5556")
        
        # Configuration
        self.outgoing_filters = ['/test']
        self.incoming_prefix = '/spot'
        
        # Message type registry
        self.msg_types = {
            'std_msgs/String': String, 'std_msgs/Int32': Int32, 'std_msgs/Float64': Float64,
            'std_msgs/Bool': Bool, 'geometry_msgs/Twist': Twist, 'geometry_msgs/Point': Point,
            'geometry_msgs/Pose': Pose, 'geometry_msgs/PoseStamped': PoseStamped,
            'sensor_msgs/LaserScan': LaserScan, 'nav_msgs/Odometry': Odometry,
            'nav_msgs/Path': Path
        }
        
        # Storage
        self.ros1_subs = {}  # Outgoing subscribers
        self.ros1_pubs = {}  # Incoming publishers
        
        # Discovery timer and ZMQ listener
        rospy.Timer(rospy.Duration(5.0), self.discover_topics)
        threading.Thread(target=self.zmq_listener, daemon=True).start()
        
        rospy.loginfo(f'Bridge started. Filters: {self.outgoing_filters}')
    
    def discover_topics(self, event=None):
        """Find and subscribe to new outgoing topics"""
        try:
            for topic, topic_type in rospy.get_published_topics():
                if (not topic.startswith('/rosout') and 
                    self._matches_filters(topic) and 
                    topic not in self.ros1_subs):
                    self._create_outgoing_sub(topic, topic_type)
        except Exception as e:
            rospy.logwarn(f'Discovery error: {e}')
    
    def _matches_filters(self, topic):
        return any(fnmatch.fnmatch(topic, pattern) for pattern in self.outgoing_filters)
    
    def _create_outgoing_sub(self, topic, topic_type):
        """Create subscriber for outgoing topic"""
        if topic_type not in self.msg_types:
            rospy.logwarn(f'Unknown type: {topic_type}')
            return
        
        msg_class = self.msg_types[topic_type]
        callback = lambda msg: self._send_to_ros2(topic, msg, topic_type)
        
        self.ros1_subs[topic] = {
            'sub': rospy.Subscriber(topic, msg_class, callback, queue_size=10),
            'type': topic_type
        }
        
        rospy.loginfo(f'Subscribed to outgoing: {topic} ({topic_type})')
    
    def _send_to_ros2(self, topic, msg, topic_type):
        """Send ROS1 message to ROS2"""
        try:
            zmq_msg = {
                'topic': topic,
                'msg_type': topic_type.split('/')[-1],
                'full_type': topic_type,
                'data': self._msg_to_dict(msg),
                'timestamp': rospy.get_time(),
                'source': 'ros1'
            }
            
            topic_key = f"ros1{topic}"
            rospy.loginfo(f"Sending to ROS2: {topic_key}, data: {zmq_msg['data']}")  # DEBUG
            
            self.pub_socket.send_multipart([
                topic_key.encode(),
                json.dumps(zmq_msg).encode()
            ])
            
            rospy.loginfo(f"Successfully sent {topic} to ROS2")  # DEBUG
        except Exception as e:
            rospy.logerr(f'Error sending {topic}: {e}')
    
    def zmq_listener(self):
        """Listen for ROS2 messages and auto-create publishers"""
        while not rospy.is_shutdown():
            try:
                if self.sub_socket.poll(1000):
                    _, msg_data = self.sub_socket.recv_multipart(zmq.NOBLOCK)
                    zmq_msg = json.loads(msg_data.decode())
                    self._handle_incoming(zmq_msg)
            except (zmq.Again, Exception) as e:
                if not isinstance(e, zmq.Again):
                    rospy.logerr(f'ZMQ error: {e}')
    
    def _handle_incoming(self, zmq_msg):
        """Handle incoming message from ROS2"""
        try:
            topic = zmq_msg['topic']
            full_type = zmq_msg.get('full_type', zmq_msg['msg_type'])
            data = zmq_msg['data']
            
            # Get or create publisher
            pub_topic = f"{self.incoming_prefix}{topic}"
            if pub_topic not in self.ros1_pubs:
                self._create_incoming_pub(pub_topic, topic, full_type)
            
            # Publish message
            if pub_topic in self.ros1_pubs:
                pub_info = self.ros1_pubs[pub_topic]
                ros_msg = self._dict_to_msg(data, pub_info['msg_class'])
                if ros_msg:
                    pub_info['pub'].publish(ros_msg)
                    
        except Exception as e:
            rospy.logerr(f'Error handling incoming: {e}')
    
    def _create_incoming_pub(self, pub_topic, orig_topic, full_type):
        """Create publisher for incoming topic"""
        ros1_type = self._map_ros2_to_ros1_type(full_type)
        
        if ros1_type in self.msg_types:
            msg_class = self.msg_types[ros1_type]
            self.ros1_pubs[pub_topic] = {
                'pub': rospy.Publisher(pub_topic, msg_class, queue_size=10),
                'msg_class': msg_class,
                'orig_topic': orig_topic
            }
            rospy.loginfo(f'Created incoming publisher: {orig_topic} -> {pub_topic} ({ros1_type})')
        else:
            rospy.logwarn(f'Unknown type for {orig_topic}: {ros1_type}')
    
    def _map_ros2_to_ros1_type(self, ros2_type):
        """Map ROS2 message types to ROS1 equivalents"""
        mappings = {
            'geometry_msgs/msg/Twist': 'geometry_msgs/Twist',
            'geometry_msgs/msg/Point': 'geometry_msgs/Point',
            'geometry_msgs/msg/Pose': 'geometry_msgs/Pose',
            'geometry_msgs/msg/PoseStamped': 'geometry_msgs/PoseStamped',
            'std_msgs/msg/String': 'std_msgs/String',
            'std_msgs/msg/Int32': 'std_msgs/Int32',
            'std_msgs/msg/Float64': 'std_msgs/Float64',
            'std_msgs/msg/Bool': 'std_msgs/Bool',
            'sensor_msgs/msg/LaserScan': 'sensor_msgs/LaserScan',
            'nav_msgs/msg/Odometry': 'nav_msgs/Odometry',
            'nav_msgs/msg/Path': 'nav_msgs/Path'
        }
        return mappings.get(ros2_type, ros2_type)
    
    def _msg_to_dict(self, msg):
        """Convert ROS message to dict"""
        if hasattr(msg, '__slots__'):
            return {slot: self._msg_to_dict(getattr(msg, slot)) if hasattr(getattr(msg, slot), '__slots__')
                    else [self._msg_to_dict(item) if hasattr(item, '__slots__') else item for item in getattr(msg, slot)]
                    if isinstance(getattr(msg, slot), (list, tuple))
                    else getattr(msg, slot) for slot in msg.__slots__}
        return msg
    
    def _dict_to_msg(self, data, msg_class):
        """Convert dict to ROS message"""
        try:
            msg = msg_class()
            
            rospy.loginfo(f"Converting data {data} to message class {msg_class}")  # DEBUG
            
            for key, value in data.items():
                if hasattr(msg, key):
                    attr = getattr(msg, key)
                    rospy.loginfo(f"Setting {key} = {value} (type: {type(value)})")  # DEBUG
                    if hasattr(attr, '__slots__'):
                        setattr(msg, key, self._dict_to_nested(value, type(attr)))
                    else:
                        setattr(msg, key, value)
                else:
                    rospy.logwarn(f"Message class {msg_class} has no attribute '{key}'")
            
            rospy.loginfo(f"Final message: {msg}")  # DEBUG
            return msg
        except Exception as e:
            rospy.logerr(f'Dict to msg error: {e}')
            return None
    
    def _dict_to_nested(self, data, msg_class):
        """Convert dict to nested message"""
        if not isinstance(data, dict):
            return data
        msg = msg_class()
        for key, value in data.items():
            if hasattr(msg, key):
                setattr(msg, key, value)
        return msg
    
    def __del__(self):
        """Cleanup ZMQ resources"""
        for socket in [getattr(self, 'sub_socket', None), getattr(self, 'pub_socket', None)]:
            if socket:
                socket.close()
        if hasattr(self, 'context'):
            self.context.term()

def main():
    try:
        bridge = ROS1ZMQBridge()
        rospy.spin()
    except rospy.ROSInterruptException:
        pass

if __name__ == '__main__':
    main()

# Configuration:
# - Edit outgoing_filters for topics to send to ROS2
# - Edit incoming_prefix for ROS2 topics received  
# - Install: pip install pyzmq
# - Run: python ros1_zmq_bridge.py