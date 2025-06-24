#!/usr/bin/env python3
"""
Simplified Auto-Discovery ROS2 <-> ZeroMQ Bridge
Maintains all features with cleaner, more concise code
IP addresses are now loaded from external config file
"""

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy
import zmq
import json
import time
import threading
import importlib
import fnmatch
import os
import yaml

class ROS2ZMQBridge(Node):
    def __init__(self):
        super().__init__('ros2_zmq_bridge')
        
        # Load configuration
        self.config = self._load_config()
        
        # ZeroMQ setup with configurable IP, hardcoded ports
        self.zmq_context = zmq.Context()
        self.pub_socket = self.zmq_context.socket(zmq.PUB)
        self.pub_socket.connect(f"tcp://{self.config['ros1_ip']}:5555")
        self.sub_socket = self.zmq_context.socket(zmq.SUB)
        self.sub_socket.connect(f"tcp://{self.config['ros1_ip']}:5556")
        self.sub_socket.setsockopt(zmq.SUBSCRIBE, b"ros1")

        self._log_network_info()
        
        # Configuration
        self.outgoing_filters = ['/shared/*']
        self.incoming_prefix = '/lab'
        
        # Storage
        self.msg_types = {}  # Dynamic message type registry
        self.ros2_subs = {}  # Outgoing topic subscribers
        self.ros2_pubs = {}  # Incoming topic publishers
        self.last_seen = {}  # Activity tracking
        
        # Timers
        self.create_timer(5.0, self.discover_topics)
        self.create_timer(30.0, self.cleanup_inactive)
        
        # Start ZMQ listener
        threading.Thread(target=self.zmq_listener, daemon=True).start()
        
        self.get_logger().info(f'Bridge started. Filters: {self.outgoing_filters}')

    def _load_config(self):
        """Load configuration from YAML file"""
        # Get the directory where this script is located
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_file = os.path.join(script_dir, 'ros2_bridge_config.yml')
        
        try:
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    config = yaml.safe_load(f)
                    self.get_logger().info(f'Loaded config from {config_file}')
                    return config
            else:
                self.get_logger().error(f'Config file not found: {config_file}')
                raise FileNotFoundError(f'Config file not found: {config_file}')
        except Exception as e:
            self.get_logger().error(f'Error loading config: {e}')
            raise

    def _log_network_info(self):
        """Log network and ZMQ connection information"""
        import socket
        
        # Get local IP addresses
        hostname = socket.gethostname()
        try:
            local_ip = socket.gethostbyname(hostname)
            self.get_logger().info(f"Container hostname: {hostname}")
            self.get_logger().info(f"Container IP: {local_ip}")
        except:
            self.get_logger().warn("Could not determine container IP")
        
        # Log ZMQ configuration
        self.get_logger().info("=== ZMQ Configuration ===")
        self.get_logger().info(f"Publishing (ROS2 -> ROS1): tcp://{self.config['ros1_ip']}:5555")
        self.get_logger().info(f"Subscribing (ROS1 -> ROS2): tcp://{self.config['ros1_ip']}:5556")
        self.get_logger().info("=========================")
    
    def discover_topics(self):
        """Find and subscribe to new outgoing topics"""
        for topic, types in self.get_topic_names_and_types():
            if (not self._is_internal_topic(topic) and 
                self._matches_filters(topic) and 
                topic not in self.ros2_subs):
                self._create_outgoing_sub(topic, types[0])
    
    def _is_internal_topic(self, topic):
        return topic.startswith(('/parameter_events', '/rosout'))
    
    def _matches_filters(self, topic):
        return any(fnmatch.fnmatch(topic, pattern) for pattern in self.outgoing_filters)
    
    def _create_outgoing_sub(self, topic, type_name):
        """Create subscriber for outgoing topic"""
        msg_class = self._get_msg_class(type_name)
        if not msg_class:
            return
        
        qos = QoSProfile(reliability=ReliabilityPolicy.RELIABLE, history=HistoryPolicy.KEEP_LAST, depth=10)
        callback = lambda msg: self._send_to_ros1(topic, msg, type_name)
        
        self.ros2_subs[topic] = {
            'sub': self.create_subscription(msg_class, topic, callback, qos),
            'type': type_name,
            'last_seen': time.time()
        }
        
        self.get_logger().info(f'Subscribed to outgoing: {topic} ({type_name})')
    
    def _get_msg_class(self, type_name):
        """Get or import message class"""
        if type_name in self.msg_types:
            return self.msg_types[type_name]
        
        # Convert ROS1 format to ROS2 format
        if '/' in type_name and '/msg/' not in type_name:
            parts = type_name.split('/')
            if len(parts) == 2:
                ros2_type = f"{parts[0]}/msg/{parts[1]}"
                self.get_logger().info(f"Converting ROS1 type '{type_name}' to ROS2 type '{ros2_type}'")
                return self._get_msg_class(ros2_type)
        
        try:
            parts = type_name.split('/')
            if len(parts) >= 3:
                module_name = f"{parts[0]}.{'.'.join(parts[1:-1])}"
                class_name = parts[-1]
                self.get_logger().info(f"Importing {module_name}.{class_name}")
                module = importlib.import_module(module_name)
                msg_class = getattr(module, class_name)
                self.msg_types[type_name] = msg_class
                self.get_logger().info(f"Successfully imported {type_name}")
                return msg_class
        except Exception as e:
            self.get_logger().warn(f'Could not import {type_name}: {e}')
        return None
    
    def _send_to_ros1(self, topic, msg, type_name):
        """Send ROS2 message to ROS1"""
        try:
            self.ros2_subs[topic]['last_seen'] = time.time()
            
            # Remove '/shared' prefix from topic name
            clean_topic = topic
            if topic.startswith('/shared/'):
                clean_topic = topic[7:]  # Remove '/shared' (7 characters)
            elif topic == '/shared':
                clean_topic = '/'
            
            # Convert ROS2 type to ROS1 type
            ros1_type = self._convert_ros2_to_ros1_type(type_name)
            
            zmq_msg = {
                'topic': clean_topic,
                'msg_type': ros1_type.split('/')[-1],  # Use converted type
                'full_type': ros1_type,  # Use converted type
                'data': self._msg_to_dict(msg),
                'timestamp': time.time(),
                'source': 'ros2'
            }
            
            self.pub_socket.send_multipart([
                f"ros2{clean_topic}".encode(),
                json.dumps(zmq_msg).encode()
            ])
            
            self.get_logger().info(f'Sent topic {topic} -> {clean_topic} (type: {type_name} -> {ros1_type}) to ROS1')
            
        except Exception as e:
            self.get_logger().error(f'Error sending {topic}: {e}')

    def _convert_ros2_to_ros1_type(self, ros2_type):
        """Convert ROS2 message type to ROS1 equivalent"""
        mappings = {
            'std_msgs/msg/String': 'std_msgs/String',
            'std_msgs/msg/Int32': 'std_msgs/Int32',
            'std_msgs/msg/Float64': 'std_msgs/Float64',
            'std_msgs/msg/Float32': 'std_msgs/Float64',
            'std_msgs/msg/Bool': 'std_msgs/Bool',
            'geometry_msgs/msg/Twist': 'geometry_msgs/Twist',
            'geometry_msgs/msg/Point': 'geometry_msgs/Point',
            'geometry_msgs/msg/Pose': 'geometry_msgs/Pose',
            'geometry_msgs/msg/PoseStamped': 'geometry_msgs/PoseStamped',
            'sensor_msgs/msg/LaserScan': 'sensor_msgs/LaserScan',
            'nav_msgs/msg/Odometry': 'nav_msgs/Odometry',
            'nav_msgs/msg/Path': 'nav_msgs/Path'
        }
        
        # Return mapped type or try to auto-convert
        if ros2_type in mappings:
            return mappings[ros2_type]
        
        # Auto-convert ROS2 format to ROS1 format
        if '/msg/' in ros2_type:
            parts = ros2_type.split('/msg/')
            if len(parts) == 2:
                converted = f"{parts[0]}/{parts[1]}"
                self.get_logger().info(f"Auto-converted {ros2_type} -> {converted}")
                return converted
        
        # Return original if no conversion found
        return ros2_type

    def zmq_listener(self):
        """Listen for ROS1 messages and auto-create publishers"""
        while rclpy.ok():
            try:
                if self.sub_socket.poll(1000):
                    self.get_logger().info("ZMQ message received from ROS1!")  # DEBUG
                    _, msg_data = self.sub_socket.recv_multipart(zmq.NOBLOCK)
                    zmq_msg = json.loads(msg_data.decode())
                    self.get_logger().info(f"Message content: {zmq_msg}")  # DEBUG
                    self._handle_incoming(zmq_msg)
            except (zmq.Again, Exception) as e:
                if not isinstance(e, zmq.Again):
                    self.get_logger().error(f'ZMQ error: {e}')
    
    def _handle_incoming(self, zmq_msg):
        """Handle incoming message from ROS1"""
        try:
            topic = zmq_msg['topic']
            msg_type = zmq_msg['msg_type']
            full_type = zmq_msg.get('full_type', msg_type)
            data = zmq_msg['data']
            
            self.get_logger().info(f"Processing topic: {topic}, type: {full_type}")  # DEBUG
            
            self.last_seen[f"in_{topic}"] = time.time()
            
            # Get or create publisher
            pub_topic = f"{self.incoming_prefix}{topic}"
            self.get_logger().info(f"Target publish topic: {pub_topic}")  # DEBUG
            
            if pub_topic not in self.ros2_pubs:
                self.get_logger().info(f"Creating new publisher for {pub_topic}")  # DEBUG
                self._create_incoming_pub(pub_topic, topic, msg_type, full_type)
            
            # Publish message
            if pub_topic in self.ros2_pubs:
                self.get_logger().info(f"Publishing to {pub_topic}")  # DEBUG
                pub_info = self.ros2_pubs[pub_topic]
                ros_msg = self._dict_to_msg(data, pub_info['msg_class'])
                if ros_msg:
                    self.get_logger().info(f"Message converted successfully: {ros_msg}")  # DEBUG
                    pub_info['pub'].publish(ros_msg)
                    self.get_logger().info(f"Message published to {pub_topic}")  # DEBUG
                else:
                    self.get_logger().error(f"Failed to convert message for {pub_topic}")  # DEBUG
            else:
                self.get_logger().error(f"Publisher not found for {pub_topic}")  # DEBUG
                    
        except Exception as e:
            self.get_logger().error(f'Error handling incoming: {e}')
    
    def _create_incoming_pub(self, pub_topic, orig_topic, msg_type, full_type):
        """Create publisher for incoming topic"""
        self.get_logger().info(f"Attempting to create publisher for {pub_topic}")  # DEBUG
        self.get_logger().info(f"Message type: {msg_type}, Full type: {full_type}")  # DEBUG
        
        msg_class = self._get_msg_class(full_type) or self._get_msg_class(msg_type)
        
        if msg_class:
            self.get_logger().info(f"Message class found: {msg_class}")  # DEBUG
            self.ros2_pubs[pub_topic] = {
                'pub': self.create_publisher(msg_class, pub_topic, 10),
                'msg_class': msg_class,
                'orig_topic': orig_topic
            }
            self.get_logger().info(f'Created incoming publisher: {orig_topic} -> {pub_topic}')
        else:
            self.get_logger().error(f'Could not find message class for {full_type} or {msg_type}')  # DEBUG
    
    def cleanup_inactive(self):
        """Remove inactive topics"""
        now = time.time()
        timeout = 60.0
        
        # Clean outgoing
        inactive = [t for t, info in self.ros2_subs.items() if now - info['last_seen'] > timeout]
        for topic in inactive:
            self.get_logger().info(f'Removing inactive outgoing: {topic}')
            del self.ros2_subs[topic]
        
        # Clean incoming
        inactive = [k for k, t in self.last_seen.items() if k.startswith('in_') and now - t > timeout * 3]
        for key in inactive:
            orig_topic = key[3:]  # Remove 'in_' prefix
            pub_topic = f"{self.incoming_prefix}{orig_topic}"
            if pub_topic in self.ros2_pubs:
                self.get_logger().info(f'Removing inactive incoming: {pub_topic}')
                del self.ros2_pubs[pub_topic]
            del self.last_seen[key]
    
    def _msg_to_dict(self, msg):
        """Convert ROS message to dict"""
        if hasattr(msg, '__slots__'):
            result = {}
            for slot in msg.__slots__:
                # Strip leading underscore for ROS1 compatibility
                clean_slot = slot.lstrip('_')
                value = getattr(msg, slot)
                
                if hasattr(value, '__slots__'):
                    result[clean_slot] = self._msg_to_dict(value)
                elif isinstance(value, (list, tuple)):
                    result[clean_slot] = [self._msg_to_dict(item) if hasattr(item, '__slots__') else item for item in value]
                else:
                    result[clean_slot] = value
            return result
        return msg
    
    def _dict_to_msg(self, data, msg_class):
        """Convert dict to ROS message"""
        try:
            msg = msg_class()
            for key, value in data.items():
                if hasattr(msg, key):
                    attr = getattr(msg, key)
                    if hasattr(attr, '__slots__'):
                        setattr(msg, key, self._dict_to_nested(value, type(attr)))
                    elif isinstance(value, list) and len(value) > 0 and hasattr(getattr(msg, key), '__slots__'):
                        setattr(msg, key, [self._dict_to_nested(item, type(attr)) for item in value])
                    else:
                        setattr(msg, key, value)
            return msg
        except Exception as e:
            self.get_logger().error(f'Dict to msg error: {e}')
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
        for socket in [getattr(self, 'pub_socket', None), getattr(self, 'sub_socket', None)]:
            if socket:
                socket.close()
        if hasattr(self, 'zmq_context'):
            self.zmq_context.term()

def main():
    rclpy.init()
    bridge = ROS2ZMQBridge()
    try:
        rclpy.spin(bridge)
    except KeyboardInterrupt:
        pass
    finally:
        bridge.destroy_node()
        rclpy.shutdown()

if __name__ == '__main__':
    main()

# Installation:
# pip install pyzmq pyyaml
# 
# Usage:
# 1. Create ros2_bridge_config.yml with ros1_ip setting
# 2. Run: python ros2_zmq_bridge.py