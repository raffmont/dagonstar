
import os
import time
import socket
import paramiko
import importlib
from enum import Enum
from paramiko import SSHClient
from fabric.api import local, env
from libcloud.compute.types import Provider
from libcloud.compute.types import NodeState
from libcloud.compute.providers import get_driver

import cloud_manager

class CloudManager(object):

    @staticmethod
    def getInstance(keyparams, provider, params, name=None, create_instance=True, flavour=None, id=None):
        driver = get_driver(provider)
        conn = driver(**params)
        manager = globals()[provider.upper()]
        node = manager.createInstance(conn, name, flavour, keyparams) if create_instance else CloudManager.getExistingInstance(conn, id=id, name=name)
        node =CloudManager.waitUntilRunning(conn, node)
        #node = conn.wait_until_running([node])
        return node

    @staticmethod
    def waitUntilRunning(conn, node):
        while node.state is not NodeState.RUNNING:
            try:
                node = CloudManager.getExistingInstance(conn, uuid=node.uuid)
            except Exception, e:
                pass
            time.sleep(1)
        return node

    @staticmethod
    def addToKnowHosts(node):
        command = "ssh-keyscan -H %s >> ~/.ssh/known_hosts" % (node)
        result = local(command, capture=False, shell="/bin/bash")
        # Check if the execution failed    
        if result.failed:
            raise Exception('Failed to add to know hosts')

    @staticmethod
    def createInstance(conn, name, flavour, keyparams):
        if(flavour is None):
            raise Exception('The characteristics of the image has not been specified')
        sizes = conn.list_sizes()
        size = [s for s in sizes if s.id == flavour['size']]
        image = conn.get_image(flavour['image'])
        size = size[0] if len(size) > 0  else None
        if image is None or size is None:
            raise Exception('Size or image doesn\'t exists')
        if keyparams['option'] == KeyOptions.CREATE:
            key = KeyPair.createPairKey(conn,keyparams['keypath'],keyparams['cloudargs'])
        elif keyparams['option'] == KeyOptions.GET:
            key = KeyPair.getExistingPairKey(conn,keyparams['keyname'])
        elif keyparams['option'] == KeyOptions.IMPORT:
            key = KeyPair.importKey(conn, keyparams['keyname'],keyparams['keypath'])
        node = conn.create_node(name=name, image=image, size=size,
                          ex_keyname=key.name)
        return node

    @staticmethod
    def isPortOpen(host, port, timeout=5):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host,port))
        return result is 0

    @staticmethod
    def getSSHConnection(host, username, keypath):
        #CloudManager.addToKnowHosts(host) #add to know hosts
        ssh = SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        while not CloudManager.isPortOpen(host, 22):
            continue
        ssh.connect(host, username=username, key_filename=keypath)
        return ssh


    @staticmethod
    def executeCommand(ssh_instance, command):
        stdin, stdout, stderr = ssh_instance.exec_command(command)
        res =  stdout.readlines()
        print "command",command
        print res
        return res
    
    @staticmethod
    def getExistingInstance(conn, id=None,name=None, uuid=None):
        if id is None and name is None and uuid is None:
            raise Exception('Must specified an intance\'s id or name')
        nodes = conn.list_nodes()
        node = None
        if id is not None:
            node = [node for node in nodes if node.id == id]
        elif name is not None:
            node = [node for node in nodes if node.name == name]
        elif uuid is not None:
            node = [node for node in nodes if node.uuid == uuid]
        if len(node) == 0:
            raise Exception('Instance doesn\'t exists')
        return node[0]

class KeyOptions(Enum):
    CREATE = "CREATE"
    IMPORT = "IMPORT"
    GET = "GET"

class KeyPair(object):

    @staticmethod
    def generate_RSA(bits=2048):
        '''
        Generate an RSA keypair with an exponent of 65537 in PEM format
        param: bits The key length in bits
        Return private key and public key
        '''
        from Crypto.PublicKey import RSA 
        new_key = RSA.generate(bits, e=65537) 
        public_key = new_key.publickey().exportKey("OpenSSH") 
        private_key = new_key.exportKey("PEM") 
        return private_key, public_key

    @staticmethod
    def createPairKey(conn,filename,args):
        from os import chmod
        from inspect import getargspec

        ###CHECK FOR THE PARAMS OF THE FUNCTION
        sig = getargspec(conn.create_key_pair)
        fooArgs = sig.args
        fooParams = dict()

        for arg in fooArgs:
            if arg in args:
                fooParams[arg] = args[arg]

        #### CREATE NEW KEY
        key_pair = conn.create_key_pair(**fooParams)
        privateKey = key_pair.private_key
        if privateKey is None:
            privateKey = args['private_key']
        with open(filename, 'w') as content_file:
            chmod(filename, 0600)
            content_file.write(privateKey)
        return key_pair
    
    @staticmethod
    def getExistingPairKey(conn,keyname):
        keys = conn.list_key_pairs()
        key_pair = [key for key in keys if key.name == keyname]
        key_pair = key_pair[0] if len(key_pair) > 0 else None
        return key_pair
    
    @staticmethod
    def importKey(conn, keyname, key_path):
        key_file_path = os.path.expanduser(key_path)
        key_pair = conn.import_key_pair_from_file(name=key_path,
                                          key_file_path=key_file_path)
        return key_pair


class EC2(object):
    @staticmethod
    def createInstance(conn, name, flavour, keyparams):
        if(flavour is None):
            raise Exception('The characteristics of the image has not been specified')
        sizes = conn.list_sizes()
        size = [s for s in sizes if s.id == flavour['size']]
        image = conn.get_image(flavour['image'])
        size = size[0] if len(size) > 0  else None
        if image is None or size is None:
            raise Exception('Size or image doesn\'t exists')
        if keyparams['option'] == KeyOptions.CREATE:
            key = KeyPair.createPairKey(conn,keyparams['keypath'],keyparams['cloudargs'])
        elif keyparams['option'] == KeyOptions.GET:
            key = KeyPair.getExistingPairKey(conn,keyparams['keyname'])
        elif keyparams['option'] == KeyOptions.IMPORT:
            key = KeyPair.importKey(conn, keyparams['keyname'],keyparams['keypath'])
        node = conn.create_node(name=name, image=image, size=size,
                          ex_keyname=key.name)
        return node
    
class DIGITALOCEAN(object):
    @staticmethod
    def createInstance(conn, name, flavour, keyparams):
        if(flavour is None):
            raise Exception('The characteristics of the image has not been specified')
        sizes = conn.list_sizes()
        size = [s for s in sizes if s.id == flavour['size']]
        image = conn.get_image(flavour['image'])
        size = size[0] if len(size) > 0  else None
        locations = conn.list_locations()
        location = [l for l in locations if l.id == flavour['location']]
        location = location[0] if len(location) > 0 else None
        if image is None or size is None or location is None:
            raise Exception('Size, location or image doesn\'t exists')
        if keyparams['option'] == KeyOptions.CREATE:
            key = KeyPair.createPairKey(conn,keyparams['keypath'],keyparams['cloudargs'])
        elif keyparams['option'] == KeyOptions.GET:
            key = KeyPair.getExistingPairKey(conn,keyparams['keyname'])
        elif keyparams['option'] == KeyOptions.IMPORT:
            key = KeyPair.importKey(conn, keyparams['keyname'],keyparams['keypath'])
        node = conn.create_node(name=name, image=image, size=size, location=location,
                          ex_create_attr={"ssh_keys":[key.fingerprint]})
        return node
    