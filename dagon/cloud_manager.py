
import os
import paramiko
from enum import Enum
from paramiko import SSHClient
from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

class CloudManager(object):

    @staticmethod
    def getInstance(keyparams, provider, params, name=None, create_instance=True, flavour=None, id=None):
        driver = get_driver(provider)
        conn = driver(**params)
        node = CloudManager.createInstance(conn, name, flavour, keyparams) if create_instance else CloudManager.getExistingInstance(conn, id)
        return node

    @staticmethod
    def createInstance(conn, name, flavour, keyparams):
        if(flavour is None):
            raise Exception('The characteristics of the image has not been specified')
        print flavour['size'],flavour['image']
        sizes = conn.list_sizes()
        size = [s for s in sizes if s.id == flavour['size']]
        image = conn.get_image(flavour['image'])
        size = size[0] if len(size) > 0  else None
        if image is None or size is None:
            raise Exception('Size or image doesn\'t exists')
        if keyparams['option'] == KeyOptions.CREATE:
            key = KeyPair.createPairKey(conn,keyparams['keyname'],keyparams['keypath'])
        elif keyparams['option'] == KeyOptions.GET:
            key = KeyPair.getExistingPairKey(conn,keyparams['keyname'])
        elif keyparams['option'] == KeyOptions.IMPORT:
            key = KeyPair.importKey(conn, keyparams['keyname'],keyparams['keypath'])
        node = conn.create_node(name=name, image=image, size=size,
                          ex_keyname=key.name)
        return node

    @staticmethod
    def executeCommand(instance, username, keypath, command):
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(instance.public_ips[0], username=username, key_filename=keypath)
        stdin, stdout, stderr = ssh.exec_command(command)
        res =  stdout.readlines()
        print command
        print res
        print stderr.readlines()
        ssh.close()
        return res
    
    @staticmethod
    def getExistingInstance(conn, id):
        nodes = conn.list_nodes()
        node = [node for node in nodes if node.id == id][0]
        return node

class KeyOptions(Enum):
    CREATE = "CREATE"
    IMPORT = "IMPORT"
    GET = "GET"

class KeyPair(object):
    @staticmethod
    def createPairKey(conn,keyname,filename):
        #### CREATE NEW KEY
        key_pair = conn.create_key_pair(name=keyname)
        file = open(filename,"w+")
        file.write(key_pair.private_key) 
        file.close()
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
