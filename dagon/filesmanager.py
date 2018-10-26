
import ntpath
import paramiko
from scp import SCPClient
from paramiko import SSHClient
from fabric.api import local, env, run

class FilesManager:

    @staticmethod
    def path_leaf(path):
        head, tail = ntpath.split(path)
        return tail or ntpath.basename(head) 

    @staticmethod
    def putDataInRemote(ip, ori, dest, ssh_username=None, ssh_password=None, keypath=None):
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(ip, username=ssh_username,password=ssh_password, key_filename=keypath)
        scp = SCPClient(ssh.get_transport())
        scp.put(ori,dest, recursive=True)
        scp.close()

    @staticmethod
    def getDataFromRemote(ip, ori, dest, ssh_username=None, ssh_password=None, keypath=None):
        print ip,ssh_username,keypath
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(ip, username=ssh_username,password=ssh_password, key_filename=keypath)
        scp = SCPClient(ssh.get_transport())
        scp.get(ori, recursive=True, local_path=dest)
        scp.close()

    @staticmethod
    def mkdirRemote(absolute_path, ip, ssh_username, ssh_password):
        env.host_string = ip
        env.user = ssh_username
        env.password = ssh_password
        run('mkdir -p {0}'.format(absolute_path))