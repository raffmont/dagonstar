
import ntpath
from paramiko import SSHClient
from scp import SCPClient
from fabric.api import local, env, run

class FilesManager:

    @staticmethod
    def path_leaf(path):
        head, tail = ntpath.split(path)
        return tail or ntpath.basename(head) 

    @staticmethod
    def putDataInRemote(ip, ssh_username, ssh_password, ori, dest):
        ssh = SSHClient()
        ssh.load_system_host_keys()
        ssh.connect(ip, username=ssh_username,password=ssh_password)
        scp = SCPClient(ssh.get_transport())
        scp.put(ori,dest, recursive=True)
        scp.close()

    @staticmethod
    def getDataFromRemote(ip, ssh_username, ssh_password, ori, dest):
        ssh = SSHClient()
        ssh.load_system_host_keys()
        ssh.connect(ip, username=ssh_username,password=ssh_password)
        scp = SCPClient(ssh.get_transport())
        scp.get(ori, recursive=True, local_path=dest)
        scp.close()

    @staticmethod
    def mkdirRemote(absolute_path, ip, ssh_username, ssh_password):
        env.host_string = ip
        env.user = ssh_username
        env.password = ssh_password
        run('mkdir -p {0}'.format(absolute_path))