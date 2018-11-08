import ntpath
import paramiko
import globus_sdk
from enum import Enum
from scp import SCPClient
from paramiko import SSHClient
from fabric.api import local, env, run
from connection import Connection

class DataTransfer(Enum):
    GLOBUS = 1
    SCP = 2

    #Infer the data transportation method (default SCP)
    @staticmethod
    def inferDataTransportation(ip, endpoint):
        if endpoint is not None and Connection.isPortOpen(ip,2811) and Connection.isPortOpen(ip,7512):
            return DataTransfer.GLOBUS
        else:
            return DataTransfer.SCP

class SCPManager:

    def __init__(self, _from=None, _to=None):
        self._from = _from
        self._to = _to

    @staticmethod
    def path_leaf(path):
        head, tail = ntpath.split(path)
        return tail or ntpath.basename(head) 

    def putDataInRemote(self, sshClient, ori, dest):
        scp = SCPClient(sshClient.getConnection().get_transport())
        scp.put(ori,dest, recursive=True)
        scp.close()
        
    def getDataFromRemote(self,sshClient, ori, dest):
        scp = SCPClient(sshClient.getConnection().get_transport())
        scp.get(ori, recursive=True, local_path=dest)
        scp.close()

    @staticmethod
    def mkdirRemote(sshClient, absolute_path, ip, ssh_username, ssh_password):
        env.host_string = ip
        env.user = ssh_username
        env.password = ssh_password
        run('mkdir -p {0}'.format(absolute_path))

    def copyData(self, ori, dest, intermedary):
        if self._to is None:
            self.getDataFromRemote(self._from, ori, intermedary)
        else:
            self.getDataFromRemote(self._from, ori, intermedary)
            self.putDataInRemote(self._to, intermedary, dest)


class GlobusManager:

    TRANSFER_TOKEN = "AgK5pqpamdmwvwjN7GpYqonNMa0pk6Pn9yeYXeXGQvYqNGXvb8CeCWVd63NMlE4mDPjMnyDP5764wDc1mm2Yxu3bE1"

    def __init__(self, _from, _to):
        self._from = _from
        self._to = _to

    @staticmethod
    def path_leaf(path):
        SCPManager.path_leaf(path)

    def copyDirectory(self, ori, dest, tc):
        tdata = globus_sdk.TransferData(tc, self._from,
                                 self._to,
                                 label="SDK example",
                                 sync_level="checksum")
        
        tdata.add_item(ori, dest,recursive=True)
        transfer_result = tc.submit_transfer(tdata)
        while not tc.task_wait(transfer_result["task_id"], timeout=1):
            task = tc.get_task(transfer_result["task_id"])
            if task['nice_status'] != "OK":
                tc.cancel_task(task["task_id"])
                return task['nice_status']
        return "OK"
    
    def copyFile(self, ori, dest, tc):
        tdata = globus_sdk.TransferData(tc, self._from,
                                 self._to,
                                 label="SDK example",
                                 sync_level="checksum")
        
        tdata.add_item(ori, dest)
        transfer_result = tc.submit_transfer(tdata)
        while not tc.task_wait(transfer_result["task_id"], timeout=1):
            task = tc.get_task(transfer_result["task_id"])
            if task['nice_status'] != "OK":
                tc.cancel_task(task["task_id"])
                return task['nice_status']
        return "OK"

    def copyData(self, ori, dest):
        #print "xxx", ori, dest
        authorizer = globus_sdk.AccessTokenAuthorizer(GlobusManager.TRANSFER_TOKEN)
        tc = globus_sdk.TransferClient(authorizer=authorizer)
        res = self.copyDirectory(ori, dest, tc)

        if res == "NOT_A_DIRECTORY":
            res = self.copyFile(ori, dest, tc)
        
        if res is not "OK":
            raise Exception(res)

    @staticmethod
    def mkdirRemote(endpoint, path):
        try:
            authorizer = globus_sdk.AccessTokenAuthorizer(GlobusManager.TRANSFER_TOKEN)
            tc = globus_sdk.TransferClient(authorizer=authorizer)
            tc.operation_mkdir(endpoint, path=path)
        except Exception, e:
            print e
