import shutil
import os
import re

from task import Task
from dagon import Workflow
from docker_task import LocalDockerTask
from dockercontainer.container import Container
from communication.ssh import SSHManager
from dockercontainer.docker_client import DockerRemoteClient

from cloud.cloud_manager import CloudManager
from communication.data_transfer import DataTransfer
from communication.data_transfer import GlobusManager
from communication.data_transfer import SCPManager

class RemoteTask(Task):

    def __init__(self, name, ssh_username, keypath, command, ip=None, working_dir=None, local_working_dir=None, endpoint=None):
        Task.__init__(self,name)
        self.ip = ip
        self.keypath = keypath
        self.command = command
        self.endpoint = endpoint
        self.working_dir = working_dir
        self.ssh_username = ssh_username
        self.local_working_dir = local_working_dir

    #only to other tasks know that this is a remote task
    def isTaskRemote(self):
        return True
        
    #start ssh connection
    def startSSHConnection(self):
        self.ssh_connection = SSHManager(self.ssh_username, self.ip, self.keypath)

    #Return the transfer type method (globus, scp)
    def getTransferType(self):
        return self.transfer

    #return SSH client
    def getSSHClient(self):
        return self.ssh_connection
    
    #return endpoint ID for globus transfer
    def getEndpoint(self):
        return self.endpoint

    #change the ip
    def setIp(self, ip):
        self.ip = ip

    # # Remove the scratch directory if needed
    def remove_scratch(self):
        # Check if the scratch directory must be removed
        if self.reference_count==0 and self.remove_scratch_dir is True:
            # Remove the scratch directory
            shutil.move(self.working_dir,self.working_dir+"-removed")
            SSHManager.executeCommand(self.ssh_connection,
                        'mv {0} {1}'.format(self.working_dir, self.working_dir+"-removed"))
            self.workflow.logger.debug("Removed %s",self.working_dir)

    def createWorkingDir(self):
        if self.working_dir is None:
            # Set a scratch directory as working directory
            self.working_dir = self.workflow.get_scratch_dir_base()+"/"+self.get_scratch_name()
            self.local_working_dir = self.working_dir
            
            # Create scratch directory
            
            if(self.transfer == DataTransfer.SCP):
                SSHManager.executeCommand(self.ssh_connection, "mkdir -p " + self.working_dir)
            else:
                GlobusManager.mkdirRemote(self.endpoint, self.working_dir)
            os.makedirs(self.local_working_dir)
            # Set to remove the scratch directory
            self.remove_scratch_dir=True
        else:
            # Set to NOT remove the scratch directory
            os.makedirs(self.local_working_dir)
            self.remove_scratch_dir=False

    # Method to be overrided 
    def execute(self):
        pass

    # Method overrided
    def pre_run(self):
        Task.pre_run(self)
        RemoteTask.createWorkingDir(self)


class DockerRemoteTask(LocalDockerTask,RemoteTask):
    def __init__(self,name,command,image=None, containerID=None, ip=None,ssh_username=None, keypath=None,working_dir=None,local_working_dir=None,endpoint=None):
        LocalDockerTask.__init__(self, name, command, containerID=containerID, working_dir=working_dir, image=image)
        RemoteTask.__init__(self,name=name, ssh_username=ssh_username, keypath=keypath, command=command, ip=ip, working_dir=working_dir, local_working_dir=local_working_dir, endpoint=endpoint)
        self.startSSHConnection()
        self.transfer = DataTransfer.inferDataTransportation(self.ip,self.endpoint)
        self.docker_client = DockerRemoteClient(self.ssh_connection)

    #pre_run the task
    def pre_run(self):
        RemoteTask.pre_run(self)
        if(self.containerID is None):
            self.containerID  = self.createContainer()
        self.container = Container(self.containerID, self.docker_client)
    
    def remove_scratch(self):
        RemoteTask.remove_scratch(self)

    # Method overrided 
    def execute(self):
        LocalDockerTask.execute(self)


class CloudTask(RemoteTask):
    def __init__(self, name, command, provider, ssh_username, keyparams=None, create_instance=True, flavour=None, working_dir=None, local_working_dir=None,instance_name=None, id=None, endpoint=None):
        RemoteTask.__init__(self,name=name, ssh_username=ssh_username, keypath=keyparams['keypath'], command=command, working_dir=working_dir, local_working_dir=local_working_dir, endpoint=endpoint)
        print provider
        self.node = CloudManager.getInstance(id=id,keyparams=keyparams,flavour=flavour,
                   provider=provider,create_instance=create_instance,name=instance_name)
        self.setIp(self.node.public_ips[0])
        self.startSSHConnection()
        self.transfer = DataTransfer.inferDataTransportation(self.ip,self.endpoint)

    # Method overrided 
    def execute(self):
        
        self.workflow.logger.debug("%s: Scratch directory: %s",self.name,self.working_dir)

        # Change to the scratch directory
        #os.chdir(self.working_dir)

        # Applay some command pre processing
        command=self.pre_process_command(self.command)
        #command = self.command
        
        # Get the arguments splitted by the schema
        args=command.split(Workflow.SCHEMA)
        for i in range(1,len(args)):
            # Split each argument in elements by the slash
            elements=args[i].split("/")
            
            # The task name is the first element
            task_name=elements[0]

            # Extract the task
            task=self.workflow.find_task_by_name(task_name)
            if task is not None:
                inputF=re.split("> |>>", elements[1])[0].strip()
                inputF=re.split(" ",inputF)[0].strip()
                
                if task.getTransferType() == DataTransfer.GLOBUS and self.getTransferType() == DataTransfer.GLOBUS:
                    gm = GlobusManager(task.getEndpoint(), self.getEndpoint())
                    gm.copyData(task.working_dir+"/"+inputF, self.working_dir+"/"+inputF)
                else:
                    scpM = SCPManager(task.getSSHClient(), self.ssh_connection)
                    scpM.copyData(task.working_dir+"/"+inputF, self.working_dir+"/"+inputF, self.local_working_dir+"/"+inputF)
                command=command.replace(Workflow.SCHEMA+task.name,self.working_dir)
                    
        # Apply some command post processing
        command=self.post_process_command(command)
        
        # Execute the bash command
       
        self.result=SSHManager.executeCommand(self.ssh_connection, command)
        if self.result["code"] == 1:
            raise Exception(self.result["error"].rstrip())

  
        # Remove the reference
        # For each workflow:// in the command

        # Get the arguments splitted by the schema
        args=self.command.split(Workflow.SCHEMA)
        for i in range(1,len(args)):
            # Split each argument in elements by the slash
            elements=args[i].split("/")

            # The task name is the first element
            task_name=elements[0]

            # Extract the task
            task=self.workflow.find_task_by_name(task_name)
            if task is not None:

                # Remove the reference from the task
                task.decrement_reference_count()

        # Remove the scratch directory
        self.remove_scratch()