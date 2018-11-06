import logging
from logging.config import fileConfig
import errno    
import os
import shutil
import tempfile
import re
from fabric.api import local, env, run
from fabric.context_managers import cd
from paramiko import SSHClient
from scp import SCPClient
import os.path
from os import path


from task import Task
from . import Workflow
from Container import Container
from filesmanager import SCPManager
from filesmanager import DataTransfer
from docker_tools import DockerClient
from cloud_manager import CloudManager

class Docker(Task):

    #Params:
    # 1) name: task name
    # 2) command: command to be executed
    # 3) image: docker image which the container is going to be created
    # 4) host: URL of the host, by default use the unix local host
    def __init__(self,name,command,image=None, containerID=None,
                 ip=None,port=None,ssh_username=None,
                 keypath=None,working_dir=None,endpoint=None):
        Task.__init__(self,name)
        self.command=command
        self.working_dir=working_dir
        self.containerID=containerID
        self.image=image
        self.ip = ip
        print self.ip
        self.endpoint = endpoint
        self.ssh_username = ssh_username
        self.keypath = keypath
        self.isRemote = not ip == None
        if(self.isRemote):  
            self.ssh_client = CloudManager.getSSHConnection(ip, ssh_username, keypath)
            self.docker_client = DockerClient(ssh_client=self.ssh_client)
            if endpoint is not None and CloudManager.isPortOpen(ip,2811) and CloudManager.isPortOpen(ip,7512):
                self.transfer = DataTransfer.GLOBUS
            else:
                self.transfer = DataTransfer.SCP
        else:
            self.docker_client = DockerClient()

    def asJson(self):
        jsonTask=Task.asJson(self)
        jsonTask['command']=self.command
        return jsonTask

    def getSSHClient(self):
        return self.ssh_connection
    
    def getEndpoint(self):
        return self.endpoint

    # Increment the reference count
    def increment_reference_count(self):
        self.reference_count=self.reference_count+1

    # Decremet the reference count 
    def decrement_reference_count(self):
        self.reference_count=self.reference_count-1

        # Remove the scratch directory
        self.remove_scratch()

    # Method overrided
    def pre_run(self):
        # For each workflow:// in the command string
        ### Extract the referenced task
        ### Add a reference in the referenced task

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

                # Add the dependency to the task
                self.add_dependency_to(task)

                # Add the reference from the task
                task.increment_reference_count()

        self.createWorkingDir()
        
        
        if(self.containerID is None):
            print "Creating container"
            self.container = self.docker_client.run(image=self.image, detach=True, 
                            volume={"host":self.workflow.get_scratch_dir_base(),
                            "container":self.workflow.get_scratch_dir_base()})
        else:
            print "Getting container"
            self.container = Container(self.containerID, self.docker_client)


    def createWorkingDir(self):
        if self.working_dir is None:
            # Set a scratch directory as working directory
            self.working_dir = self.workflow.get_scratch_dir_base()+"/"+self.get_scratch_name()

            # Create scratch directory
            if self.isRemote:
                self.docker_client.exec_command("mkdir -p " + self.working_dir)
            else:
                os.makedirs(self.working_dir)

            # Set to remove the scratch directory
            self.remove_scratch_dir=True
        else:
            # Set to NOT remove the scratch directory
            self.remove_scratch_dir=False


    # Pre process command
    def pre_process_command(self,command):
        return "cd "+self.working_dir+";"+command

    # Post process the command
    def post_process_command(self,command):
        return command+"|tee ./"+self.name+"_output.txt"

    # # Remove the scratch directory if needed
    def remove_scratch(self):
        # Check if the scratch directory must be removed
        if self.reference_count==0 and self.remove_scratch_dir is True:
        # Remove the scratch directory
        #shutil.rmtree(self.working_dir)
            #shutil.move(self.working_dir,self.working_dir+"-removed")
            if self.isRemote: self.remove_remote_scratch()
            self.workflow.logger.debug("Removed %s",self.working_dir)

    def remove_remote_scratch(self):
        self.docker_client.exec_command('mv {0} {1}'.format(self.working_dir, self.working_dir+"-removed"))

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
                if(self.isRemote):
                    inputF=re.split("> |>>", elements[1])[0].strip()
                    scpM = SCPManager()
                    scpM.putDataInRemote(self.ssh_client, task.working_dir+"/"+inputF, self.working_dir+"/"+inputF)
                    #SCPManager.copyData(self.ssh_client, task.working_dir+"/"+inputF, self.working_dir+"/"+inputF)
                    command=command.replace(Workflow.SCHEMA+task.name,self.working_dir)
                else:   
                    # Substitute the reference by the actual working dir
                    command=command.replace(Workflow.SCHEMA+task.name,task.working_dir)
                    
        # Apply some command post processing
        command=self.post_process_command(command)
        
        # Execute the bash command
        print command
        try:
            self.result=self.container.exec_in_cont("sh -c \'"+command+"\'")
            #if self.isRemote:
                #scpM = SCPManager()
                #scpM.getDataFromRemote(self.ssh_client,  self.working_dir, self.workflow.get_scratch_dir_base())
            #pass
        except Exception as e:
            print e
            raise Exception('Executable raised a execption')
   
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