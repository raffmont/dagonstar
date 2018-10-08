import logging
from logging.config import fileConfig
import errno    
import os
import shutil
import tempfile
import docker
from fabric.api import local, env
from fabric.context_managers import cd

from task import Task
from . import Workflow

class Docker(Task):

    #Params:
    # 1) name: task name
    # 2) command: command to be executed
    # 3) image: docker image which the container is going to be created
    # 4) host: URL of the host, by default use the unix local host
    def __init__(self,name,command,image,host='unix://var/run/docker.sock',working_dir=None):
        Task.__init__(self,name)
        self.command=command
        self.working_dir=working_dir
        self.image=image
        self.host = host
        self.isRemote = host.startswith("tcp")

    def asJson(self):
        jsonTask=Task.asJson(self)
        jsonTask['command']=self.command
        return jsonTask

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
            shutil.move(self.working_dir,self.working_dir+"-removed")
            self.workflow.logger.debug("Removed %s",self.working_dir)

    # Creates a container on remote host
    def createContainer(self, client, command):
        container = None
        #print self.workflow.get_scratch_dir_base()
        if(self.isRemote):
            container=client.containers.run(self.image, "sh -c \'"+command+"\'",
             volumes={self.working_dir:
             {'bind' : self.working_dir, 'mode':'rw'}},detach=True)
        else:
            container=client.containers.run(self.image, "sh -c \'"+command+"\'",
             volumes={self.workflow.get_scratch_dir_base():
             {'bind' : self.workflow.get_scratch_dir_base(), 'mode':'rw'}},detach=True)
        return container


    # Method overrided 
    def execute(self):
        print self.command
        if self.working_dir is None:
            # Set a scratch directory as working directory
            self.working_dir = self.workflow.get_scratch_dir_base()+"/"+self.get_scratch_name()

            # Create scratch directory
            os.makedirs(self.working_dir)

            # Set to remove the scratch directory
            self.remove_scratch_dir=True
        else:
            # Set to NOT remove the scratch directory
            self.remove_scratch_dir=False
        
        self.workflow.logger.debug("%s: Scratch directory: %s",self.name,self.working_dir)

        # Change to the scratch directory
        #os.chdir(self.working_dir)

        # Applay some command pre processing
        command=self.pre_process_command(self.command)
        #command = self.command
        
        # Get the arguments splitted by the schema
        args=command.split(Workflow.SCHEMA)
        wd = self.working_dir
        
        for i in range(1,len(args)):
            # Split each argument in elements by the slash
            elements=args[i].split("/")

            # The task name is the first element
            task_name=elements[0]

            # Extract the task
            task=self.workflow.find_task_by_name(task_name)
            if task is not None:
                wd = task.working_dir
                # Substitute the reference by the actual working dir
                command=command.replace(Workflow.SCHEMA+task.name,task.working_dir)
       
        # Apply some command post processing
        command=self.post_process_command(command)
        #print command
        # Execute the bash command
        
        client = docker.DockerClient(base_url=self.host)

        try: #verifies if Docker is running on the server
            client.ping()
        except Exception as e:
            raise Exception('Docker is not running on server ('+self.host+')')
        

        try:
            self.result=self.createContainer(client,command)
            print "logs",self.result.logs()
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
        
        self.remove_scratch();