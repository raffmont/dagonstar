import os
import re
import shutil
from task import Task
from . import Workflow
from filesmanager import SCPManager
from filesmanager import DataTransfer
from filesmanager import GlobusManager
from cloud_manager import CloudManager

class CloudTask(Task):
    def __init__(self, name, command, provider, params, ssh_username, keypath=None,keyparams=None, create_instance=True, flavour=None, working_dir=None, local_working_dir=None,instance_name=None, id=None, endpoint=None):
        Task.__init__(self,name)
        self.command=command
        self.working_dir=working_dir
        self.local_working_dir = local_working_dir
        self.endpoint = endpoint
        self.ssh_username = ssh_username
        self.keypath = keypath if keypath is not None else keyparams['keypath']
        self.node = CloudManager.getInstance(id=id,keyparams=keyparams,flavour=flavour,
                   provider=provider,params=params,create_instance=create_instance,name=instance_name)
        self.ssh_connection = CloudManager.getSSHConnection(self.node.public_ips[0], self.ssh_username, self.keypath)

        if endpoint is not None and CloudManager.isPortOpen(self.node.public_ips[0],2811) and CloudManager.isPortOpen(self.node.public_ips[0],7512):
            self.transfer = DataTransfer.GLOBUS
        else:
            self.transfer = DataTransfer.SCP
    
    def isTaskRemote(self):
        return True
    
    def getSSHClient(self):
        return self.ssh_connection
    
    def getEndpoint(self):
        return self.endpoint

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
            CloudManager.executeCommand(self.ssh_connection,
                        'mv {0} {1}'.format(self.working_dir, self.working_dir+"-removed"))
            self.workflow.logger.debug("Removed %s",self.working_dir)
    
    def createWorkingDir(self):
        if self.working_dir is None:
            # Set a scratch directory as working directory
            self.working_dir = self.workflow.get_scratch_dir_base()+"/"+self.get_scratch_name()
            self.local_working_dir = self.workflow.get_scratch_dir_base()+"/"+self.get_scratch_name()
            # Create scratch directory
            if(self.transfer == DataTransfer.SCP):
                CloudManager.executeCommand(self.ssh_connection, "mkdir -p " + self.working_dir)
            else:
                GlobusManager.mkdirRemote(self.endpoint, self.working_dir)
            os.makedirs(self.local_working_dir)
            # Set to remove the scratch directory
            self.remove_scratch_dir=True
        else:
            # Set to NOT remove the scratch directory
            # Create local scratch directory
            os.makedirs(self.local_working_dir)
            self.remove_scratch_dir=False

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
        #print "Creating container"
        
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

                if(self.transfer == DataTransfer.SCP):
                    scpM = SCPManager(task.getSSHClient(), self.ssh_connection)
                    scpM.copyData(task.working_dir+"/"+inputF, self.working_dir+"/"+inputF, self.local_working_dir+"/"+inputF)
                else:
                    gm = GlobusManager(task.getEndpoint(), self.getEndpoint())
                    gm.copyData(task.working_dir+"/"+inputF, self.working_dir+"/"+inputF)
                command=command.replace(Workflow.SCHEMA+task.name,self.working_dir)
                    
        # Apply some command post processing
        command=self.post_process_command(command)
        
        # Execute the bash command
        try:
            self.result=CloudManager.executeCommand(self.ssh_connection, "sh -c \'"+command+"\'")
            #SCPManager.getDataFromRemote(self.ssh_connection,self.working_dir, self.workflow.get_scratch_dir_base())
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