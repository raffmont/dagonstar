import logging
from logging.config import fileConfig
import errno    
import os
import shutil
import tempfile
from fabric.api import local, env
from fabric.context_managers import cd

import boto3
from paramiko import SSHClient
from scp import SCPClient

from task import Task
from . import Workflow

class Batch(Task):

  env.use_ssh_config = True

  def __init__(self,name,command,working_dir=None):
    Task.__init__(self,name)
    self.command=command
    self.working_dir=working_dir

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
    self.remove_scratch();

  # # Remove the scratch directory if needed
  def remove_scratch(self):
    # Check if the scratch directory must be removed
    if self.reference_count==0 and self.remove_scratch_dir is True:
      # Remove the scratch directory
      #shutil.rmtree(self.working_dir)
      shutil.move(self.working_dir,self.working_dir+"-removed")
      self.workflow.logger.debug("Removed %s",self.working_dir)

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

  # Method overrided 
  def execute(self):
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
        # Substitute the reference by the actual working dir
        command=command.replace(Workflow.SCHEMA+task.name,task.working_dir)

    # Apply some command post processing
    command=self.post_process_command(command)

    # Execute the bash command
    self.result=local(command, capture=True)

    # Check if the execution failed    
    if self.result.failed:
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

class Slurm(Batch):

  def __init__(self,name,command,partition=None,ntasks=None,working_dir=None):
    Batch.__init__(self,name,command,working_dir)
    self.partition=partition
    self.ntasks=ntasks

  # Pre process the command
  def pre_process_command(self,command):

    partition_text=""
    if self.partition is not None:
      partition_text="--partition="+self.partition

    ntasks_text=""
    if self.ntasks is not None:
      ntasks_text="--ntasks="+self.ntasks

    # Add the slurm batch command
    command="sbatch "+partition_text+" "+ntasks_text+" --job-name "+self.name+" --chdir "+self.working_dir+" --output="+self.name+"_output.txt --wait "+command
    return command

  # Post process the command
  def post_process_command(self,command):
    return command

class AWSEC2(Batch):

  def __init__(self,name,image_id,machine_type,working_dir=None):
    Batch.__init__(self,name,None)
    self.image_id=image_id
    self.machine_type=machine_type


