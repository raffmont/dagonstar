import datetime
import time
from threading import Thread
import logging
from dagon import Workflow
from logging.config import fileConfig

from dagon import Status

class Task(Thread):

  def __init__(self,name):
    Thread.__init__(self)
    self.name=name
    self.nexts=[]
    self.prevs=[]
    self.reference_count=0

    self.running = False
    self.workflow = None 
    self.set_status(Status.READY)

  def get_scratch_name(self):
    #return datetime.datetime.now().strftime("%Y%m%d%H%M%S")+"-"+self.name
    millis = int(round(time.time() * 1000))
    return str(millis)+"-"+self.name

  # asJson
  def asJson(self):
    jsonTask={ "name": self.name }
    return jsonTask

  # Set the workflow
  def set_workflow(self,workflow):
    self.workflow=workflow

  # Set the current status
  def set_status(self,status):
    self.status=status
    if self.workflow is not None:
      self.workflow.logger.debug("%s: %s",self.name,self.status)    

  # Add the dependency to a task
  def add_dependency_to(self,task):
    task.nexts.append(self)
    self.prevs.append(task)
  
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
 
  # Method to be overrided 
  def execute(self):
    pass

  def run(self):
    if self.workflow is not None:
      # Change the status
      self.set_status(Status.WAITING)

      # Wait for each previous tasks
      for task in self.prevs:
        task.join()

      # Check if one of the previous tasks crashed
      for task in self.prevs:
        if (task.status==Status.FAILED):
          self.set_status(Status.FAILED)
          return

      # Change the status
      self.set_status(Status.RUNNING)

      # Execute the task Job   
      try:
        self.workflow.logger.debug("%s: Executing...",self.name)
        self.execute()
      except Exception, e:
        self.workflow.logger.error("%s: Except: %s",self.name,str(e))
        self.set_status(Status.FAILED)
        return

      # Start all next task
      for task in self.nexts:
        if (task.status==Status.READY):
          self.workflow.logger.debug("%s: Starting task: %s",self.name,task.name)
          try:
            task.start()
          except:
            self.workflow.logger.warn("%s: Task %s already started.",self.name, task.name)

      # Change the status
      self.set_status(Status.FINISHED)
      return
