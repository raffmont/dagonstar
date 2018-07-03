import datetime
import time
from threading import Thread
import logging
from logging.config import fileConfig

from yawe import Status

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
 
  # Method to be overrided
  def pre_run(self):
    pass
 
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
