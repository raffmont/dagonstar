from yawe import Workflow
from yawe import batch

if __name__ == '__main__':

  workflow=Workflow("demo")

  taskA=batch.Batch("Tokio","/bin/hostname >tokio.out")
  taskB=batch.Batch("Berlin","/bin/date")
  taskC=batch.Batch("Nairobi","/usr/bin/uptime")
  taskD=batch.Batch("Mosco","cat workflow://Tokio/tokio.out")

  workflow.add_task(taskA)
  workflow.add_task(taskB)
  workflow.add_task(taskC)
  workflow.add_task(taskD)

  taskB.add_dependency_to(taskA)
  taskC.add_dependency_to(taskA)
  taskD.add_dependency_to(taskB)
  taskD.add_dependency_to(taskC)
  
  workflow.run()
  


