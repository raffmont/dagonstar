import json
from dagon import Workflow
from dagon import batch

if __name__ == '__main__':

  config={
    "scratch_dir_base":"/tmp/",
    "remove_dir":False
  }

  # Create the orchestration workflow
  workflow=Workflow("Taskflow-Demo",config)

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

  jsonWorkflow=workflow.asJson()
  with open('taskflow-demo.json', 'w') as outfile:
    stringWorkflow=json.dumps(jsonWorkflow,sort_keys=True, indent=2)
    outfile.write(stringWorkflow)
  
  workflow.run()
  


