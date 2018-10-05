import json
from dagon import Workflow
from dagon import batch
from dagon import docker_task as dt
if __name__ == '__main__':

  config={
    "scratch_dir_base":"/tmp/test",
    "remove_dir":False
  }

  # Create the orchestration workflow
  workflow=Workflow("Taskflow-Demo",config)

  taskA=dt.Docker("Tokio","/bin/hostname >tokio.out", "ubuntu:latest")
  taskB=dt.Docker("Berlin","/bin/date", "ubuntu:latest")
  taskC=dt.Docker("Nairobi","/usr/bin/uptime", "ubuntu:latest")
  taskD=dt.Docker("Mosco","cat workflow://Tokio/tokio.out", "ubuntu:latest")

  workflow.add_task(taskA)
  workflow.add_task(taskB)
  workflow.add_task(taskC)
  workflow.add_task(taskD)

  taskB.add_dependency_to(taskA)
  taskC.add_dependency_to(taskA)
  taskD.add_dependency_to(taskB)
  taskD.add_dependency_to(taskC)

  jsonWorkflow=workflow.asJson()
  with open('taskflow-demo-docker.json', 'w') as outfile:
    stringWorkflow=json.dumps(jsonWorkflow,sort_keys=True, indent=2)
    outfile.write(stringWorkflow)
  
  workflow.run()
  


