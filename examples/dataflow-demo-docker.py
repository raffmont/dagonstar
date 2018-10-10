from dagon import Workflow
from dagon import batch
from dagon import docker_task as dt
import json
import sys
import datetime
import os.path

# Check if this is the main
if __name__ == '__main__':

  config={
    "scratch_dir_base":"/tmp/test/",
    "remove_dir":False
  }

  # Create the orchestration workflow
  workflow=Workflow("DataFlow-Demo",config)
  
  # The task a
  taskA=dt.Docker("Tokio","echo Soy Tokio > f1.txt", 
                  "ubuntu", "saturn.uniparthenope.it", "2375",
                  ssh_username="dante")
  
  # The task b
  taskB=dt.Docker("Berlin","echo Soy Berlin > f2.txt; cat workflow://Tokio/f1.txt >> f2.txt", 
                    "ubuntu")
  
  # The task c
  taskC=dt.Docker("Nairobi","echo Soy Nairobi > f2.txt; cat workflow://Tokio/f1.txt >> f2.txt", 
                  "ubuntu", "saturn.uniparthenope.it", "2375",
                  ssh_username="dante")
  
  # The task d
  taskD=dt.Docker("Mosco","cat workflow://Berlin/f2.txt workflow://Nairobi/f2.txt > f3.txt", "ubuntu")
  
  # add tasks to the workflow
  workflow.add_task(taskA)
  workflow.add_task(taskB)
  workflow.add_task(taskC)
  workflow.add_task(taskD)

  workflow.make_dependencies()

  jsonWorkflow=workflow.asJson()
  with open('dataflow-demo-docker.json', 'w') as outfile:
    stringWorkflow=json.dumps(jsonWorkflow,sort_keys=True, indent=2)
    outfile.write(stringWorkflow)
 
  # run the workflow
  workflow.run()
