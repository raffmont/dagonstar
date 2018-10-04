from dagon import Workflow
from dagon import batch
import json
import sys
import datetime
import os.path

# Check if this is the main
if __name__ == '__main__':

  config={
    "scratch_dir_base":"/tmp/",
    "remove_dir":False
  }

  # Create the orchestration workflow
  workflow=Workflow("DataFlow-Demo",config)
  
  # The task a
  taskA=batch.Batch("Tokio","echo $RANDOM > f1.txt")
  
  # The task b
  taskB=batch.Batch("Berlin","echo $RANDOM > f2.txt; cat workflow://Tokio/f1.txt >> f2.txt")
  
  # The task c
  taskC=batch.Batch("Nairobi","echo $RANDOM > f2.txt; cat workflow://Tokio/f1.txt >> f2.txt")
  
  # The task d
  taskD=batch.Batch("Mosco","cat workflow://Berlin/f2.txt workflow://Nairobi/f2.txt > f3.txt")
  
  # add tasks to the workflow
  workflow.add_task(taskA)
  workflow.add_task(taskB)
  workflow.add_task(taskC)
  workflow.add_task(taskD)

  workflow.make_dependencies()

  jsonWorkflow=workflow.asJson()
  with open('dataflow-demo.json', 'w') as outfile:
    stringWorkflow=json.dumps(jsonWorkflow,sort_keys=True, indent=2)
    outfile.write(stringWorkflow)
 
  # run the workflow
  workflow.run()
