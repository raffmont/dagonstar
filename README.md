# dagonstar
DAGon\* is a simple Python based workflow engine able to run job on everything from the local machine to distributed virtual HPC clusters hosted in private and public clouds.

*** Installation ***
	git clone https://github.com/raffmont/dagonstar.git
	cd dagonstar
	virtualenv venv
	. venv/bin/activate
	pip install -r requirements.txt
        export PYTHONPATH=$PWD:$PYTHONPATH

*** Demo ***

Copy the configuration file in the examples directory.

	cp dagon.ini.sample examples/dagon.ini
	cd examples

Edit the ini file matching your system configuration.

Task oriented workflow.

The workflow is defined as tasks and their explicit dependencies.

	python taskflow-demo.py

Data oriented workflow.

The workflow is defined by data dependencies (task dependencies are automatically resolved)

	pyrhon dataflow-demo.py

