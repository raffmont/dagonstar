
from fabric.api import local, env, run, settings, hide
from paramiko import SSHClient
from Container import Container
import time

class DockerClient(object):
    
    def __init__(self,ssh_client=None):
        self.isLocal = ssh_client == None
        self.ssh = ssh_client
    
    def run(self, image, command=None, volume=None, ports=None, detach=False):
        docker_command = "docker run"
        
        if(detach):
            docker_command += " -t -d"

        if not volume == None:
            docker_command += " -v \'%s\':\'%s\'" % (volume['host'],volume['container'])
        if not ports == None:
            docker_command += " -p \'%s\':\'%s\'" % (ports['host'],ports['container'])
        docker_command += " %s" % image
        
        if not command == None:
            docker_command += " " +command
        
        output = self.exec_command(docker_command)
        if detach:
            self.cont_key = output.strip()
            return Container(self.cont_key, self)
        else:
            return output
    
    def exec_command(self, command):
        with settings(
            hide('warnings', 'running', 'stdout', 'stderr'),
            warn_only=True
        ):
            res = None
            if self.isLocal:
                res = local(command,capture=True)
                if not res.failed:
                    return res.stdout
                else:
                    raise Exception(res.stdout)
            else:
                stdin, stdout, stderr = self.ssh.exec_command(command, get_pty=True)
                res = ""
                for line in iter(stdout.readline, ""):
                    res += line
                stderr = stderr.read()
                print stderr, res
                if not stderr.strip():
                    return res
                else:
                    raise Exception(stderr)