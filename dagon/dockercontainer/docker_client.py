from fabric.api import local, env, run, settings, hide

class DockerClient(object):
    def exec_command(self, command):
        with settings(
            hide('warnings', 'running', 'stdout', 'stderr'),
            warn_only=True
        ):
            res = local(command,capture=True)
            if not res.failed:
                return res.stdout
            else:
                raise Exception(res.stdout)

class DockerRemoteClient(DockerClient):

    def __init__(self, ssh):
        self.ssh = ssh

    def exec_command(self, command):
        with settings(
            hide('warnings', 'running', 'stdout', 'stderr'),
            warn_only=True
        ):
            stdout, stderr = self.ssh.executeCommand(command)
            print stdout, stderr
            if len(stderr) == 0:
                return stdout
            else:
                raise Exception(stderr)