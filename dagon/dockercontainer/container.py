from fabric.api import local, env, run, settings, hide

class Container(object):
    
    def __init__(self, key, client):
        self.key = key
        self.client = client

    def logs(self):
        command = "docker logs " + self.key
        res = self.client.exec_command(command)
        return res

    def exec_in_cont(self, command):
        docker_command = "docker exec -t " + self.key + " " + command
        res = self.client.exec_command(docker_command)
        return res

    def rm(self, force=False):
        band = "-f" if force else ""
        command = "docker rm %s %s" % (band,self.key)
        try:
            self.client.exec_command(command)
        except Exception, e:
            print e
            return False
        return True

    def stop(self):
        command = "docker stop %s" % (self.key)
        try:
            self.client.exec_command(command)
        except Exception, e:
            print e
            return False
        return True

    def __copy2cont(self, ori, dest):
        command = "docker cp %s %s:%s" % (ori, self.key, dest)
        try:
            self.client.exec_command(command)
        except Exception, e:
            print e
            return False
        return True
    