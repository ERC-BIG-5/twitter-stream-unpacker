import paramiko
# from paramiko.config import SSHConfig
#
# config = SSHConfig()
# config.parse(open("/home/rsoleyma/.ssh/config"))
#
#
# conf = config.lookup('transfer1')
# print(conf)

# ssh = paramiko.SSHClient()
#
# ssh.load_system_host_keys("/home/rsoleyma/.ssh/known_hosts")
# pkey = paramiko.RSAKey.from_private_key_file(conf["identityfile"][0])
# ssh.connect(conf["hostname"], username=conf["user"], pkey=pkey, look_for_keys=False, allow_agent=False)
# # Execute rsync command
# stdin, stdout, stderr = ssh.exec_command('ls')
#
# ssh.close()
