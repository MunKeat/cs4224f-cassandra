import subprocess
#suppose if you're in virtual env
a=subprocess.Popen(["whereis","cqlsh"],stdout=subprocess.PIPE)
b=a.stdout.readline()
c=b.split(" ")
#c[1] is the cqlsh in virtual env, c[2] is the real cqlsh