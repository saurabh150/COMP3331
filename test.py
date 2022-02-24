f = open('4103.pdf', 'r')
read = f.read(1024)
flag = 1
while read:
    print(flag)
    read = f.read(1024)
    flag += 1