#coding=gbk
import sys
count=0
f=open(r'd:/argv.txt','w')
for argument in sys.argv:
    print ("Argument %d is %s"%(count,argument))
    f.write("Argument %d is: %s/n"%(count,argument))
    count+=1
f.close()
sys.stderr.write('就不让你锁!')    #前面的都可以不看，关键是后面两行
exit(1)