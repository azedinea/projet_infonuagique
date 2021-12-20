#!/usr/bin/python3.8

""" Ce progamme proxy recoit les requetes clients et le redirige vers le bon serveur 
    Arguments possible: direct (ecriture/lecture directe dans le master 
                        random - on choisit 1 parmi les slaves, si ecriture ca va aller dans le master
                        balancer - si lecture, on ping master et slaves, le plus rapide est choisi, si ecriture c'est fait dans le master 
"""

import socket
import pickle
import random
import subprocess
import sys
import time
import getopt
import struct


# Pour choisir choisir le server qui repond vite au ping
def fastestping(l1) :
    hostlist=l1
    pt=99999.9
    mysqlhost=master
    for x in hostlist :
         cmdping="ping -c 3 %s |tail -1 |cut -d/ -f5" % (x)
         result=subprocess.check_output(cmdping, stderr=subprocess.STDOUT, shell=True)
         res=str(result.decode('utf-8').strip())
         print("server-ping",x,res)
         if float(res) < pt:
                mysqlhost=x
                pt=float(res)
    return mysqlhost


def main(argv):
    """Main.""" 
hitmod =sys.argv[1]
master='172.31.88.190'
slave=['172.31.86.69','172.31.92.13']
server=['172.31.86.69','172.31.88.190','172.31.92.13']

#lisner
lisnerport=5001
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(('', lisnerport))
print ('Listening port : ' + str(5001))
s.listen(1)  # one connection
c, addr = s.accept()
print ('connection from: ' + str(addr))

#sendTO
sqlhost=master
sqlport=3306
sqluser='rep_user'
sqlpass='log8415e'
sqldb='test1'

while True:
    req = c.recv(2048)
    if not req:
        break
    data=pickle.loads(req)
    ReqType=data['reqType']
    requete=data['req']
    if hitmod=='direct' or ReqType==1 :
        sqlhost=master
        print('direct_hit or insert, master will be used')
    elif ReqType==2:
        print('ReqType==2')
        if hitmod=='random' :
            #print('random selectonne')
            sqlhost= random.choice(slave)
            #print('random hit, a slave will be choose',sqlhost)
        elif hitmod=='balancer' :
            sqlhost = fastestping(server)
            #print('balanced hit, best ping to choose server')
    print ('Requete:'+requete+' Type: '+str(ReqType)+' server choisi :',sqlhost)
            
    sqlcommand = "MYSQL_PWD=%s mysql -u %s -h %s --database %s -e\"%s\" " % (sqlpass, sqluser, sqlhost, sqldb, requete)
    res = subprocess.check_output(sqlcommand, stderr=subprocess.STDOUT, shell=True)
    
    pickleData = pickle.dumps(res)
    c.send(pickleData)
 
s.close()
c.close()

if __name__ == '__main__':
    main(sys.argv[1:])  

