#!/usr/bin/python3.8
"""Python module that receives TCP requests."""

import socket
import pickle
import subprocess
import random


def main():
    """Main."""
    
    slave=['172.31.86.69','172.31.88.190','172.31.92.13']
    master='172.31.88.190'
    sqlhost=master
    sqlport=3306
    sqluser='rep_user'
    sqlpass='password'
    sqldb='test1'
    
    # connexion

    s = socket.socket()
    s.bind(('',5001))

    print ('Listening port : ' + str(5001))

    s.listen(1)  # Listen to one connection
    c, addr = s.accept()
    print ('connection from: ' + str(addr))

    while True:
        data = c.recv(2048) 
        if not data:
            break

        #data = str(data)
        obj = pickle.loads(data)
        ReqType = obj['reqType']
        requete = obj['req']

        print (requete)
        if ReqType == 1:
            sqlhost=master
        elif ReqType == 2 :
            sqlhost = random.choice(slave)
        print('la requete est envoyee au :',sqlhost)
        sqlcommand = "MYSQL_PWD=%s mysql -u %s -h %s --database %s -e\"%s\" " % (sqlpass, sqluser, sqlhost, sqldb, requete)
        res = subprocess.check_output(sqlcommand, stderr=subprocess.STDOUT, shell=True)
        pickleData = pickle.dumps(res)
        c.send(pickleData)

   
    c.close()
    s.close()

if __name__ == '__main__':
    main()

