#!/usr/bin/python3.8

""" Ce progamme verifie que les connexions et requetes recues sont conformes """

import socket
import re
import pickle
import random
import subprocess
import sys


def request_validator(data) :
    
    selpat=re.compile("^(select[\s])\*([\s])from Persons")
    intpat=re.compile("^(insert ignore into Persons values)")
    print('on test la requete si conforne',data)
    if data.startswith('insert'):
        if intpat.match(data):
             print('insertion confirmee')
             return True
    if data.startswith('select'):
        if selpat.match(data) :
             print('select confirme')
             return True
    print('validation complete')
    return False


def main():
    """Main."""
    #lisener
    s = socket.socket()
    s.bind(('', 5001))
    print ('Listening port : ' + str(5001))
    s.listen(1)  # Listen to one connection
    c, addr = s.accept()
    print ('connection from: ' + str(addr))

    #connector
    phost='172.31.27.25'
    pport=5001
    ps = socket.socket()
    ps.connect((phost, pport))

    while True:
        data1 = c.recv(2048)  # Max bytes
        if not data1:
            break
        data=pickle.loads(data1)
        print ('ce qui est recu :',data)
        ReqType=data['reqType']
        requete=data['req']
        print('requete recuperee:' , requete) 
        if request_validator(requete) :
            ps.send(data1)
            output = ps.recv(2048)
            c.send(output)
        
    c.close()
    ps.close()

if __name__ == '__main__':
    main()

