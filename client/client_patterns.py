#!/usr/bin/python3.8

""" Ce progamme lit un fichier sql et envoie des requetes a un proxy """

import socket
import time
import pickle

host="54.160.232.203"
port=5001
ReqFile='ReqFile.sql'
whichhit=''

s = socket.socket()
s.connect((host, port))
print('connexion faite :',host)

with open(ReqFile, 'r') as f:
        print('ouvrture fichier: ',ReqFile)
        lines = f.readlines()
        for line in lines:
            cline = line.rstrip()
            print ('On traite la ligne: ',cline)
            if cline.startswith('insert') :
                #la requete est pour le master
                Reqtype=1
                print('insert request')
            elif cline.startswith('select') :
                #la requete est pour le slave
                Reqtype=2
                print('select request')
            else:
                #la requete est en erreur
                Reqtype=3
                print('erreur de line')
            
            if Reqtype != 3 :      
                data={'reqType':Reqtype,'req':cline}
                #https://stackoverflow.com/questions/53576851/socket-programming-in-python-using-pickle
                pickleData = pickle.dumps(data)
                s.send(pickleData)
                print('ligne envoyee')
            rep=s.recv(4096)
            dataRecu=pickle.loads(rep)
            print ('Requete recus par le proxy:' + str(dataRecu.decode('utf-8').strip()))
            time.sleep(0.5)
f.close()
s.close()
print ('socket fermee')


