#!/usr/local/bin/python3.7
# -*- coding: utf-8 -*-
import sys
from time import sleep

rc = 0
print("argumentos recebidos:", sys.argv[:] )
arq = open("teste1.txt","w")
for st in sys.argv[:]:
    arq.write("  ")
    arq.write(st)
arq.close()
sleep(10)
print("terminou com rc = ", rc)
sys.exit(rc)

