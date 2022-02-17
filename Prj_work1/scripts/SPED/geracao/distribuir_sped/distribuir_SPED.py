#!/usr/local/bin/python3.7
# -*- coding: utf-8 -*-
"""
----------------------------------------------------------------------------------------------
  SISTEMA ..: 
  MODULO ...: 
  SCRIPT ...: prepara_arquivos.py
  CRIACAO ..: 27/10/2020
  AUTOR ....: Airton Borges da Silva Filho / KYROS Consultoria
  DESCRICAO : Renomeia os arquivos REGERADOS E PROTOCOLADOS nas pastas /arquivos/SPED_FISCAL/REGERADOS e
                /arquivos/SPED_FISCAL/REGERADOS para os nomes padronizados 
                e move para as sub-pastas necessárias para o processamento.
----------------------------------------------------------------------------------------------
  HISTORICO : 
    * 27/10/2020 - Airton Borges da Silva Filho / KYROS Consultoria - Criacao do script.
----------------------------------------------------------------------------------------------
      31/08/2021 - Arthur - PTITES-136
                 - Ajuste do script para o novo Padrão.  
----------------------------------------------------------------------------------------------                 
"""
import sys
import os
SD = '/' if os.name == 'posix' else '\\'
dir_base = os.path.join( os.path.realpath('.').split(SD+'PROD'+SD)[0], 'PROD') if os.path.realpath('.').__contains__(SD+'PROD'+SD) else os.path.join( os.path.realpath('.').split(SD+'DEV')[0], 'DEV')
sys.path.append(dir_base)
import configuracoes
import comum
import sql
import util

import datetime
import shutil
from pathlib import Path
global ret

comum.carregaConfiguracoes(configuracoes)

disco = ('' if os.name == 'posix' else 'D:')

def tail(f, n):
    assert n >= 0
    pos, lines = n+1, []
    while len(lines) <= n:
        try:
            f.seek(-pos, 2)
        except IOError:
            f.seek(0)
            break
        finally:
            lines = list(f)
        pos *= 2
    return lines[-n:]

def nome_arquivo(mascara,diretorio):
    qdade = 0
    nomearq = "" 
    directory = Path(diretorio)
    files = directory.glob(mascara)
    sorted_files = sorted(files, reverse=False)
    if sorted_files:
        for f in sorted_files:
            qdade = qdade + 1
            nomearq = f
            log("# ", qdade ," - ",f )
    else: 
        nomearq=""
    return(nomearq)

def lista_arquivos(mascara,diretorio):
    #data_criacao = lambda f: f.stat().st_ctime removido, por mim
    data_modificacao = lambda f: f.stat().st_mtime
    qdade = 0
    nomearq = [] 
    directory = Path(diretorio)
    files = directory.glob(mascara)
    sorted_files = sorted(files, key=data_modificacao, reverse=False)
    if sorted_files:
        for f in sorted_files:
            qdade = qdade + 1
            nomearq.append(f)
            log("# ",qdade ," - ",f )
    else: 
        nomearq = []
    return(nomearq)

def finalok(f):
    arqok = False
    nlin=1
    try :
        fd = open(f,'r') 
        lin = fd.readline()
    except :
        fd = open(f,'r', encoding=comum.encodingDoArquivo(f))
        lin = fd.readline() 
    try :    
        while (lin):
            nlin = nlin + 1
            if (lin.startswith('|9999|')):
                arqok = True
                break
            else:
                lin = fd.readline()
    except Exception as e :
        arqok = False 
        log("#")
        log("#### ERRO. - ERRO ENCONTRADO NO ARQUIVO APÓS A LINHA ", '{:,}'.format(nlin).replace(',','.'))
        log("#### ERRO. - CÓDIGO INTERNO DO ERRO NO SYSTEMA = ", e)
        log("#### ERRO. - PROCURA PELO |9999| INTERROMPIDA DEVIDO A ESTE ERRO. ")
        
        if (str(e).startswith("'charmap'")):
            ec=str(e).split(' ')[5]
            log("#### ERRO. - CÓDIGO DO CARACTERE NÃO RECONHECIDO = ", ec)
    fd.close()
    return(arqok)

def retornaIDArquivo(path) :
    try :
        fd = open(path,'r') 
        lin = fd.readline()
    except :
        fd = open(path,'r', encoding=comum.encodingDoArquivo(path))
        lin = fd.readline()
    fd.close()
    if lin and lin.startswith('|0000|') :
        ano = lin.split('|')[4][4:] 
        mes = lin.split('|')[4][2:4]
        uf = lin.split('|')[9]
        insc = lin.split('|')[10]
        compet_i = lin.split('|')[4]
        compet_f = lin.split('|')[5]
        return [uf, insc, compet_i, compet_f, mes, ano] or [ False, False, False, False, False, False]
    return False, False, False, False, False, False
      
def dtf():
    return (datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S'))

def processar():
    global dir_base
    ret = 0
    dir_base_sped = SD + 'arquivos' + SD + 'SPED_FISCAL'
    dir_protocolados = os.path.join(dir_base_sped, 'PROTOCOLADOS')
    dir_regerados = os.path.join(dir_base_sped, 'REGERADOS')

    log("Diretorio protocolado: ", dir_protocolados)
    log("Diretorio regerados: ", dir_regerados)

    if not os.path.isdir(dir_protocolados) :
        os.makedirs(dir_protocolados)
    if not os.path.isdir(dir_regerados) :
        os.makedirs(dir_regerados)

    mascara_regerado = "*.*"
    mascara_protocolado = "*.*"
        
    log("-"* 100)    
    log("# Lista de arquivos REGERADOS a serem processados ...")    
    listaregerados = lista_arquivos(mascara_regerado,dir_regerados)
    log("-"* 100)    
    log("# Lista de arquivos PROTOCOLADOS a serem processados ...")    
    listaprotocolados = lista_arquivos(mascara_protocolado,dir_protocolados)
    log("-"* 100)    


    ####  REGERADOS


    log("# ")
    log("# Organizando os arquivos REGERADOS...")
    log("#")
    for arquivo in listaregerados:
        log("-"*100)
        log("# Arquivo ",arquivo)
        ufa,iea,dtia,dtfa,ma,aa = retornaIDArquivo(arquivo)
        if (util.validauf(ufa) and iea and dtia and int(ma) > 0 and int(ma) < 13 and int(aa) > (datetime.datetime.now().year)-50 and int(aa) <=datetime.datetime.now().year):
            log("# UF           = ", ufa)
            log("# IE           = ", iea)
            log("# Data inicial = ", dtia)
            log("# Data final   = ", dtfa)
            log("# Mês          = ", ma)
            log("# Ano          = ", aa)
            log("#")

            log("# Verificando se existe registro |9999| no arquivo, aguarde... ")
            if (finalok(arquivo)):
                log("# Registro final |9999| encontrando.  continuando...")
            else:
                log("#"*100)
                log("#")
                log("#### ERRO. - REGISTRO FINAL |9999| NÃO ENCONTRADO.")
                log("#### ERRO. - REMOVA OU CORRIJA O ARQUIVO ", arquivo)
                log("#")
                log("#"*100)
                ret = 99
                continue

            mascara = "SPED_"+ma+aa+"_"+ufa+"_"+iea+"_REG*.txt"
            log("# Nome padronizado = ", mascara)
            nova_pasta = os.path.join(dir_regerados,ufa,aa,ma)
            if not os.path.isdir(nova_pasta) :
                os.makedirs(nova_pasta)
            log("# ")
            log("# Arquivos existentes que possuem o nome padrão...")
            ultimo_arquivo = nome_arquivo(mascara,nova_pasta)
            if (ultimo_arquivo == ""):
                novo_nome = "SPED_"+ma+aa+"_"+ufa+"_"+iea+"_REG_V001.txt"
            else:
                proximo = '{:03d}'.format(int((str(ultimo_arquivo).split(".")[0]).split("_")[6][1:]) + 1)
                novo_nome = "SPED_"+ma+aa+"_"+ufa+"_"+iea+"_REG_V"+proximo+".txt"
            novo_arquivo = os.path.join(nova_pasta,novo_nome)
            velho_arquivo = os.path.join(dir_regerados,arquivo)
            log("#")
            log("# Definições de padronização e distribuição :")
            log("# Arquivo origem  = ",velho_arquivo)
            log("# Arquivo destino = ",novo_arquivo)
            log("#")
            shutil.move(velho_arquivo,novo_arquivo, copy_function = shutil.copytree)
        else:
            log("#"*100)
            log("#")
            log("#### ERRO. - INVÁLIDO.")
            log("#### ERRO. - REMOVA OU CORRIJA O ARQUIVO ", arquivo)
            log("#")
            log("#"*100)
            ret = 99
    log("-"*100)
    

    ####  PROTOCOLADOS


    log("# ")
    log("# Organizando os arquivos PROTOCOLADOS...")
    log("#")
    for arquivo in listaprotocolados:
        log("-"*100)
        log("# Arquivo ",arquivo)
        ufa,iea,dtia,dtfa,ma,aa = retornaIDArquivo(arquivo)
        if (util.validauf(ufa) and iea and dtia and int(ma) > 0 and int(ma) < 13 and int(aa) > (datetime.datetime.now().year)-50 and int(aa) <=datetime.datetime.now().year):
            log("# UF           = ", ufa)
            log("# IE           = ", iea)
            log("# Data inicial = ", dtia)
            log("# Data final   = ", dtfa)
            log("# Mês          = ", ma)
            log("# Ano          = ", aa)
            log("#")

            log("# Verificando se existe registro |9999| no arquivo, aguarde... ")
            if (finalok(arquivo)):
                log("# Registro final |9999| encontrando.  continuando...")
            else:
                log("#"*100)
                log("#")
                log("#### ERRO. - REGISTRO FINAL |9999| NÃO ENCONTRADO.")
                log("#### ERRO. - REMOVA OU CORRIJA O ARQUIVO ", arquivo)
                log("#")
                log("#"*100)
                ret = 99
                continue

            mascara = "SPED_"+ma+aa+"_"+ufa+"_"+iea+"_PROT*.txt"
            log("# Nome padronizado = ", mascara)
            nova_pasta = os.path.join(dir_protocolados,ufa,aa,ma)
            if not os.path.isdir(nova_pasta) :
                os.makedirs(nova_pasta)
            log("# ")
            log("# Arquivos existentes que possuem o nome padrão...")
            ultimo_arquivo = nome_arquivo(mascara,nova_pasta)
            if (ultimo_arquivo == ""):
                novo_nome = "SPED_"+ma+aa+"_"+ufa+"_"+iea+"_PROT_V001.txt"
            else:
                proximo = '{:03d}'.format(int((str(ultimo_arquivo).split(".")[0]).split("_")[6][1:]) + 1)
                novo_nome = "SPED_"+ma+aa+"_"+ufa+"_"+iea+"_PROT_V"+proximo+".txt"
            novo_arquivo = os.path.join(nova_pasta,novo_nome)
            velho_arquivo = os.path.join(dir_protocolados,arquivo)
            log("#")
            log("# Definições de padronização e distribuição :")
            log("# Arquivo origem  = ",velho_arquivo)
            log("# Arquivo destino = ",novo_arquivo)
            log("#")
            shutil.move(velho_arquivo,novo_arquivo, copy_function = shutil.copytree)
        else:
            log("#"*100)
            log("#")
            log("#### ERRO. - INVÁLIDO.")
            log("#### ERRO. - REMOVA OU CORRIJA O ARQUIVO ", arquivo)
            log("#")
            log("#"*100)
            ret = 99
    log("-"*100)
    return(ret) 

if __name__ == "__main__":
    ret = processar()

    log("Codigo de saida = ",ret)
    if (ret > 0): 
        log("ERRO, verifique as mensagens anteriores")
    sys.exit(ret)



