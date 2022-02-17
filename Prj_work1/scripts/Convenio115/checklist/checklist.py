#!/usr/local/bin/python3.7
# -*- coding: utf-8 -*-
"""
----------------------------------------------------------------------------------------------
  SISTEMA ..: 
  MODULO ...: 
  SCRIPT ...: checklist.py
  CRIACAO ..: 30/11/2021
  AUTOR ....: Airton Borges da Silva Filho / KYROS Consultoria
  DESCRICAO :

            Executa proc : GFCADASTRO.pkg_valida_checklist.sp_valida_checklist 

    ./checklist.py ddmmyyyyinicial ddmmyyyyfinal serie filial uf 
    
    GFCADASTRO.pkg_valida_checklist.sp_valida_checklist (
        p_dtinip => TO_CHAR(:MES_ANO,'DD/MM/YYYY'),
        p_dtfimp => TO_CHAR(LAST_DAY(TO_DATE(:MES_ANO,'DD/MM/YYYY')),'DD/MM/YYYY'),
        p_seriep => :SERIE,
        p_filialp => :FILI_COD,
        p_estadop => :UNFE_SIG,
        p_nivelex => 'C',
        p_returnp => :OUT_RETORNO

"""
import sys
import os

#Airton
#Arquivo 1

SD = ('/' if os.name == 'posix' else '\\')
dir_base = os.path.join( os.path.realpath('.').split(SD+'PROD'+SD)[0], 'PROD') if os.path.realpath('.').__contains__(SD+'PROD'+SD) else os.path.join( os.path.realpath('.').split(SD+'DEV'+SD)[0], 'DEV')
sys.path.append(dir_base)

import configuracoes
import comum
import sql

import cx_Oracle

comum.carregaConfiguracoes(configuracoes)

log = comum.log
status_final = 0
log.gerar_log_em_arquivo = True

ret                 = 0
CC_IE               = ""
CC_FILIAL           = ""
DT_MESANO_INICIO    = ""
DT_MESANO_FIM       = ""
CC_SERIE            = ""


def processar(CC_IE,CC_FILIAL,DT_MESANO_INICIO,DT_MESANO_FIM,CC_SERIE) :
    execproc("SP",CC_FILIAL,DT_MESANO_INICIO,DT_MESANO_FIM,CC_SERIE)

#/portaloptrib/TESHUVA/sparta/PROD/scripts/Saneamento/Telecom/saneador_nf  

    return True


def execproc(CC_UF,CC_FILIAL,DT_MESANO_INICIO,DT_MESANO_FIM,CC_SERIE) :
# =============================================================================
#     GFCADASTRO.pkg_valida_checklist.sp_valida_checklist (
#         p_dtinip => TO_CHAR(:MES_ANO,'DD/MM/YYYY'),
#         p_dtfimp => TO_CHAR(LAST_DAY(TO_DATE(:MES_ANO,'DD/MM/YYYY')),'DD/MM/YYYY'),
#         p_seriep => :SERIE,
#         p_filialp => :FILI_COD,
#         p_estadop => :UNFE_SIG,
#         p_nivelex => 'C',
#         p_returnp => :OUT_RETORNO
# =============================================================================
    connection = sql.geraCnxBD(configuracoes)
    p_returnp  = connection.var(str)

    p_dtinip  = DT_MESANO_INICIO
    p_dtfimp  = DT_MESANO_FIM
    p_seriep  = CC_SERIE
    p_filialp = CC_FILIAL
    p_estadop = CC_UF
    p_nivelex = 'C'
    
    print("PARAMETROS PARA A PROC:")
    print("p_dtinip  = ", p_dtinip)
    print("p_dtfimp  = ", p_dtfimp)
    print("p_seriep  = ", p_seriep)
    print("p_filialp = ", p_filialp)
    print("p_estadop = ", p_estadop)
    print("p_nivelex = ", p_nivelex)
    print("p_returnp = ", p_returnp)


    #GFCADASTRO.pkg_valida_checklist.sp_valida_checklist
    procedure  = "GFCADASTRO.pkg_valida_checklist.sp_valida_checklist" 
    parametros = [  
                    p_dtinip,
                    p_dtfimp,
                    p_seriep,
                    p_filialp,
                    p_estadop,
                    p_nivelex,
                    p_returnp
                ]
     
    log("Chamando a procedure GFCADASTRO.pkg_valida_checklist.sp_valida_checklist ...")
    input("Continua ? ")

    connection.executaProcedure(procedure, *parametros)

    retorno = p_returnp.getvalue()

    print("RETORNO DA CHAMADA DA PROCEDURE:")
    print("p_returnp.getvalue() = ",retorno)

    return [retorno]

def inicializar() :
    ret = 0
 
    if not getattr(configuracoes, 'banco', False) :
        log("Erro falta variavel 'banco' no arquivo de configuração (.cfg).")
        ret = 1

#         addParametro(nomeParametro, identificador = None, descricao = '', obrigatorio = False, exemplo = None, default = False) : 
    comum.addParametro('CC_FILIAL',None, 'Filial(is) a ser processada.', True, '"9144"')
    comum.addParametro('DT_MESANO_INICIO',None, 'Mês e ano inicial, mês com dois di­gitos, ano com quatro di­gitos.', True, '"012015"')
    comum.addParametro('DT_MESANO_FIM',None, 'Mês e ano final, mês com dois di­gitos, ano com quatro di­gitos.', True, '"012015"')
    comum.addParametro('CC_SERIE',None, 'Série a ser processada.', True, '"U K"')
    
    if not comum.validarParametros() :
        ret = 3
        return (ret,False,False,False,False,False)
    
    iei        = "SP"
    filiali    = comum.getParametro('CC_FILIAL')        # Código da filial.  Ex: 0001. 
    mesanoii   = comum.getParametro('DT_MESANO_INICIO') # Tem que ser válida no formato MMYYYY
    mesanofi   = comum.getParametro('DT_MESANO_FIM')    # Pode ser "" ou Tem que ser válida no formato MMYYYY
    seriei     = comum.getParametro('CC_SERIE')         # Serie, ex: 17000400

    iei = iei.strip()

    print(mesanofi)

    if (mesanofi == "" or mesanofi == False):
        log("ATENÇÃO: - Não foi informado MMAAAA final, será considerado o mesmo inicial, ou seja: ",mesanoii )
        mesanofi = mesanoii
    diamesanoi = '01/'+mesanoii[0:2]+'/'+mesanoii[2:6]
    diamesanof = '01/'+mesanofi[0:2]+'/'+mesanofi[2:6]
          
    if (int(mesanoii[0:2]) < 1 or int(mesanoii[0:2]) > 12 or int(mesanofi[0:2]) < 1 or int(mesanofi[0:2]) > 12 ):
        ret = 99
        log("ERRO - Mes inicial informado é inválido!", " Foi informado ", mesanoii[0:2], " MES ANO INICIAL - INVALIDO!")

    if (int(mesanofi[0:2]) < 1 or int(mesanofi[0:2]) > 12 ):
        ret = 99
        log("ERRO - Mes final informado é inválido!", " Foi informado ", mesanofi[0:2] , ". MES ANO FINAL- INVALIDO!")

    if (filiali == ""):
        ret = 99
        log("ERRO - filial não informada")

    if (seriei == ""):
        ret = 99
        log("ERRO - id_série não informada")
   
#    print("Parametros globais ajustados.....")
#    print("iei        = ", iei)
#    print("filiaisi   = ", filiaisi)
#    print("mesanoii   = ", diamesanoi)
#    print("mesanofi   = ", diamesanof)
#    print("seriesi    = ", seriesi)
#    print("threadsi   = ", threadsi)
#    print("checklisti = ", checklisti)
    
    return (ret,iei,filiali,diamesanoi,diamesanof,seriei)

if __name__ == "__main__":
    ret,CC_IE,CC_FILIAL,DT_MESANO_INICIO,DT_MESANO_FIM,CC_SERIE = inicializar()
    if (ret == 0): 
        ret = processar(CC_IE,CC_FILIAL,DT_MESANO_INICIO,DT_MESANO_FIM,CC_SERIE)
        if ( ret != 0) :
            log('ERRO no processamento ... Verifique')
    sys.exit(ret)


