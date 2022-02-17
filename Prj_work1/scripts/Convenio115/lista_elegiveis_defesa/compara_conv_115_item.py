#!/usr/local/bin/python3.7
# -*- coding: utf-8 -*-
"""
------------------------------------------------------------------------------
MODULO ...: TESHUVA
SCRIPT ...: compara_conv_115_item.py
CRIACAO ..: 19/01/2022
AUTOR ....: EDUARDO DA SILVA FERREIRA 
            / KYROS TECNOLOGIA (eduardof@kyros.com.br)
DESCRICAO.: Este script possibilita a execução 
            dos objetos para comparação de mestre e item
------------------------------------------------------------------------------

------------------------------------------------------------------------------
  HISTORICO : 
  18/01/2022 : EDUARDO DA SILVA FERREIRA 
            / KYROS TECNOLOGIA (eduardof@kyros.com.br) 
        - Criação do Script.
                 
------------------------------------------------------------------------------
"""
#### PATRONIZACAO PARA O PAINEL DE EXECUCOES....
import sys
import os
gv_cc_sep_dir = ('/' if os.name == 'posix' else '\\')
gv_cc_dir_bse = os.path.join( \
    os.path.realpath('.').split( \
    gv_cc_sep_dir+'PROD'+gv_cc_sep_dir)[0], 'PROD') \
    if os.path.realpath('.').__contains__( \
        gv_cc_sep_dir+'PROD'+gv_cc_sep_dir) \
    else os.path.join( os.path.realpath('.').split( \
        gv_cc_sep_dir+'DEV'+gv_cc_sep_dir)[0], 'DEV')
sys.path.append(gv_cc_dir_bse)
# imports do sparta
import configuracoes
from layout import \
    carregaLayout\
    , quebraRegistro       
from comum import \
    log \
    , carregaConfiguracoes\
    , buscaDadosSerie\
    , encodingDoArquivo             
log.gerar_log_em_arquivo = True
carregaConfiguracoes(configuracoes)
carregaLayout()
# demais imports
import traceback
from glob import glob

gv_ob_dados_serie = {}


def listaArquivosComparar(p_nm_diretorio,p_ob_lista_arqs_mestre_comparar=[]):
    """
    Lista de Arquivos do Diretório -> lista
    """
    global gv_ob_dados_serie
    global gv_cc_sep_dir
    v_ob_lista_processar = []
    v_nm_diretorio = p_nm_diretorio + str(gv_cc_sep_dir) + "*.*"
    log("Diretorio de busca....: " + str(v_nm_diretorio) + " >> ")

    if  not os.path.exists(p_nm_diretorio):
        log("ERRO >> Diretorio Inexistente ! " + str(p_nm_diretorio) + " >> ")
        return None

    if not gv_ob_dados_serie:
        log("ERRO >> Não existe dados da serie! >> ")
        return None

    try:
        v_cc_uf = str(gv_ob_dados_serie.get("uf","")).strip().upper()
        v_nr_pos = 28\
        if int(gv_ob_dados_serie.get("ano",0)) >= 2017 else 10        

        v_ob_lista_arquivos = [nm_arquivo for nm_arquivo in glob(v_nm_diretorio, recursive=False)]  

        #v_ob_lista_arquivos = [nm_arquivo\
        #                      for nm_arquivo in os.listdir(p_nm_diretorio)]
        if not v_ob_lista_arquivos:
            log("ERRO >> Arquivos Inexistente ! " + str(p_nm_diretorio) + " >> ")
            return None            

        for cc_arquivo in v_ob_lista_arquivos:
            v_nm_arquivo = ""
         
            try:
                v_nm_arquivo = cc_arquivo.split(gv_cc_sep_dir)[-1]
            except:
                v_nm_arquivo = ""
            
            if not v_nm_arquivo:
                log('Ignorando arquivo com nome fora do padrao', cc_arquivo + " ...")
                continue

            if  not os.path.isfile(cc_arquivo):
                log('Ignorando arquivo com nome fora do padrao', cc_arquivo + " ...")
                continue

            try:
                if v_nm_arquivo.strip().upper().startswith(v_cc_uf):
                    if v_nm_arquivo[v_nr_pos] == "I":
                        v_nm_arquivo_mestre_teste = v_nm_arquivo
                        #log("v_nm_arquivo_mestre_teste: " + str(v_nm_arquivo_mestre_teste))
                        v_ob_troca = list(v_nm_arquivo_mestre_teste) 
                        v_ob_troca[v_nr_pos] = "M"
                        v_nm_arquivo_mestre_teste = ''.join(v_ob_troca)
                        #log("v_nm_arquivo_mestre_teste: " + str(v_nm_arquivo_mestre_teste))
                        #se arquivo_mestre_teste em v_lista_arqs_Mestre_comparar faça
                        #   se arquivo não esta em lista_processar faça
                        #       lista_processar.append(arquivo)
                        if v_nm_arquivo_mestre_teste in p_ob_lista_arqs_mestre_comparar:
                            if v_nm_arquivo not in v_ob_lista_processar:
                                v_ob_lista_processar.append(v_nm_arquivo)

            except:
                pass    

        try:        
            v_ob_lista_processar.sort(reverse=True)        
        except:
            pass
        return v_ob_lista_processar

    except Exception as err:
        err_desc_trace = traceback.format_exc()
        log("ERRO >> BUSCAR ARQUIVOS >> "\
        + str(err) + " - TRACE - " + err_desc_trace + " >> ")
        return None


def comparaItem(p_id_serie):
    """
    Compara dados de Mestre e Item
    """
    v_nr_quantidade_autenticos = 0
    global gv_ob_dados_serie
    global gv_cc_sep_dir
    try:
        if not p_id_serie:
            log("ERRO >> ID SERIE Inexistente ! " + str(p_id_serie) + " >> ")
            return None              
        
        gv_ob_dados_serie =\
        buscaDadosSerie(p_id_serie)  

        if not gv_ob_dados_serie:
            log("ERRO >> Não existe dados da serie! >> ")
            return None

        if not gv_ob_dados_serie.get("uf",""):
            log("ERRO >> Não existe dados da UF na serie! >> ")
            return None
        
        if not gv_ob_dados_serie.get("ano",""):
            log("ERRO >> Não existe dados da ANO na serie! >> ")
            return None

        if not gv_ob_dados_serie.get("dir_serie",""):
            log("ERRO >> Não existe dados do DIRETORIO na serie! >> ")
            return None

        v_nm_diretorio_atual_ti =\
        gv_ob_dados_serie.get("dir_serie","")\
        + str(gv_cc_sep_dir)\
        + 'OBRIGACAO'
        v_nm_diretorio_ultima_entrega =\
        gv_ob_dados_serie.get("dir_serie","")\
        + str(gv_cc_sep_dir)\
        + 'ULTIMA_ENTREGA'

        log("v_nm_diretorio_atual_ti >> " + str(v_nm_diretorio_atual_ti))
        log("v_nm_diretorio_ultima_entrega >> " + str(v_nm_diretorio_ultima_entrega))

        if  not os.path.exists(v_nm_diretorio_atual_ti):
            log("ERRO >> Diretorio Inexistente ! " + str(v_nm_diretorio_atual_ti) + " >> ")
            return None

        if  not os.path.exists(v_nm_diretorio_ultima_entrega):
            log("ERRO >> Diretorio Inexistente ! " + str(v_nm_diretorio_ultima_entrega) + " >> ")
            return None

        v_diretorio_saida = getattr(configuracoes, 'diretorio_saida',"")
        v_diretorio_saida = str(v_diretorio_saida).replace("<<id_serie>>",p_id_serie).strip()
        
        log("diretorio_saida >> " + str(v_diretorio_saida ))
        
        if  not os.path.exists(v_diretorio_saida):
            log("Diretorio Inexistente ! " + str(v_diretorio_saida) + " >> ")
            os.makedirs(v_diretorio_saida)
        
        v_arq_saida = getattr(configuracoes, 'arq_saida',"")
        if not v_arq_saida:
            log("ERRO >> Arquivo Inexistente [SAIDA] ! " + str(v_arq_saida) + " >> ")
            return None

        v_arq_sai_mestre_identicos = v_arq_saida
        v_arq_sai_mestre_identicos = str(v_arq_sai_mestre_identicos).replace("<<TIPO>>","M").strip()
        v_arquivo_saida_mestre_identicos = os.path.join( v_diretorio_saida, v_arq_sai_mestre_identicos)
        log("v_arquivo_saida_mestre_identicos >> " + str(v_arquivo_saida_mestre_identicos ))
        if  not os.path.isfile(v_arquivo_saida_mestre_identicos):
            log("ERRO >> Arquivo Inexistente [SAIDA] ! " + str(v_arquivo_saida_mestre_identicos) + " >> ")
            return None
        v_ob_lista_saida = set()
        v_ob_lista_saida_ultima_entrega  = set()
        v_nr_pos_arquivo = -1
        v_nr_pos_arquivo_ultima_entrega  = -1
        v_nr_pos_uf = -1
        v_nr_pos_dt_emissao = -1
        v_nr_pos_nr_nota = -1
        v_nr_pos_serie = -1
        v_nr_pos_modelo = -1
        # Buscar dados arquivo_saida_mestre_identicos_mestre
        ob_encoding_arquivo_saida_mestre_identicos = encodingDoArquivo( v_arquivo_saida_mestre_identicos )
        fd_arquivo_saida_mestre_identicos = open(v_arquivo_saida_mestre_identicos, 'r', encoding=ob_encoding_arquivo_saida_mestre_identicos)
        for i, linha in enumerate(fd_arquivo_saida_mestre_identicos):
            ob_linha = [str(campo).replace(chr(13), "")\
                                .replace(chr(10), "")\
                                .replace('\n', '')\
                                .replace('\r', '')\
                                .replace(";", "")\
                                .replace('"', '')\
                                .replace("'", "")\
                                .strip()                
                        for campo in linha.split(";")]
            if not i:
                for i,v in enumerate(ob_linha):
                    if str(v).upper().strip().endswith("M_ENTREGUE"):
                        if str(v).upper().strip().startswith("NOME_ARQUIVO_MESTRE")\
                        and v_nr_pos_arquivo_ultima_entrega == -1:
                            v_nr_pos_arquivo_ultima_entrega = i
                    if str(v).upper().strip().endswith("_M_ATUAL"):
                        if str(v).upper().strip().startswith("NOME_ARQUIVO_MESTRE")\
                        and v_nr_pos_arquivo == -1:
                            v_nr_pos_arquivo = i                    
                        if str(v).upper().strip().startswith("NUMERO_NF")\
                        and v_nr_pos_nr_nota == -1:
                            v_nr_pos_nr_nota = i
                        if str(v).upper().strip().startswith("DATA_EMISSAO")\
                        and v_nr_pos_dt_emissao == -1:
                            v_nr_pos_dt_emissao = i
                        if str(v).upper().strip().startswith("UF_")\
                        and v_nr_pos_uf == -1:
                            v_nr_pos_uf = i
                        if str(v).upper().strip().startswith("SERIE_")\
                        and v_nr_pos_serie == -1:
                            v_nr_pos_serie = i
                        if str(v).upper().strip().startswith("MODELO_")\
                        and v_nr_pos_modelo == -1:
                            v_nr_pos_modelo = i
                if v_nr_pos_arquivo_ultima_entrega == -1\
                or v_nr_pos_arquivo == -1\
                or v_nr_pos_uf == -1\
                or v_nr_pos_dt_emissao == -1\
                or v_nr_pos_nr_nota == -1\
                or v_nr_pos_serie == -1\
                or v_nr_pos_modelo == -1:
                    log("ERRO >> Inexistente [SAIDA] de registros para arquivos mestre ! " + str(v_arquivo_saida_mestre_identicos) + " >> ")
                    return None        
                log("v_nr_pos_arquivo_ultima_entrega >> " + str(v_nr_pos_arquivo_ultima_entrega ))   
                log("v_nr_pos_arquivo >> " + str(v_nr_pos_arquivo ))   
                log("v_nr_pos_uf >> " + str(v_nr_pos_uf )) 
                log("v_nr_pos_dt_emissao >> " + str(v_nr_pos_dt_emissao ))      
                log("v_nr_pos_nr_nota >> " + str(v_nr_pos_nr_nota )) 
                log("v_nr_pos_serie >> " + str(v_nr_pos_serie ))
                log("v_nr_pos_modelo >> " + str(v_nr_pos_modelo ))
                continue
            v_ob_lista_saida.add(ob_linha[v_nr_pos_arquivo])
            v_ob_lista_saida_ultima_entrega.add(ob_linha[v_nr_pos_arquivo_ultima_entrega])
        fd_arquivo_saida_mestre_identicos.close()
        log("lista_arquivos_mestre_saida >> " + str(v_ob_lista_saida ))
        log("lista_arquivos_mestre_saida_ultima_entrega >> " + str(v_ob_lista_saida_ultima_entrega ))           
        if  not v_ob_lista_saida or not  v_ob_lista_saida_ultima_entrega:
            log("ERRO >> Inexistente [SAIDA] para arquivos mestre ! " + str(v_arquivo_saida_mestre_identicos) + " >> ")
            return None                
        v_ob_lista_arqs_mestre_comparar = list(v_ob_lista_saida)
        v_ob_lista_arquivos_diretorio_atual_ti =\
        listaArquivosComparar(v_nm_diretorio_atual_ti,v_ob_lista_arqs_mestre_comparar)
        v_ob_lista_arqs_mestre_comparar = list(v_ob_lista_saida_ultima_entrega)
        v_ob_lista_arquivos_diretorio_ultima_entrega =\
        listaArquivosComparar(v_nm_diretorio_ultima_entrega,v_ob_lista_arqs_mestre_comparar)
        log("v_ob_lista_arquivos_diretorio_atual_ti >> " + str(v_ob_lista_arquivos_diretorio_atual_ti))
        log("v_ob_lista_arquivos_diretorio_ultima_entrega >> " + str(v_ob_lista_arquivos_diretorio_ultima_entrega))
        if  not v_ob_lista_arquivos_diretorio_atual_ti:
            log("ERRO >> Arquivo Inexistente ! " + str(v_nm_diretorio_atual_ti) + " >> ")
            return None
        if  not v_ob_lista_arquivos_diretorio_ultima_entrega:
            log("ERRO >> Arquivo Inexistente ! " + str(v_nm_diretorio_ultima_entrega) + " >> ")
            return None

        v_LayoutItem           = 'LayoutItem'
        v_dic_campos = dict()
        try:
            if int(gv_ob_dados_serie.get("ano",0)) < 2017: 
                v_LayoutItem           += '_Antigo'
        except:
            log("ERRO >> Busca Layout_Antigo ! " + str(gv_ob_dados_serie.get("ano",0)) + " >> ")
            return None        
        log("LayoutItem >> " + str(v_LayoutItem ))                
        try:
            v_dic_campos[v_LayoutItem] =\
            v_dic_campos.get(v_LayoutItem, carregaLayout.dic_layouts[v_LayoutItem]['dic_campos'])        
        except:
            log("ERRO >> Carregamento LayoutItem ! " + str(gv_ob_dados_serie.get("ano",0)) + " >> ")
            return None        
        log("dic_campos >> " + str(v_dic_campos)) 

        v_cc_uf = str(gv_ob_dados_serie.get("uf","")).strip().upper()
        #pos = 28 se int(dados_serie[‘ano’]) >= 2017 senão 10
        v_nr_pos = 28\
        if int(gv_ob_dados_serie.get("ano",0)) >= 2017 else 10        

        # criar o arquivo de saida do item
        v_arq_saida_item = v_arq_saida
        v_arq_saida_item = str(v_arq_saida_item).replace("<<TIPO>>","I").strip()
        v_arquivo_saida_item = os.path.join( v_diretorio_saida, v_arq_saida_item )
        fd_arquivo_saida_item = open(v_arquivo_saida_item, 'w', newline='')
        
        # arquivo_mestre_anterior = ‘’
        nr_contador_linhas = 0
        arquivo_mestre_anterior = ''
        # para cada linha no arq_saida_resultado_Mestre_identicos faça
        v_lista_itens = list()
        v_ds_cabecalho = ""
        v_ds_linha     = ""
        ob_encoding_arquivo_saida_mestre_identicos = encodingDoArquivo( v_arquivo_saida_mestre_identicos )
        fd_arquivo_saida_mestre_identicos = open(v_arquivo_saida_mestre_identicos, 'r', encoding=ob_encoding_arquivo_saida_mestre_identicos)
        for i, linha in enumerate(fd_arquivo_saida_mestre_identicos):
            if not i:
                v_ds_cabecalho = str(linha)\
                                 .replace('\n', '')\
                                 .replace('\t', '')        
                continue
            else:
                v_ds_linha = str(linha)\
                            .replace('\n', '')\
                            .replace('\t', '')
            arq_saida_resultado_mestre_identicos = [str(campo).replace(chr(13), "")\
                                .replace(chr(10), "")\
                                .replace('\n', '')\
                                .replace('\r', '')\
                                .replace(";", "")\
                                .replace('"', '')\
                                .replace("'", "")\
                                .strip()                
                        for campo in linha.split(";")]
            arquivo_mestre = arq_saida_resultado_mestre_identicos[v_nr_pos_arquivo]
            arquivo_mestre_ultima_entrega = arq_saida_resultado_mestre_identicos[v_nr_pos_arquivo_ultima_entrega]            
            #pega o nome do arquivo_mestre na linha
            # ?????? nao existe empresa e filial
            #pega a chave da nota (Empresa, Filial, UF Filial, Data de Emissão, Número de NF)
            uf = arq_saida_resultado_mestre_identicos[v_nr_pos_uf]
            nr_nota = arq_saida_resultado_mestre_identicos[v_nr_pos_nr_nota]
            dt_emissao = arq_saida_resultado_mestre_identicos[v_nr_pos_dt_emissao]
            serie = arq_saida_resultado_mestre_identicos[v_nr_pos_serie]
            modelo = arq_saida_resultado_mestre_identicos[v_nr_pos_modelo]
            #log("exemplo...")
            #log("arquivo_mestre ..." + str(arquivo_mestre))
            #log("uf ..." + str(uf))
            #log("nota ..." + str(nr_nota))
            #log("data emissao ..." + str(dt_emissao))
            #log("serie ..." + str(serie))
            #log("modelo ..." + str(modelo))
            #se arquivo_mestre diferente de arquivo_mestre_anterior faça
            linha_item_atual_ti = ''    
            linha_item_ultima_entrega = ''
            v_ob_reg_item_ultima_entrega = None
            if arquivo_mestre != arquivo_mestre_anterior:
                if arquivo_mestre_anterior:#se arquivo_mestre_anterior diferente de ‘’ faça
                    try:
                        fd_arq_item_atual_ti.close()
                    except:
                        pass
                    try:
                        fd_arq_item_ultima_entrega.close()
                    except:
                        pass
                    #feche o arq_item_atual_ti
                    #feche o arq_item_ultima_entrega
                arquivo_mestre_anterior = arquivo_mestre 
                
                arq_item_atual_ti = arquivo_mestre
                v_ob_troca = list(arq_item_atual_ti) 
                v_ob_troca[v_nr_pos] = "I"
                arq_item_atual_ti = ''.join(v_ob_troca)

                arq_item_ultima_entrega = arquivo_mestre_ultima_entrega 
                v_ob_troca = list(arq_item_ultima_entrega) 
                v_ob_troca[v_nr_pos] = "I"
                arq_item_ultima_entrega = ''.join(v_ob_troca)            

                #se arq_item_atual_ti em v_lista_atual_ti faça
                #    remove da v_lista_atual_ti o arq_cad_atual_ti
                #senão
                #    volte para o inicio do Enquanto ... ???? qual ????
                #se arq_item_ultima_entrega em v_lista_ultima_entrega faça
                #    remove da v_lista_ ultima_entrega o arq_item_ultima_entrega
                #senão
                #    volte para o inicio do Enquanto … ???? qual ????
                if arq_item_atual_ti in v_ob_lista_arquivos_diretorio_atual_ti:
                    v_ob_lista_arquivos_diretorio_atual_ti.remove(arq_item_atual_ti)
                else:
                    continue
                if  arq_item_ultima_entrega in v_ob_lista_arquivos_diretorio_ultima_entrega:
                    v_ob_lista_arquivos_diretorio_ultima_entrega.remove(arq_item_ultima_entrega)
                else:
                    continue 

                #abre o arq_item_atual_ti do diretorio v_diretorio_atual_ti em modo leitura (‘r’)
                #abre o arq_item_ultima_entrega do diretorio v_diretorio_ultima_entrega em modo leitura (‘r’)
                v_arq_item_atual_ti = os.path.join( v_nm_diretorio_atual_ti, arq_item_atual_ti )
                v_arq_item_ultima_entrega = os.path.join( v_nm_diretorio_ultima_entrega, arq_item_ultima_entrega )
                ob_encoding_arq_item_atual_ti = encodingDoArquivo( v_arq_item_atual_ti )
                ob_encoding_arq_item_ultima_entrega = encodingDoArquivo( v_arq_item_ultima_entrega )
                fd_arq_item_atual_ti = open(v_arq_item_atual_ti, 'r', encoding=ob_encoding_arq_item_atual_ti)
                fd_arq_item_ultima_entrega = open(v_arq_item_ultima_entrega, 'r', encoding=ob_encoding_arq_item_ultima_entrega)
                #linha_item_atual_ti = próxima linha do arq_item_atual_ti
                #linha_item_ultima_entrega = próxima linha do arq_item_ultima_entrega
                linha_item_atual_ti = fd_arq_item_atual_ti.readline()    
                ##troca_dicionario>>linha_item_ultima_entrega = fd_arq_item_ultima_entrega.readline()
                v_ob_reg_item_ultima_entrega = dict()
                for linha_item_ultima_entrega in fd_arq_item_ultima_entrega:
                    registro_item_ultima_entrega = quebraRegistro(linha_item_ultima_entrega, v_LayoutItem)
                    v_cc_chave = ""
                    v_cc_chave +=  str(registro_item_ultima_entrega[v_dic_campos[v_LayoutItem]['NUMERO_NF']-1]).strip() + "|" 
                    v_cc_chave +=  str(registro_item_ultima_entrega[v_dic_campos[v_LayoutItem]['DATA_EMISSAO']-1]).strip() + "|"
                    v_cc_chave +=  str(registro_item_ultima_entrega[v_dic_campos[v_LayoutItem]['SERIE']-1]).strip() + "|"
                    v_cc_chave +=  str(registro_item_ultima_entrega[v_dic_campos[v_LayoutItem]['MODELO']-1]).strip() + "|"
                    v_ob_lista_chave = list()
                    if v_cc_chave in v_ob_reg_item_ultima_entrega:
                        v_ob_lista_chave = v_ob_reg_item_ultima_entrega[v_cc_chave]
                    v_ob_lista_chave.append(registro_item_ultima_entrega)
                    v_ob_reg_item_ultima_entrega[v_cc_chave] = v_ob_lista_chave
                registro_item_ultima_entrega = None

            nr_contador_linhas += 1
            v_fim_nota = False
            v_achou_nota = False
            flag_identico = True
            v_lista_itens = []
            #enquanto não v_fim_nota e não v_achou_nota faça
            while (not v_fim_nota and not v_achou_nota)\
            and linha_item_atual_ti and v_ob_reg_item_ultima_entrega:##troca_dicionario>> and linha_item_ultima_entrega:
            
                registro_item_atual_ti = quebraRegistro(linha_item_atual_ti, v_LayoutItem)
                registro_item_ultima_entrega = None ##troca_dicionario>>registro_item_ultima_entrega = quebraRegistro(linha_item_ultima_entrega, v_LayoutItem)
                #se chave da nota igual a chave em registro_item_atual_ti faça
                # 'UF': 2 m 'DATA_EMISSAO': 6, 'MODELO': 7, 'SERIE': 8, 'NUMERO_NF'
                item_uf = str(registro_item_atual_ti[v_dic_campos[v_LayoutItem]['UF']-1]).strip()
                item_nr_nota = str(registro_item_atual_ti[v_dic_campos[v_LayoutItem]['NUMERO_NF']-1]).strip()
                item_dt_emissao = str(registro_item_atual_ti[v_dic_campos[v_LayoutItem]['DATA_EMISSAO']-1]).strip() 
                item_serie = str(registro_item_atual_ti[v_dic_campos[v_LayoutItem]['SERIE']-1]).strip()
                item_modelo = str(registro_item_atual_ti[v_dic_campos[v_LayoutItem]['MODELO']-1]).strip()
                v_cc_chave = ""
                v_cc_chave +=  str(registro_item_atual_ti[v_dic_campos[v_LayoutItem]['NUMERO_NF']-1]).strip() + "|" 
                v_cc_chave +=  str(registro_item_atual_ti[v_dic_campos[v_LayoutItem]['DATA_EMISSAO']-1]).strip() + "|"
                v_cc_chave +=  str(registro_item_atual_ti[v_dic_campos[v_LayoutItem]['SERIE']-1]).strip() + "|"
                v_cc_chave +=  str(registro_item_atual_ti[v_dic_campos[v_LayoutItem]['MODELO']-1]).strip() + "|"
                    
                #log("item uf ..." + str(item_uf))
                #log("item nota ..." + str(item_nr_nota))
                #log("item data emissao ..." + str(item_dt_emissao))
                #log("item serie ..." + str(item_serie))
                #log("item modelo ..." + str(item_modelo))
                if item_nr_nota == nr_nota\
                and item_dt_emissao == dt_emissao\
                and item_serie == serie\
                and item_modelo == modelo\
                :            
                    v_achou_nota = True
                    flag_identico = False
                    #para campo em v_campos_item_validar faça
                    v_campos_arq_item_atual_ti_validar =\
                    str(registro_item_atual_ti[v_dic_campos[v_LayoutItem]['HASH_CODE_ARQ']-1]).strip()
                    if v_cc_chave in v_ob_reg_item_ultima_entrega:
                        for reg in v_ob_reg_item_ultima_entrega[v_cc_chave]:
                            registro_item_ultima_entrega = reg        
                            v_campos_arq_item_ultima_entrega_validar =\
                            str(registro_item_ultima_entrega[v_dic_campos[v_LayoutItem]['HASH_CODE_ARQ']-1]).strip()
                            #se registro_item_atual_ti[campo] diferente de registro_item_ultima_entrega[campo] faça
                            if v_campos_arq_item_atual_ti_validar == v_campos_arq_item_ultima_entrega_validar:
                                flag_identico = True
                                break

                    if flag_identico:
                        pass
                        #Apenda em v_lista_itens os dados 
                        # registro_mestre_atual_ti, 
                        # registro_cad_atual_ti, 
                        # registro_item_atual_ti
                        v_nr_quantidade_autenticos += 1
                        for i,(k,v) in enumerate(v_dic_campos[v_LayoutItem].items()):    
                            if v_nr_quantidade_autenticos==1:
                                if not i:
                                    v_ds_cabecalho += '"NOME_ARQUIVO_ITEM_I_ATUAL"' + ';'
                                    v_ds_cabecalho += '"NOME_ARQUIVO_ITEM_I_ENTREGUE"' + ';'

                                v_ds_cabecalho += '"' + str(k) + '_I_ATUAL"' + ';'                                    
                                v_ds_cabecalho += '"' + str(k) + '_I_ENTREGUE"' + ';'                                    

                            if not i:
                                v_ds_linha += '"' + str(arq_item_atual_ti) + '"' + ';'
                                v_ds_linha += '"' + str(arq_item_ultima_entrega) + '"' + ';'

                            v_ds_linha += \
                                '"' + (str(registro_item_atual_ti[int(v)-1])\
                                .replace(chr(13), "")\
                                .replace(chr(10), "")\
                                .replace('\n', '')\
                                .replace('\r', '')\
                                .replace(";", "")\
                                .strip()\
                                ) + '"' + ';'
                            v_ds_linha += \
                                '"' + (str(registro_item_ultima_entrega[int(v)-1])\
                                .replace(chr(13), "")\
                                .replace(chr(10), "")\
                                .replace('\n', '')\
                                .replace('\r', '')\
                                .replace(";", "")\
                                .strip()\
                                ) + '"' + ';'

                        if v_nr_quantidade_autenticos==1:
                            if v_ds_cabecalho:
                                v_lista_itens.append(v_ds_cabecalho)
                        if v_ds_linha:
                           v_lista_itens.append(v_ds_linha)
                         
                else:#senão faça
                    if v_achou_nota: #se v_achou_nota for verdadeiro faça
                        v_fim_nota = True
                linha_item_atual_ti = fd_arq_item_atual_ti.readline()    
                ##troca_dicionario>>linha_item_ultima_entrega = fd_arq_item_ultima_entrega.readline()    
            #se flag_identico for verdadeiro e v_achou_nota for verdadeiro faça
            if flag_identico and v_achou_nota:
                pass
                #para cada linha em v_lista_itens os dados faça
                # escreve no arq_saida_resultado_identicos os campos de 
                #               registro_mestre_atual_ti, 
                #               registro_cad_atual_ti, 
                #               registro_item_atual_ti 
                # conforme layout de modelo.
                for linha in v_lista_itens:
                    fd_arquivo_saida_item.write(linha + '\n') 
                v_lista_itens = list()            
        try:
           fd_arquivo_saida_item.close()
        except:
            pass
        try:
           fd_arquivo_saida_mestre_identicos.close()
        except:
            pass
        try:
            fd_arq_item_atual_ti.close()
        except:
            pass
        try:
            fd_arq_item_ultima_entrega.close()
        except:
            pass
        log("*"*150)
        log("")
        log(" - Quantidade de registros autenticos >> " + str(v_nr_quantidade_autenticos ))
        log(" - Arquivo de saida do item >> " + str(v_arquivo_saida_item ))
        log("")
        log("*"*150)
        return v_nr_quantidade_autenticos

    except Exception as err:
        err_desc_trace = traceback.format_exc()
        log("ERRO >> COMPARAR ARQUIVOS >> "\
        + str(err) + " - TRACE - " + err_desc_trace + " >> ")
        try:
           fd_arquivo_saida_item.close()
        except:
            pass
        try:
           fd_arquivo_saida_mestre_identicos.close()
        except:
            pass
        try:
            fd_arq_item_atual_ti.close()
        except:
            pass
        try:
            fd_arq_item_ultima_entrega.close()
        except:
            pass
                
        return None

if __name__ == "__main__":
    if len(sys.argv) < 2:
        log('#### ERRO ') 
        log('-'* 100)
        log('QUANTIDADE DE PARAMETROS INVALIDA')
        log('-'* 100)
        log('EXEMPLO')
        log('-'* 100)
        log( '%s <ID SERIE LEVANTAMENTO>'%( sys.argv[0] ) )
        log('-'* 100)
        sys.exit(99)
    else:
        v_id_serie = sys.argv[1]
        log("v_id_serie: " + str(v_id_serie))
        v_cc_retorno = comparaItem(v_id_serie)
        log(v_cc_retorno)
        if v_cc_retorno is None:
            sys.exit(99)
        else:
            sys.exit(0)    