# -*- coding: utf-8 -*-
"""Demonstra exemplo da funcionalidade do pacote de OS
"""
import sys
import os
from datetime import datetime
from traceback import format_exc
import shutil


def df(p_dt_valor: datetime = datetime.now()) -> str:
    """Data atual do sistema operacional em texto

    Args:
        p_dt_valor (datetime, optional): [description]. 
        Defaults to datetime.now().

    Returns:
        str: Data em Texto formatado dd/mm/yyyy
    """
    return p_dt_valor.strftime('%d/%m/%Y %H:%M:%S')


def fnc_formata_tamanho(p_nr_tamanho: int = 0) -> int:
    v_ds_texto = "B"
    v_nr_tamanho = float(p_nr_tamanho)
    if (v_nr_tamanho < (1024)):
        pass
    elif (v_nr_tamanho < (1024 ** 2)):
        v_ds_texto = "K"
        v_nr_tamanho /= 1024
    elif (v_nr_tamanho < (1024 ** 3)):
        v_ds_texto = "M"
        v_nr_tamanho /= (1024 ** 2)
    elif (v_nr_tamanho < (1024 ** 4)):
        v_ds_texto = "G"
        v_nr_tamanho /= (1024 ** 3)
    elif (v_nr_tamanho < (1024 ** 5)):
        v_ds_texto = "T"
        v_nr_tamanho /= (1024 ** 4)
    else:
        v_ds_texto = "P"
        v_nr_tamanho /= (1024 ** 5)
    return f'{round(v_nr_tamanho,2)} {v_ds_texto}'


def fnc_procura_arquivos(p_cc_local: str,\
                    p_cc_term_arquivo: str = "*",\
                    p_cc_local_destino: str = "",\
                    p_cc_acao_destino: str = "",\
                    p_cc_rename_destino: str = ""\
                    ):
    """Formata o tamaho

    Args:
        p_cc_local (str): Caminho de origem do arquivo
        p_cc_term_arquivo (str): (Opcional, valor default *) 
                Parte do arquivo que deseja procurar
        p_cc_local_destino (str): (Opcional, valor default "") 
                Caminho de destino do arquivo
        p_cc_acao_destino (str): (Opcional, valor default "") 
                Acao de destino do arquivo a ser realizado. 
                Exemplo: "" ou cp para copia, mv para mover, del para apagar

    Raises:
        Exception: [description]

    Returns:
        [type]: Codigo Erro e a Lista 
    """
    v_nr_ret = 0
    v_ob_lista = []
    v_ob_dict = {}
    v_ob_dict['STATUS'] = ''
    v_ob_dict['MSG'] = ''
    v_ob_dict['OPER_DESTINO'] = 'N/A'
    if p_cc_local_destino:
        if str(p_cc_local_destino).strip().lower() in ("cp", "del", "mv", "ren"):
            v_nr_ret = 1
            v_ob_dict['STATUS'] = 'ERRO'
            v_ob_dict['MSG'] += f' >> A operacao destino é irregular !{p_cc_acao_destino} junto com o destino {p_cc_local_destino}'    
        elif not p_cc_acao_destino:
            v_ob_dict['OPER_DESTINO'] = 'cp'    
        elif str(p_cc_acao_destino).strip().lower() in ("cp", "del", "mv", "ren"):
            v_ob_dict['OPER_DESTINO'] = str(p_cc_acao_destino).strip().lower()
        else:
            v_nr_ret = 1
            v_ob_dict['STATUS'] = 'ERRO'
            v_ob_dict['MSG'] += f' >> A operacao destino é irregular !{p_cc_acao_destino}'    
    else:
        if str(p_cc_acao_destino).strip().lower() in ("del", "ren"):
            v_ob_dict['OPER_DESTINO'] = str(p_cc_acao_destino).strip().lower()
    v_ob_dict['OPER_REN_DESTINO'] = p_cc_rename_destino
    if not v_ob_dict['OPER_REN_DESTINO'] and v_ob_dict['OPER_DESTINO'] in ("ren"):
        v_nr_ret = 1
        v_ob_dict['STATUS'] = 'STOP'
        v_ob_dict['MSG'] += f' >> Para renomear deve ser informado! {p_cc_rename_destino}'
    
    v_ob_dict['DIR'] = p_cc_local
    if not os.path.isdir(p_cc_local):
        v_nr_ret = 1
        v_ob_dict['STATUS'] = 'STOP'
        v_ob_dict['MSG'] += f' >> O diretório {p_cc_local} nao foi possivel de ser localizado !'
    try:
        v_ob_dict['DIR_RAIZ'] = os.path.dirname(v_ob_dict['DIR'])
    except Exception:
        v_ob_dict['DIR_RAIZ'] = v_ob_dict['DIR']
    if not p_cc_local_destino or v_ob_dict['OPER_DESTINO'] in ('N/A', 'del', "ren"):
        v_ob_dict['DIR_RAIZ_DESTINO'] = v_ob_dict['DIR_RAIZ']
    else:
        v_ob_dict['DIR_RAIZ_DESTINO'] = p_cc_local_destino
        if v_ob_dict['OPER_DESTINO'] not in ('N/A', 'del', "ren"):
            if ((str(v_ob_dict['DIR_RAIZ_DESTINO']).strip().upper() + ('/' if os.name == 'posix' else '\\'))\
                == (str(v_ob_dict['DIR']).strip().upper() + ('/' if os.name == 'posix' else '\\')))\
            or ((str(v_ob_dict['DIR_RAIZ_DESTINO']).strip().upper())\
                == (str(v_ob_dict['DIR']).strip().upper() + ('/' if os.name == 'posix' else '\\')))\
            or ((str(v_ob_dict['DIR_RAIZ_DESTINO']).strip().upper() + ('/' if os.name == 'posix' else '\\'))\
                == (str(v_ob_dict['DIR']).strip().upper() ))\
            or ((str(v_ob_dict['DIR_RAIZ_DESTINO']).strip().upper())\
                == (str(v_ob_dict['DIR']).strip().upper() ))\
            :
                v_nr_ret = 1
                v_ob_dict['STATUS'] = 'STOP'
                v_ob_dict['MSG'] += f' >> O diretório {p_cc_local_destino} nao pode ser igual ao diretório de origem ! {p_cc_local}'        
        if v_nr_ret == 0 and v_ob_dict['OPER_DESTINO'] not in ('N/A','del', "ren"):
            if not os.path.isdir(v_ob_dict['DIR_RAIZ_DESTINO']):
                try:
                    os.mkdir(v_ob_dict['DIR_RAIZ_DESTINO'])
                except Exception:
                    try:
                        os.makedirs(v_ob_dict['DIR_RAIZ_DESTINO'])
                    except (Exception) as err:    
                        ds_err_trace = format_exc()
                        v_nr_ret = 1
                        v_ob_dict['STATUS'] = 'STOP'
                        v_ob_dict['MSG'] += f' >> O diretório {p_cc_local_destino} nao foi possivel de ser criado ! {ds_err_trace}  >> {str(err)}'        
            
        v_ob_dict['DIR_DESTINO'] = str(v_ob_dict['DIR']).\
                                replace(v_ob_dict['DIR_RAIZ'],\
                                v_ob_dict['DIR_RAIZ_DESTINO'] + ('/' if os.name == 'posix' else '\\'))
                            
    v_ob_lista.append(v_ob_dict)                
    v_qt = 0
    print(df(),"ORIGEM: ", p_cc_local,\
        " >> TERMO: ", p_cc_term_arquivo,\
        " >> DESTINO: ", p_cc_local_destino,\
        " >> OPERACAO: ", p_cc_acao_destino)
    try:
        if v_nr_ret == 0:
            for caminho_raiz, diretorios, arquivos in os.walk(p_cc_local):
                if str(v_ob_lista[0]['OPER_DESTINO']) not in ('N/A','del', "ren") and str(v_ob_lista[0]['DIR_RAIZ_DESTINO']):
                    if ((str(v_ob_lista[0]['DIR_DESTINO']).strip().upper() + ('/' if os.name == 'posix' else '\\'))\
                        in (str(caminho_raiz).strip().upper() + ('/' if os.name == 'posix' else '\\'))):
                        continue    
                for arquivo in arquivos:
                    v_ob_dict = {}
                    v_ob_dict['STATUS'] = 'OK'
                    v_ob_dict['MSG'] = ''
                    v_ob_dict['ARQUIVO'] = arquivo                        
                    if p_cc_term_arquivo == "*"\
                    or p_cc_term_arquivo.upper() in arquivo.upper():
                        try:
                            v_ob_dict['DIR'] = caminho_raiz                            
                            v_ob_dict['PATH'] = os.path.join(v_ob_dict['DIR'], v_ob_dict['ARQUIVO'])
                            v_ob_dict['DIR_RAIZ'] = os.path.dirname(v_ob_dict['DIR'])\
                                if os.path.dirname(v_ob_dict['DIR']) else v_ob_dict['DIR']
                            v_ob_dict['DIR_RAIZ_DESTINO'] = str(v_ob_dict['DIR']).\
                                replace(v_ob_lista[0]['DIR_RAIZ'],\
                                v_ob_lista[0]['DIR_RAIZ_DESTINO'] + ('/' if os.name == 'posix' else '\\'))
                            v_ob_dict['NOME_ARQUIVO_SEM_EXTENSAO'], v_ob_dict['EXTENSAO'] = os.path.splitext(arquivo)                            
                            v_ob_dict['SIZE'] = os.path.getsize(v_ob_dict['PATH'])
                            v_ob_dict['SIZE_TYPE'] = fnc_formata_tamanho(v_ob_dict['SIZE'])
                            v_ob_dict['OPER_DESTINO'] = v_ob_lista[0]['OPER_DESTINO']
                            v_ob_dict['OPER_REN_DESTINO'] = v_ob_lista[0]['OPER_REN_DESTINO']
                            v_ob_dict['PATH_DESTINO'] = os.path.join(v_ob_dict['DIR_RAIZ_DESTINO'], str(v_ob_dict['OPER_REN_DESTINO']) + str(v_ob_dict['ARQUIVO']))
                            if v_ob_dict['OPER_DESTINO'] not in ('N/A','del', "ren") and str(v_ob_dict['DIR_RAIZ_DESTINO']):
                                if not os.path.isdir(v_ob_dict['DIR_RAIZ_DESTINO']):
                                    try:
                                        os.mkdir(v_ob_dict['DIR_RAIZ_DESTINO'])
                                    except Exception:
                                        os.makedirs(v_ob_dict['DIR_RAIZ_DESTINO'])
                                #"cp", "del", "mv"
                                if v_ob_dict['OPER_DESTINO'] == 'cp':
                                    try:
                                        shutil.copyfile(v_ob_dict['PATH'], v_ob_dict['DIR_RAIZ_DESTINO'])
                                    except Exception:
                                        try:
                                            shutil.copy(v_ob_dict['PATH'], v_ob_dict['PATH_DESTINO'])
                                        except Exception:
                                            shutil.copy2(v_ob_dict['PATH'], v_ob_dict['PATH_DESTINO'])

                                if v_ob_dict['OPER_DESTINO'] == 'mv':
                                    shutil.move(v_ob_dict['PATH'], v_ob_dict['PATH_DESTINO'])
                            else:
                                if v_ob_dict['OPER_DESTINO']  in ('del'):
                                    os.remove(v_ob_dict['PATH'])
                                if v_ob_dict['OPER_DESTINO']  in ("ren"):
                                    os.rename(v_ob_dict['PATH'], v_ob_dict['PATH_DESTINO'])                 
                        # If source and destination are same
                        except shutil.SameFileError as ds_err:
                            v_nr_ret = 1
                            v_ob_dict['STATUS'] = 'ERRO'
                            v_ob_dict['MSG'] += " >> Problemas no diretório [Source and destination represents the same file.] >> " + str(ds_err)
                        # If destination is a directory.
                        except IsADirectoryError as ds_err:
                            v_nr_ret = 1
                            v_ob_dict['STATUS'] = 'ERRO'
                            v_ob_dict['MSG'] += " >> Problemas no diretório >> " + str(ds_err)
                        except PermissionError as ds_err:
                            v_nr_ret = 1
                            v_ob_dict['STATUS'] = 'ERRO'
                            v_ob_dict['MSG'] += " >> Sem permissao na leitura do arquivo >> " + str(ds_err)
                        except FileNotFoundError as ds_err:
                            v_nr_ret = 1
                            v_ob_dict['STATUS'] = 'ERRO'
                            v_ob_dict['MSG'] += " >> Problemas ao encontrar arquivo >> " + str(ds_err)
                        except Exception as ds_err:
                            v_nr_ret = 1
                            v_ob_dict['STATUS'] = 'ERRO'
                            v_ob_dict['MSG'] += " >> Desconhecido >> " + str(ds_err)
                            raise Exception("ERRO >> " + " Desconhecido >> " + str(caminho_raiz) + ">>" + str(arquivo)+ ">>" + str(ds_err))
                        else:
                            v_qt += 1                            
                        finally:
                            v_ob_lista.append(v_ob_dict)
        
    except (Exception) as err:
        ds_err_trace = format_exc()
        v_nr_ret = 1
        v_ob_lista[0]['STATUS'] = 'STOP'
        v_ob_lista[0]['MSG'] += " >> FALHA : " + ds_err_trace + " >> " + str(err)        

    if v_ob_lista:
        for index, dicionario in enumerate(v_ob_lista):
            for i, (k, v) in enumerate(dicionario.items()):
                print(df(), index, '.', i+1, ') ', k, ':', v)
            print()     
        try:
            print(df(), len([x['ARQUIVO'] for x in list(\
                filter(lambda x: 'OK' in x['STATUS'] , v_ob_lista)) ]),\
                 'arquivo(s) localizados ...', v_ob_lista[0]['DIR'])
            print(df(), len([x['ARQUIVO'] for x in list(\
                filter(lambda x: 'ERRO' in x['STATUS'], v_ob_lista)) ]),\
                 'arquivo(s) com erro ...', v_ob_lista[0]['DIR'])
        except:
            pass
    return v_nr_ret, v_ob_lista



def main(*args, **kwargs) -> int:
    """Funcao principal

    Returns:
        [type]: 0 - Sucesso / 1 - Erro
    """
    v_nr_ret = 0
    try:
        v_cc_rename_destino = ""
        v_cc_acao_destino = ""
        v_cc_termo = "*"
        v_cc_diretorio = ""#C:\\Users\\Kyros\\VSProjects\\UdemyPhyton3LuizOtavio
        v_cc_diretorio_destino = "" 
        for idx, parametros_entrada in enumerate(args):
            print(df(), "(", idx, ")", parametros_entrada)
            for index, valor in enumerate(parametros_entrada):
                print(df(), "(", idx, ".", index, ")", valor)  
                # if index == 0 and valor:
                #     try:
                #         if os.path.isfile(valor):
                #             v_cc_diretorio = os.path.dirname(valor)
                #     except Exception as err:
                #         v_cc_diretorio = ""
                if index == 0 and valor:
                    v_cc_diretorio = valor
                if index == 1 and valor and v_cc_diretorio:
                    v_cc_termo = valor
                if index == 2 and v_cc_diretorio and v_cc_termo:
                    if str(valor).strip().lower() in ("del","ren"):
                        v_cc_diretorio_destino = v_cc_diretorio 
                        v_cc_acao_destino = valor
                    elif not valor:
                        v_cc_diretorio_destino = v_cc_diretorio
                    else:    
                        v_cc_diretorio_destino = valor
                if index == 3 and valor and v_cc_diretorio and v_cc_termo and v_cc_diretorio_destino:
                    if not v_cc_acao_destino:
                        v_cc_acao_destino = valor
                    elif str(v_cc_acao_destino).strip().lower() in ("ren"):
                        v_cc_rename_destino = valor
                if index == 4 and valor and v_cc_diretorio and v_cc_termo and v_cc_diretorio_destino and v_cc_acao_destino:
                    if not v_cc_rename_destino:
                        v_cc_rename_destino = valor
                        
        for index, valor in kwargs:
            print(df(), "[", index, "]", valor)
        
        if v_cc_diretorio and v_cc_termo:    
            v_nr_ret, v_ob_lista = \
            fnc_procura_arquivos(p_cc_local=v_cc_diretorio,
                                 p_cc_term_arquivo=v_cc_termo,
                                 p_cc_local_destino=v_cc_diretorio_destino,
                                 p_cc_acao_destino=v_cc_acao_destino,
                                 p_cc_rename_destino=v_cc_rename_destino)        
    
        return v_nr_ret
    except (ValueError) as err:
        print(df(), '<<ERR>>', 'STOP', 'ValueError',  err)
        return 1
    except (Exception) as err:
        ds_err_trace = format_exc()
        print(df(), '<<ERR>>', 'STOP', ds_err_trace, err)
        return 1


if __name__ == "__main__":
    """Procedimentos a serem acionados
    """
    print(df(), "Start")
    v_nr_ret = main(sys.argv[1:])
    if not isinstance(v_nr_ret, int):
        v_nr_ret = 1
    print(df(), "End", v_nr_ret)
    sys.exit(v_nr_ret)
