#!/bin/bash

clear

# Variavel para tornar padrao o diretorio RAIZ, mesmo nos ambientes dos fornecedores
DIRNAME_0=`dirname $0`
DATA=`date +%Y%m%d%H%M%S`
BASENAME_0=`basename $0 | awk -F. '{print $1}'`
TIPO_ARQ="SQL"
DIR_SISTEMA=`dirname $0`
DIR_LOGS=`dirname $0`
# Define o arquivo de log
ARQ_LOG="${DIRNAME_0}/${BASENAME_0}_${DATA}.log"

#
# Programa para gravar mensagens no arquivo de LOG
#
Log ()
####################################################################################################
{
echo " $(date +'%Y-%m-%d %H:%M:%S')-> $1" | tee -a ${ARQ_LOG}
}

#
# Funcao para gravar de inicio de processamento
#
fc_inicio()
{
Log "**************************************************************************************************************************"
Log " Inicio de Processamento - ${BASENAME_0}.sh"
Log " Diretorio principal: ${DIR_SISTEMA}"
Log "**************************************************************************************************************************"
Log " "
}

#
# Funcao para sair do shell script
#
fc_saida()
{

COD_ERRO=$1

Log " "
Log "**************************************************************************************************************************"
Log " Fim de Processamento - ${BASENAME_0}.sh - RC: ${COD_ERRO} "
Log "**************************************************************************************************************************"
Log " "

exit ${COD_ERRO}
}

#
# Verificacao de ocorrencia de erro durante a execucao do comando no banco de dados
#
fc_verifica_execucao_sql()
{
if [ ! -r $1 ]
then
   Log " ERRO: Nao existe LOG de comando SQL para ser verificado"
   return 102
fi
if test $(grep -c "CONEXAO BANCO DE DADOS - OK" $1) -ne 0
then
   Log " Conexao com Banco de Dados realizada com sucesso"
   return 0
fi
if test $(grep -c "invalid username/password" $1) -ne 0
then
   Log " ERRO: PCP favor verificar usuario / senha de conexao de banco de dados"
   return 201
fi
if test $(grep -c "no listener " $1) -ne 0
then
   Log " ERRO: PCP verificar se o BD esta ativo - direcionar para equipe de DBAs"
   return 202
fi
if test $(grep -c "ORA-20999" $1) -ne 0
then
   Log " ERRO: enviar LOG para equipe de DESENVOLVIMENTO"
   return 901
fi
if test $(grep -c "SQL\*Loader-" $1) -ne 0
then
   Log " ERRO: na execucao do comando SQLLDR. Favor acionar o Analista da equipe de DESENVOLVIMENTO"
   return 203
fi
if test $(grep -c "ORA-" $1) -ne 0
then
   Log " ERRO no comando de Banco de Dados (nao catalogado). Favor acionar o Analista da equipe de DESENVOLVIMENTO"
   return 203
fi
Log " Verificacao de LOG SQL nao encontrou erros no arquivo: $1"
return 0
}

#
# Teste de Conexao - Banco de Dados
#
fc_test_conn_DB()
{
sleep 1
DATA_LOG_SQL=`date +%Y%m%d%H%M%S`
ARQ_LOG_SQL="${DIR_LOGS}/${BASENAME_0}_${TIPO_ARQ}_${DATA_LOG_SQL}_sql.log"

sqlplus -silent << FIM >> $ARQ_LOG_SQL
${BD_CONN}

set serveroutput on size 1000000
declare
   --
   v_erro varchar2(200);
   --
begin
   --
   select 'CONEXAO BANCO DE DADOS - OK'
     into  v_erro
     from  dual;
   dbms_output.put_line(v_erro);
   --
end;
/
FIM

# Verifica erro na execucao do comando sql
fc_verifica_execucao_sql $ARQ_LOG_SQL
CD_ERRO=$?

if [ ${CD_ERRO} -ne 0 ]
then
   fc_saida ${CD_ERRO}
fi
}


#
# Executa como nohup
#
fc_execute_nohup()
{
ARQ_EXE=$1
# Verifica se parametro de entrada foi passado
if [ $# -lt 1 ]
then
	Log " "
	Log " ERRO - Parametros nao foi informado corretamente ! ${ARQ_EXE}"
	Log " "
	fc_saida 101
fi	
sleep 1
DATA_LOG_SQL=`date +%Y%m%d%H%M%S`
ARQ_LOG_SQL="${DIR_LOGS}/${BASENAME_0}_${TIPO_ARQ}_${DATA_LOG_SQL}_sql.log"
ARQ_LOG_ERR="${DIR_LOGS}/${BASENAME_0}_${TIPO_ARQ}_${DATA_LOG_SQL}_err.err"
nohup sqlplus ${BD_CONN} @${ARQ_EXE} > ${ARQ_LOG_SQL} 2> ${ARQ_LOG_ERR} &
CD_ERRO=$?
if [ ${CD_ERRO} -ne 0 ]
then
	Log " "
	Log " ERRO durante acionamento SQL ! ${ARQ_EXE}"
	Log " "
	fc_saida 101
fi
sleep 10
# Verifica erro na execucao do comando sql
fc_verifica_execucao_sql $ARQ_LOG_SQL
CD_ERRO=$?

if [ ${CD_ERRO} -ne 0 ]
then
   fc_saida ${CD_ERRO}
fi
}

#
# Executa normalmente
#
fc_execute()
{
ARQ_EXE=$1
# Verifica se parametro de entrada foi passado
if [ $# -lt 1 ]
then
	Log " "
	Log " ERRO - Parametros nao foi informado corretamente ! ${ARQ_EXE}"
	Log " "
	fc_saida 101
fi	
sleep 1
DATA_LOG_SQL=`date +%Y%m%d%H%M%S`
ARQ_LOG_SQL="${DIR_LOGS}/${BASENAME_0}_${TIPO_ARQ}_${DATA_LOG_SQL}_sql.log"
ARQ_LOG_ERR="${DIR_LOGS}/${BASENAME_0}_${TIPO_ARQ}_${DATA_LOG_SQL}_err.err"
sqlplus -silent << FIM >> $ARQ_LOG_SQL
${BD_CONN}

@$ARQ_EXE
FIM
# Verifica erro na execucao do comando sql
fc_verifica_execucao_sql $ARQ_LOG_SQL
CD_ERRO=$?

if [ ${CD_ERRO} -ne 0 ]
then
   fc_saida ${CD_ERRO}
fi
}


# Deve ser passado pelo menos um parametro
export ARQUIVO_EXECUTA_SQL=$(echo $1 | tr '[:upper:]' '[:upper:]')
export CONEXAO_BASE_DADOS=${2:-CONEXAO} 
export TIPO_ACIONAMENTO=${3:-NOHUP} 

# Verifica se parametro de entrada foi passado
if [ $# -lt 1 ]
then
	Log " "
	Log " ERRO - Parametros nao foi informado corretamente ! ${ARQUIVO_EXECUTA_SQL} >> ${CONEXAO_BASE_DADOS}"
	Log " "
	fc_saida 101
else
	if [ -e $ARQUIVO_EXECUTA_SQL ]
	then
		Log " "		
		Log " ${ARQUIVO_EXECUTA_SQL} eh valido!"
		Log " "
	else
		Log " "		
		Log " ${ARQUIVO_EXECUTA_SQL} nao eh valido!"
		Log " "
		fc_saida 101
	fi
	case ${CONEXAO_BASE_DADOS} in
		"GFCLONE7"|"GF_CLONE_DEV7"|"GFPRODC7"|"7"|"10.238.45.228"|"svc_gfprodc7"|"Clone 7"|"Clone7"|"CLONE 7"|"CLONE7")
			export BD_CONN="gfcadastro/vivo2019@10.238.45.230/svc_gfprodc7"
			export STRING_HOST="10.238.10.109"
			export NRO_BASE="7"
		;;
		"GFCLONE6"|"GF_CLONE_DEV6"|"GFPRODC6"|"6"|"10.238.45.227"|"svc_gfprodc6"|"Clone 6"|"Clone6"|"CLONE 6"|"CLONE6")
			export BD_CONN="gfcadastro/vivo2019@10.238.45.230/svc_gfprodc6"
			export STRING_HOST="10.238.10.210"
			export NRO_BASE="6"
		;;
		"GFREAD"|"GF_CLONE_GFREAD"|"READ")
			export BD_CONN="GFREAD/vivo2019@10.238.10.173/gfread"
			export STRING_HOST="10.238.10.174"
			export NRO_BASE="5"
		;;	
		"GFREAD_OPENRISOW"|"GF_CLONE_GFREAD_OPENRISOW"|"READ_OPENRISOW")
			export BD_CONN="OPENRISOW/OPENRISOW@10.238.10.173/gfread"
			export STRING_HOST="10.238.10.174"
			export NRO_BASE="5"
		;;
		"GFCLONE5"|"GF_CLONE_DEV5"|"DEV5"|"5"|"10.238.10.173"|"Clone 5"|"Clone5"|"CLONE 5"|"CLONE5")
			export BD_CONN="gfcadastro/vivo2019@10.238.10.173/gfprod"
			export STRING_HOST="10.238.10.174"
			export NRO_BASE="5"
		;;
		"GF_CLONE_DEV2"|"DEV2"|"2"|"10.238.10.207"|"GFCLONEPREPROD"|"Clone 2"|"Clone2"|"CLONE 2"|"CLONE2")
			export BD_CONN="gfcadastro/vivo2019@10.238.10.207/gfprod"
			export STRING_HOST="10.238.10.209"
			export NRO_BASE="2"
		;;
		"GF_CLONE_DEV"|"DEV"|"1"|"10.238.10.106"|"GFCLONEDEV"|"Clone 1"|"Clone1"|"CLONE 1"|"CLONE1")
			export BD_CONN="gfcadastro/vivo2019@10.238.10.106/gfprod"
			export STRING_HOST="10.238.10.208"
			export NRO_BASE="1"
		;;
		*)
			Log " "		
			Log " ${CONEXAO_BASE_DADOS} nao eh valido!"
			Log " "
			fc_saida 101			
		;;
	esac
	Log " "
	Log " String de Conexao: ${BD_CONN}"
	Log " Host: ${STRING_HOST}"
	Log " Number: ${NRO_BASE}"
	Log " "
	fc_test_conn_DB
	Log " "
fi
# Grava log de inicio de execucao
fc_inicio

Log " "
Log " Processando ${TIPO_ARQ} : ${ARQUIVO_EXECUTA_SQL} >> ${CONEXAO_BASE_DADOS}"
Log " "
if [ "${TIPO_ACIONAMENTO}" != "NOHUP" ]; then
	fc_execute $ARQUIVO_EXECUTA_SQL
else
	fc_execute_nohup $ARQUIVO_EXECUTA_SQL
fi	
fc_saida 0

