#!/bin/bash
PARTICAO_NF=${1}
PARTICAO_INF=${2}
ROWID_CP=${3}
export ST_PROCESSAMENTO="Processado"
export DS_MSG_ERRO=""
sqlplus -S /nolog <<@EOF >> ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.log 2>> ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.err
CONNECT ${STRING_CONEXAO}
set define off;
SET SERVEROUTPUT ON SIZE 1000000;
set timing on;
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
ROLLBACK;
UPDATE ${TABELA_CONTROLE} cp
 SET cp.st_processamento = 'Em Processamento',
     cp.dt_ini_proc = SYSDATE
WHERE cp.rowid = '${ROWID_CP}'
 AND cp.st_processamento IN ('Aguardando','Reprocessar')
 AND cp.nm_processo = '${PROCESSO}';
COMMIT;
@EOF
RETORNO=$?
echo " INICIO >> RETORNO=${RETORNO} "
while IFS='=' read key value; do
  echo "key = '$key'; value = '$value'"
  if [ "${key}" != "" ]; then  
	#export $key=$value
	if [ "${value}" == "TRUE" ]; then    
		if [ ${RETORNO} -eq 0 ]; then	
			DS_MSG_ERRO=" ${key}=${value} >> ./${key}.sh >> ${RETORNO} >> ${PARTICAO_NF} >> ${PARTICAO_INF} >> ${ROWID_CP}"
			#if [ -e "${key}.sh"]; then
				echo "${DS_MSG_ERRO}"		
				export SPOOL_FILE=./${key}_${PARTICAO_NF}_${PROCESSO}.spool
				echo "${SPOOL_FILE} >> ./${key}.sh ${PARTICAO_NF} ${PARTICAO_INF} ${ROWID_CP}"
				./${key}.sh ${PARTICAO_NF} ${PARTICAO_INF} ${ROWID_CP}
				RETORNO=$?
			#fi
		fi
	fi
  fi	
done < <(sed 's/\s\+:\s\+/:/' ./PARAMETRO.txt)


if [ ${RETORNO} -ne 0 ]; then
    MSG_ERRO=$( grep -e 'ORA-' -e 'PLS-' -e 'ERROR' ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.log )
	MSG_ERRO_SPOOL=$( grep -e 'ORA-' -e 'PLS-' -e 'ERROR' ${SPOOL_FILE})
	ST_PROCESSAMENTO="Erro"
	DS_MSG_ERRO=" ${ST_PROCESSAMENTO} >> ${RETORNO} >> ${MSG_ERRO} >>  ${DS_MSG_ERRO} >>  ${MSG_ERRO_SPOOL}  "
	echo "${DS_MSG_ERRO} "
	if [ ${RETORNO} -eq 2 ]; then
	    DS_MSG_ERRO=" STOP >> ${DS_MSG_ERRO} "
	    printf " ${DS_MSG_ERRO} "
		  > ./.${SCRIPT}_${PROCESSO}.stop
		
	fi
fi

sqlplus -S /nolog <<@EOF >> ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.log 2>> ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.err
CONNECT ${STRING_CONEXAO}
set define off;
SET SERVEROUTPUT ON SIZE 1000000;
set timing on;
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
ROLLBACK;
UPDATE ${TABELA_CONTROLE} cp
   SET cp.st_processamento = '${ST_PROCESSAMENTO}',
       cp.dt_fim_proc = SYSDATE,
	   cp.ds_msg_erro  = substr(substr(nvl('${DS_MSG_ERRO}',' '),1,500) || ' <<||>> ' || substr(cp.ds_msg_erro,1,3400) ,1,4000)
 WHERE cp.rowid = '${ROWID_CP}';
COMMIT;
@EOF
RETORNO=$?
echo " FINAL >> RETORNO=${RETORNO} "

${WAIT}

exit ${RETORNO}
