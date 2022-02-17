#!/bin/bash
export PROCESSO="${1:-NOME_PROCESSO_99999999}" 
export DATA_INICIO="${2:-01/01/2049}"
export DATA_FIM="${3:-02/01/2049}"
export FILTRO="${4:-1=1}" # Filtro de dados SQL.  Exemplo nf.emps_cod= 'TBRA' AND nf.fili_cod = '0001'
export BASE="${5:-Clone 6}" # Base de dados
export COMMIT="${6:-ROLLBACK}" # Transacao COMMIT ou ROLLBACK
export REGRAS_HISTORIAS="${7:-}" # Regras de execucao. Exemplo: R2015_1,R2015_2,R2015_4,R2015_7,R2015_9,R2015_19,R2015_14,R2015_34,CFOP_0000,R2015_6, ERRO_122,R2015_31
export STATUS_PROCESSO="${8:-}" # Status para reprocessamento: Erro,Aguardando,Reprocessar,Em Processamento
export TABELA_NF="${9:-MESTRE_NFTL_SERV}"
export TABELA_INF="${10:-ITEM_NFTL_SERV}"
export SCRIPT="sanea_particao"
export TABELA_CONTROLE="gfcadastro.CONTROLE_PROCESSAMENTO"
export FIND="'"
export REPLACE="''"
export SCRIPT_REGRA_BEFORE_01=""
export SCRIPT_REGRA_BEFORE_02=""
echo ${PROCESSO}
echo ${DATA_INICIO} ${DATA_FIM} 
echo ${FILTRO} 
echo ${BASE} 
echo ${COMMIT} 
echo ${REGRAS_HISTORIAS}
echo ${STATUS_PROCESSO}
echo ${TABELA_NF}
echo ${TABELA_INF}
echo ${SCRIPT}
echo ${TABELA_CONTROLE}
echo ${FIND}
echo ${REPLACE}
echo ${SCRIPT_REGRA_BEFORE_01}
echo ${SCRIPT_REGRA_BEFORE_02}
echo "Inicio do processamentos ... "
if [ -f ./.alive.pid ]; then
  PID_PROCESSO=$( cat ./.alive.pid )
  kill -0 ${PID_PROCESSO} 2> /dev/null
  if [ $? -eq 0 ]; then
     printf "Processo ja em execucao com  PID%s\n\n" {$PID_PROCESSO}
     # exit 1
  fi
  rm -f ./.alive.pid
fi
echo $$ > ./.alive.pid

case ${BASE} in
    "GFCLONE7"|"GF_CLONE_DEV7"|"GFPRODC7"|"7"|"10.238.45.228"|"svc_gfprodc7"|"Clone 7"|"Clone7"|"CLONE 7"|"CLONE7")
        export STRING_CONEXAO="gfcadastro/vivo2019@10.238.45.230/svc_gfprodc7"
		export STRING_HOST="10.238.10.109"
		export NRO_BASE="7"
    ;;
    "GFCLONE6"|"GF_CLONE_DEV6"|"GFPRODC6"|"6"|"10.238.45.227"|"svc_gfprodc6"|"Clone 6"|"Clone6"|"CLONE 6"|"CLONE6")
        export STRING_CONEXAO="gfcadastro/vivo2019@10.238.45.230/svc_gfprodc6"
		export STRING_HOST="10.238.10.210"
		export NRO_BASE="6"
    ;;
    "GFREAD"|"GF_CLONE_GFREAD"|"READ")
        export STRING_CONEXAO="GFREAD/vivo2019@10.238.10.173/gfread"
		export STRING_HOST="10.238.10.174"
		export NRO_BASE="5"
	;;	
    "GFREAD_OPENRISOW"|"GF_CLONE_GFREAD_OPENRISOW"|"READ_OPENRISOW")
        export STRING_CONEXAO="OPENRISOW/OPENRISOW@10.238.10.173/gfread"
		export STRING_HOST="10.238.10.174"
		export NRO_BASE="5"
    ;;
    "GFCLONE5"|"GF_CLONE_DEV5"|"DEV5"|"5"|"10.238.10.173"|"Clone 5"|"Clone5"|"CLONE 5"|"CLONE5")
        export STRING_CONEXAO="gfcadastro/vivo2019@10.238.10.173/gfprod"
		export STRING_HOST="10.238.10.174"
		export NRO_BASE="5"
    ;;
    "GF_CLONE_DEV2"|"DEV2"|"2"|"10.238.10.207"|"GFCLONEPREPROD"|"Clone 2"|"Clone2"|"CLONE 2"|"CLONE2")
        export STRING_CONEXAO="gfcadastro/vivo2019@10.238.10.207/gfprod"
		export STRING_HOST="10.238.10.209"
		export NRO_BASE="2"
    ;;
    "GF_CLONE_DEV"|"DEV"|"1"|"10.238.10.106"|"GFCLONEDEV"|"Clone 1"|"Clone1"|"CLONE 1"|"CLONE1")
        export STRING_CONEXAO="gfcadastro/vivo2019@10.238.10.106/gfprod"
		export STRING_HOST="10.238.10.208"
		export NRO_BASE="1"
    ;;
    *)
        export STRING_CONEXAO="gfcadastro/vivo2019@${BASE}/gfprod"
		export STRING_HOST="${BASE}"
		export NRO_BASE="0"
    ;;
esac
echo ${STRING_CONEXAO}
echo ${STRING_HOST}
echo ${NRO_BASE}

export SEQUENCE_CONTROLE=$( 
sqlplus -S -m 'csv on delimiter ; QUOTE OFF' /nolog <<@EOF
CONNECT ${STRING_CONEXAO}
SET SERVEROUTPUT ON SIZE 1000000;
set heading off
set feedback off
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
select GFCADASTRO.tcsq_kyros_process_log.nextval as sq_kyros_process_log from dual; 
@EOF
exit $?)
RETORNO=$?
echo ${RETORNO}
if [ ${RETORNO} -ne 0 ]; then
    printf "Execucao interrompida!\nFavor olhar a sequence : gfcadastro.CONTROLE_PROCESSAMENTO!\n"
	printf "${SEQUENCE_CONTROLE}!\n"
    exit 1	
elif [ ${SEQUENCE_CONTROLE} -eq 0 ]; then
	printf "ATENCAO: Favor olhar : ${SEQUENCE_CONTROLE}!\n"
	exit 1
fi

echo ${SEQUENCE_CONTROLE}
export SPOOL_FILE=./${SCRIPT}_CONTROLE_PROCESSAMENTO_${PROCESSO}.spool
sqlplus -S -m 'csv on delimiter ; QUOTE OFF' /nolog <<@EOF
CONNECT ${STRING_CONEXAO}
SET define OFF;
SET serveroutput ON size 1000000;
SET timing ON;
SPOOL  ${SPOOL_FILE} 
var exit_code NUMBER = 0
whenever oserror EXIT 1;
whenever sqlerror EXIT 2;
BEGIN

	IF NVL(LENGTH(TRIM('${STATUS_PROCESSO}')),0) > 0  
	THEN
	
		UPDATE gfcadastro.CONTROLE_PROCESSAMENTO 
		SET    ST_PROCESSAMENTO                = 'Reprocessar' 
			,  ID_PROCESS                      = ${SEQUENCE_CONTROLE} 
			,  DT_INI_PROC                     = NULL
			,  DT_FIM_PROC                     = NULL
		    ,  QT_ATUALIZADOS_CLI              = 0
		    ,  QT_ATUALIZADOS_COMP             = 0
		    ,  QT_ATUALIZADOS_NF               = 0
		    ,  QT_ATUALIZADOS_INF              = 0
			,  QT_CAD_NAO_ENCONTRADO           = 0
		    ,  DS_MSG_ERRO                     = NULL 
			,  DS_FILTRO                       = SUBSTR('${FILTRO//$FIND/$REPLACE}',1,4000)
			,  DS_TRANSACAO                    = SUBSTR('${COMMIT//$FIND/$REPLACE}',1,100)
			,  DS_REGRAS                       = SUBSTR('${REGRAS_HISTORIAS//$FIND/$REPLACE}',1,4000)
			,  DS_OUTROS_PARAMETROS            = SUBSTR('STATUS_PROCESSO:${STATUS_PROCESSO//$FIND/$REPLACE}|TABELA_NF:${TABELA_NF//$FIND/$REPLACE}|TABELA_INF:${TABELA_INF//$FIND/$REPLACE}',1,4000)
		WHERE  DT_LIMITE_INF_NF BETWEEN TO_DATE('${DATA_INICIO}','DD/MM/YYYY') AND TO_DATE('${DATA_FIM}','DD/MM/YYYY') 
		AND    UPPER(TRIM(NM_PROCESSO)) = UPPER(TRIM('${PROCESSO}'))
		AND    UPPER(TRIM(TRANSLATE('${STATUS_PROCESSO}','^ ','^'))) LIKE '%' || UPPER(TRIM(TRANSLATE(ST_PROCESSAMENTO,'^ ','^'))) || '%';
	
	ELSE
	
		DELETE FROM gfcadastro.CONTROLE_PROCESSAMENTO 
		WHERE dt_limite_inf_nf BETWEEN TO_DATE('${DATA_INICIO}','DD/MM/YYYY') AND TO_DATE('${DATA_FIM}','DD/MM/YYYY') 
		AND UPPER(TRIM(NM_PROCESSO)) = UPPER(TRIM('${PROCESSO}'));
	
	END IF;

	INSERT INTO gfcadastro.CONTROLE_PROCESSAMENTO
		(
		  NM_PROCESSO,
		  NM_PARTICAO_NF ,
		  DT_LIMITE_INF_NF ,
		  DT_LIMITE_SUP_NF ,
		  QT_REGISTROS_NF ,
		  NM_PARTICAO_INF ,
		  DT_LIMITE_INF_INF ,
		  DT_LIMITE_SUP_INF ,
		  QT_REGISTROS_INF ,
		  ST_PROCESSAMENTO,
		  DT_INI_PROC,
		  DT_FIM_PROC,
		  QT_ATUALIZADOS_CLI,
		  QT_ATUALIZADOS_COMP,
		  QT_ATUALIZADOS_NF,
		  QT_ATUALIZADOS_INF,
		  DS_MSG_ERRO,
		  DS_FILTRO,
		  DS_TRANSACAO,
		  DS_REGRAS,
		  DS_OUTROS_PARAMETROS,
		  ID_PROCESS
		)
WITH partition_data AS
  (SELECT TRIM(upper(tmp_xml.base_data)) base_data ,
    TRIM(upper(tmp_xml.table_owner)) table_owner ,
    TRIM(upper(tmp_xml.table_name)) table_name ,
    to_date(SUBSTR(TRIM(regexp_replace(tmp_xml.high_value, '[a-zA-Z|_|( |)|'']')), 1, 10), 'YYYY-MM-DD') - 1 date_partition_data_value ,
    tmp_xml.partition_name
  FROM
    (SELECT dbms_xmlgen.getxmltype(q'[SELECT 'CURRENT' base_data, p.table_owner, p.table_name, p.high_value, p.partition_name  FROM   all_part_key_columns k,all_tab_cols c, all_tab_partitions p WHERE  k.owner = c.owner  AND    k.column_name = c.column_name   AND    k.name = c.table_name  AND    k.owner = p.table_owner AND    k.name = p.table_name AND    (c.data_type = 'DATE' or c.data_type like 'TIMESTAMP%') AND TRIM(upper(k.owner)) = 'OPENRISOW' AND TRIM(upper(k.name)) IN ('${TABELA_INF}','${TABELA_NF}')]' ) AS xml
    FROM dual
    UNION ALL
    SELECT dbms_xmlgen.getxmltype(q'[SELECT  null base_data, null as table_owner, null as table_name, null as high_value,null as partition_name  FROM   dual]' ) AS xml
    FROM dual
      -- UNION ALL
      -- SELECT dbms_xmlgen.getxmltype(q'[SELECT  /*+ DRIVING_SITE(p1) OTHER */ 'OTHER' base_data, p.table_owner, p.table_name, p.high_value, p.partition_name
      -- FROM   all_part_key_columns@gfread k, all_tab_cols@gfread c, all_tab_partitions@gfread p , dual p1
      -- WHERE  k.owner = c.owner AND    k.column_name = c.column_name AND    k.name = c.table_name AND    k.owner = p.table_owner AND    k.name = p.table_name AND    (c.data_type = 'DATE' or c.data_type like 'TIMESTAMP%')
      -- AND TRIM(upper(k.owner)) = 'OPENRISOW' AND TRIM(upper(k.name)) IN ('${TABELA_INF}','${TABELA_NF}')
      -- ]') AS xml
      --            FROM  dual
    ) tmp,
    XMLTABLE ( '/ROWSET/ROW' PASSING tmp.xml COLUMNS base_data VARCHAR2(30) PATH '/ROW/BASE_DATA', table_owner VARCHAR2(30) PATH '/ROW/TABLE_OWNER', table_name VARCHAR2(30) PATH '/ROW/TABLE_NAME', high_value VARCHAR2(30) PATH '/ROW/HIGH_VALUE', partition_name VARCHAR2(30) PATH '/ROW/PARTITION_NAME' ) tmp_xml
  WHERE ( LENGTH(TRIM(regexp_replace(tmp_xml.high_value, '[a-zA-Z|_|( |)|'']'))) = 18
  OR LENGTH(TRIM(regexp_replace(tmp_xml.high_value, '[a-zA-Z|_|( |)|'']')))      = 10 )
  ) ,
  tmp_data AS
  (SELECT A.base_data,
    TRUNC(A.date_partition_data_value) dt_limite_inf_nf,
    TO_CHAR(A.date_partition_data_value, 'YYYY-MM-DD') date_partition_data_value,
    A.partition_name,
    A.table_name
  FROM partition_data A
  WHERE A.base_data               IS NOT NULL
  AND A.date_partition_data_value >= TO_DATE('${DATA_INICIO}','DD/MM/YYYY')
  AND A.date_partition_data_value  < TO_DATE('${DATA_FIM}','DD/MM/YYYY')+1
  ) ,
  tmp_pivot1 AS
  (SELECT                                                          *
  FROM tmp_data PIVOT ( MAX ( partition_name ) FOR ( table_name ) IN ( '${TABELA_NF}' nm_particao_nf, '${TABELA_INF}' nm_particao_inf ) )
  ORDER BY dt_limite_inf_nf --,    base_data;
  ) ,
  tmp_pivot2 AS
  (SELECT                                                                                                             *
  FROM tmp_pivot1 PIVOT ( MAX ( nm_particao_nf ) particao_nf, MAX ( nm_particao_inf ) particao_inf FOR ( base_data ) IN ( 'OTHER' other, 'CURRENT' actual ) )
  WHERE other_particao_nf                                                                                            IS NULL
  ORDER BY dt_limite_inf_nf
    -- DT_LIMITE_INF_NF, OTHER_PARTICAO_NF, OTHER_PARTICAO_INF, ACTUAL_PARTICAO_NF, ACTUAL_PARTICAO_INF
  ),
  datas AS
  (SELECT TO_DATE('${DATA_INICIO}','DD/MM/YYYY')+(ROWNUM-1) DATA
  FROM DUAL
    CONNECT BY level <= (TO_DATE('${DATA_FIM}','DD/MM/YYYY')-TO_DATE('${DATA_INICIO}','DD/MM/YYYY'))+1
  ORDER BY 1
  )
SELECT DISTINCT UPPER(TRIM('${PROCESSO}')),
		A.ACTUAL_PARTICAO_NF AS NM_PARTICAO_NF ,
		B.DATA AS DT_LIMITE_INF_NF ,
		B.DATA AS DT_LIMITE_SUP_NF ,
		1 AS QT_REGISTROS_NF ,
		A.ACTUAL_PARTICAO_INF AS NM_PARTICAO_INF ,
		B.DATA AS DT_LIMITE_INF_INF ,
		B.DATA AS DT_LIMITE_SUP_INF ,
		1 AS ,
		'Aguardando'  AS ST_PROCESSAMENTO,
		NULL          AS DT_INI_PROC,
		NULL          AS DT_FIM_PROC,
		0             AS QT_ATUALIZADOS_CLI,
		0             AS QT_ATUALIZADOS_COMP,
		0             AS QT_ATUALIZADOS_NF,
		0             AS QT_ATUALIZADOS_INF,
		NULL          AS DS_MSG_ERRO,
		SUBSTR('${FILTRO//$FIND/$REPLACE}',1,4000)                AS DS_FILTRO,
		SUBSTR('${COMMIT//$FIND/$REPLACE}',1,100)                 AS DS_TRANSACAO,
		SUBSTR('${REGRAS_HISTORIAS//$FIND/$REPLACE}',1,4000)      AS DS_REGRAS,
		SUBSTR('STATUS_PROCESSO:${STATUS_PROCESSO//$FIND/$REPLACE}|TABELA_NF:${TABELA_NF//$FIND/$REPLACE}|TABELA_INF:${TABELA_INF//$FIND/$REPLACE}',1,4000) AS DS_OUTROS_PARAMETROS
	  , ${SEQUENCE_CONTROLE} AS SEQUENCE_CONTROLE
FROM datas B
LEFT JOIN tmp_pivot2 A
ON (B.DATA                 = A.DT_LIMITE_INF_NF
AND (A.ACTUAL_PARTICAO_NF IS NOT NULL
AND A.ACTUAL_PARTICAO_INF IS NOT NULL))
WHERE NOT EXISTS (SELECT 1 
FROM gfcadastro.CONTROLE_PROCESSAMENTO  B1 
WHERE B1.NM_PROCESSO = '${PROCESSO}' 
AND B1.DT_LIMITE_INF_NF = B.DATA )
ORDER BY B.DATA
;
		
  INSERT INTO GFCADASTRO.TCTB_KYROS_PROCESS (
      ID_IDENTIFY         
	, CC_PROCESS       	  
    , DT_VAR01            
    , DT_VAR02
    , DS_VAR01              
    , DS_VAR02              
    , DS_VAR03              
    , DS_VAR04)
  VALUES (${SEQUENCE_CONTROLE} 
	   , UPPER(TRIM(Q'[${PROCESSO}]'))
	   , TO_DATE('${DATA_INICIO}','DD/MM/YYYY')
	   , TO_DATE('${DATA_FIM}','DD/MM/YYYY')
	   , SUBSTR('${BASE//$FIND/$REPLACE}',1,4000)
	   , SUBSTR('${FILTRO//$FIND/$REPLACE}',1,4000) 
	   , SUBSTR('${REGRAS_HISTORIAS//$FIND/$REPLACE}',1,4000)
	   , SUBSTR('${COMMIT//$FIND/$REPLACE}',1,100)
	   );

  COMMIT;
  
EXCEPTION
WHEN OTHERS THEN
  ROLLBACK;
  :exit_code := 1;
END;
/
@EOF
RETORNO=$?
if [ ${RETORNO} -ne 0 ]; then
        printf "Execucao interrompida!\nFavor olhar tabela de controle: gfcadastro.CONTROLE_PROCESSAMENTO!\n"
        exit 1
fi

if [ "${SCRIPT_REGRA_BEFORE_01}" != "" ]; then
    printf "Execucao !\n : ${SCRIPT_REGRA_BEFORE_01}!\n"
	export SPOOL_FILE=./SCRIPT_REGRA_BEFORE_01_${PROCESSO}.spool
	./${SCRIPT_REGRA_BEFORE_01}.sh > SCRIPT_REGRA_BEFORE_01_${PROCESSO}.log 2> SCRIPT_REGRA_BEFORE_01_${PROCESSO}.err
	RETORNO=$?
	if [ ${RETORNO} -ne 0 ]; then
		printf "Execucao interrompida!\n: ${SCRIPT_REGRA_BEFORE_01}!\n"
		exit 1
	fi
fi

if [ "${SCRIPT_REGRA_BEFORE_02}" != "" ]; then
    printf "Execucao !\n : ${SCRIPT_REGRA_BEFORE_02}!\n"
	export SPOOL_FILE=./SCRIPT_REGRA_BEFORE_02_${PROCESSO}.spool
	./${SCRIPT_REGRA_BEFORE_02}.sh > SCRIPT_REGRA_BEFORE_02_${PROCESSO}.log 2> SCRIPT_REGRA_BEFORE_02_${PROCESSO}.err
	RETORNO=$?
	if [ ${RETORNO} -ne 0 ]; then
		printf "Execucao interrompida!\n: ${SCRIPT_REGRA_BEFORE_02}!\n"
		exit 1
	fi
fi


export MAX_PROCESSOS=$( cat PARAMETRO.CONF )
echo ${MAX_PROCESSOS}
PARTICOES=$( ./pega_particoes.sh ${MAX_PROCESSOS} )
RETORNO=$?
echo ${RETORNO}
echo ${PARTICOES}  
if [ ${RETORNO} -ne 0 ]; then
    printf "Execucao interrompida!\nFavor olhar tabela de controle e particao : gfcadastro.CONTROLE_PROCESSAMENTO!\n"
    exit 1
fi

while [ -n "$PARTICOES" ]; do
  for PARTICAO in ${PARTICOES}; do
    ROWID_CTL=$( echo ${PARTICAO} | cut -d";" -f1 )
    PARTICAO_NF=$( echo ${PARTICAO} | cut -d";" -f2 )
    PARTICAO_INF=$( echo ${PARTICAO} | cut -d";" -f3 )
    echo "ROWID=${ROWID_CTL} NF=$PARTICAO_NF INF=$PARTICAO_INF"
	export SPOOL_FILE=./${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.spool
	echo "./${SCRIPT}.sh ${PARTICAO_NF} ${PARTICAO_INF} ${ROWID_CTL} > ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.log 2> ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.err &"
	./${SCRIPT}.sh ${PARTICAO_NF} ${PARTICAO_INF} ${ROWID_CTL} > ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.log 2> ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.err &
  done
  sleep 5
  QTD_PROCESSOS=$(( ${MAX_PROCESSOS} - $( jobs | wc -l )))
  while [ ${QTD_PROCESSOS} -eq 0 ]; do
     sleep 5
     export MAX_PROCESSOS=$( cat PARAMETRO.CONF )
     QTD_PROCESSOS=$(( ${MAX_PROCESSOS} - $( jobs | wc -l )))
  done
  sleep 15
  if [ -f .${SCRIPT}_${PROCESSO}.stop ]; then
     printf "Execucao interrompida por erro fatal!\nFavor olhar tabela de controle: gfcadastro.CONTROLE_PROCESSAMENTO!\n"
     wait
     rm -f .${SCRIPT}_${PROCESSO}.stop
     exit 1
  fi    
  PARTICOES=$( ./pega_particoes.sh ${QTD_PROCESSOS} )
done
wait
rm -f ./.alive.pid
echo "Fim dos processamentos ... "
mkdir -p ${DIRETORIO_LOG} 2> /dev/null
mv -f *${PROCESSO}.log ${DIRETORIO_LOG} 2> /dev/null
mv -f *${PROCESSO}.err ${DIRETORIO_LOG} 2> /dev/null
mv -f *${PROCESSO}.spool ${DIRETORIO_LOG} 2> /dev/null
mv -f ${DIRETORIO_PRINCIPAL}*${PROCESSO}.spool ${DIRETORIO_LOG} 2> /dev/null
mv -f ${DIRETORIO_PRINCIPAL}*${PROCESSO}.err ${DIRETORIO_LOG} 2> /dev/null
mv -f ${DIRETORIO_PRINCIPAL}*${PROCESSO}.log ${DIRETORIO_LOG} 2> /dev/null
mv -f "185/MAP_2_REGRA_UNIFICADO/"*${PROCESSO}.spool ${DIRETORIO_LOG} 2> /dev/null
mv -f "185/MAP_2_REGRA_UNIFICADO/"${PROCESSO}.err ${DIRETORIO_LOG} 2> /dev/null
mv -f "185/MAP_2_REGRA_UNIFICADO/"${PROCESSO}.log ${DIRETORIO_LOG} 2> /dev/null
mv -f "185/MAP_2_REGRA_ISOLADAS/"*${PROCESSO}.spool ${DIRETORIO_LOG} 2> /dev/null
mv -f "185/MAP_2_REGRA_ISOLADAS/"${PROCESSO}.err ${DIRETORIO_LOG} 2> /dev/null
mv -f "185/MAP_2_REGRA_ISOLADAS/"${PROCESSO}.log ${DIRETORIO_LOG} 2> /dev/null
exit 0

