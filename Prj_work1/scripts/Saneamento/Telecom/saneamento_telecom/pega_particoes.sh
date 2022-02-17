#!/bin/bash
LOG="${1/%.sql/.log}"
ERR="${1/%.sql/.err}"
QTD_PARTICOES=${1}
sqlplus -S -m 'csv on delimiter ; QUOTE OFF' /nolog <<@EOF
CONNECT ${STRING_CONEXAO}
SET SERVEROUTPUT ON SIZE 1000000;
set heading off
set feedback off
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
SELECT row_id, nm_particao_nf, nm_particao_inf
  FROM (
        SELECT cp.rowid row_id, 
			   NVL(TRIM(cp.nm_particao_nf),'PARTICAO' || TO_CHAR(cp.dt_limite_inf_nf,'DDMMYYYY'))  AS nm_particao_nf, 
			   NVL(TRIM(cp.nm_particao_inf),'PARTICAO' || TO_CHAR(cp.dt_limite_inf_nf,'DDMMYYYY')) AS nm_particao_inf,
               ROW_NUMBER() OVER (ORDER BY DECODE(cp.st_processamento,'Reprocessar',1,2), cp.dt_limite_sup_nf, cp.dt_limite_sup_inf) num_linha
          FROM ${TABELA_CONTROLE} cp 
		 WHERE cp.dt_limite_inf_nf BETWEEN TO_DATE('${DATA_INICIO}','DD/MM/YYYY') AND TO_DATE('${DATA_FIM}','DD/MM/YYYY') 
		   AND cp.nm_processo = '${PROCESSO}'
           AND cp.st_processamento IN ('Aguardando','Reprocessar')
		 ORDER BY cp.dt_limite_inf_nf
       )
 WHERE num_linha <= ${QTD_PARTICOES};
@EOF
exit $?

