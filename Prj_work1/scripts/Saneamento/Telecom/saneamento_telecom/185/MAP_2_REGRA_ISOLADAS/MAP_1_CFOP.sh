#!/bin/bash
PARTICAO_NF=${1}
PARTICAO_INF=${2}
ROWID_CP=${3}
sqlplus -S /nolog <<@EOF >> ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.log 2>> ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.err
CONNECT ${STRING_CONEXAO}
set define off;
SET SERVEROUTPUT ON SIZE 1000000;
set timing on;
SPOOL  ${SPOOL_FILE} 
var v_st_processamento    VARCHAR2(50) = 'Em Processamento'
var v_msg_erro            VARCHAR2(4000) = 'MAP_1_CFOP'
var exit_code             NUMBER = 0
var v_qtd_processados     NUMBER = 0
var v_qtd_atu_nf          NUMBER = 0
var v_qtd_atu_inf         NUMBER = 0
var v_qtd_atu_comp        NUMBER = 0
var v_qtd_reg_paralizacao NUMBER = 0

WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_1_CFOP
PROMPT ### Inicio do processo ${0} - ${SERIE}  ###
PROMPT

DECLARE
 
   CURSOR c_sanea IS
	SELECT /*+ parallel(15) */
		 inf.ROWID AS rowid_inf, 
		 CASE WHEN SUBSTR(inf.CFOP,1,1) = '6' THEN '5' ELSE '6' END || SUBSTR(inf.CFOP, 2) AS CFOP
	FROM openrisow.filial f,
	     openrisow.item_nftl_serv      PARTITION (${PARTICAO_INF}) inf,  
	     openrisow.mestre_nftl_serv    PARTITION (${PARTICAO_NF})  nf   
	WHERE   ${FILTRO}
	      AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('AS1', 'AS2', 'AS3', 'T1') 
              AND (UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('ASS') OR nf.mnfst_dtemiss >= TO_DATE('01/04/2017','DD/MM/YYYY'))
		 -- AND nf.mnfst_dtemiss >= TO_DATE('01/01/2015','DD/MM/YYYY') AND nf.mnfst_dtemiss <= TO_DATE('31/12/2017','DD/MM/YYYY')	
		  AND inf.emps_cod                                               = nf.emps_cod
		  AND inf.fili_cod                                               = nf.fili_cod
		  AND inf.infst_serie                                            = nf.mnfst_serie
		  AND inf.infst_num                                              = nf.mnfst_num
		  AND inf.infst_dtemiss                                          = nf.mnfst_dtemiss
		  AND inf.mdoc_cod                                               = nf.mdoc_cod	  
	      AND f.emps_cod = inf.emps_cod
	      AND f.fili_cod = inf.fili_cod
	      AND inf.infst_ind_canc = 'N'
	      and ((f.unfe_sig = inf.uf and substr(inf.cfop,1,1) = '6') or (f.unfe_sig <> inf.uf and substr(inf.cfop,1,1) = '5'));
 

   TYPE t_sanea    IS TABLE OF c_sanea%ROWTYPE INDEX BY BINARY_INTEGER;
   v_sanea    t_sanea;
   v_reg      c_sanea%ROWTYPE;

   
   v_cnt      NUMBER     := 0;
   v_etapa    VARCHAR2(4000);
   PROCEDURE prc_tempo(pDDO IN VARCHAR2) AS 
   BEGIN
      v_etapa := substr(pDDO || ' >> ' || v_etapa,1,4000); 
      IF v_cnt <= 50 THEN
	     v_cnt := v_cnt + 1;
         DBMS_OUTPUT.PUT_LINE(substr(TO_CHAR(v_cnt) || ' >> ' || TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' ||  pDDO,1,2000)); 
	  END IF;
   EXCEPTION
     WHEN OTHERS THEN
	   v_cnt := 51;
   END;
   
BEGIN

   prc_tempo('begin');
  
  
	--
	open c_sanea;
	--
	loop
	fetch c_sanea bulk collect into v_sanea limit 100000;
	--
		if v_sanea.count > 0 then
		  --
		  forall v_index in 1 .. v_sanea.count
		    UPDATE openrisow.item_nftl_serv inf SET inf.var05 = substr('v2CFOP'||':'|| inf.CFOP ||'>>'||inf.var05,1,150) , inf.CFOP  = v_sanea(v_index).CFOP WHERE rowid = v_sanea(v_index).rowid_inf;
		    --
		    :v_qtd_processados := :v_qtd_processados + v_sanea.count;
		    :v_qtd_atu_inf     := :v_qtd_processados;
		    --
		end if;
	    --
		${COMMIT};
	    exit when c_sanea%notfound;
   --
   end loop;
   --
   close c_sanea;
   ${COMMIT};
    
   prc_tempo('end');
   dbms_output.put_line('Processados ${COMMIT} : ' || :v_qtd_processados || ' >> INF : ' || :v_qtd_atu_inf);
EXCEPTION
   WHEN OTHERS THEN
      ROLLBACK;
      prc_tempo('Erro : ' || SUBSTR(SQLERRM,1,500));
      :v_msg_erro := SUBSTR(v_etapa || ' >> ' || :v_msg_erro,1,4000);
      :v_st_processamento := 'Erro';
      :exit_code := 1;
END;
/

PROMPT Processado
ROLLBACK;
UPDATE ${TABELA_CONTROLE} cp
   SET cp.dt_fim_proc = SYSDATE,
       cp.st_processamento = :v_st_processamento,
       cp.ds_msg_erro = substr(substr(nvl(:v_msg_erro,' '),1,1000) || cp.ds_msg_erro ,1,4000),
       cp.qt_atualizados_nf = NVL(cp.qt_atualizados_nf,0) + :v_qtd_atu_nf,
       cp.qt_atualizados_inf = NVL(cp.qt_atualizados_inf,0) + :v_qtd_atu_inf,
       cp.qt_atualizados_comp = NVL(cp.qt_atualizados_comp,0) + :v_qtd_atu_comp--,
 WHERE cp.rowid = '${ROWID_CP}'
   AND cp.st_processamento = 'Em Processamento'
   AND cp.NM_PROCESSO = '${PROCESSO}';
COMMIT;


PROMPT Processado

exit :exit_code;

@EOF

RETORNO=$?

${WAIT}

exit ${RETORNO}

