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
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_27_INSERIR_ALIQUOTA_ITENS_BASE'
var exit_code             NUMBER = 0
var v_qtd_processados     NUMBER = 0
var v_qtd_atu_nf          NUMBER = 0
var v_qtd_atu_inf         NUMBER = 0
var v_qtd_atu_cli         NUMBER = 0
var v_qtd_atu_comp        NUMBER = 0
var v_qtd_reg_paralizacao NUMBER = 0

WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_REGRA_27_INSERIR_ALIQUOTA_ITENS_BASE
PROMPT ### Inicio do processo ${0} - ${SERIE}  ###
PROMPT

DECLARE
-- 03/02/2020 eduardof@kyros.com.br >> 
-- Alteração Solicitado pelo Fasuto ID 522:
-- Se:
-- (infst_base_icms <> 0 or  infst_val_icms <> 0) ( infst_aliq_icms =  0)  -- or estb_cod <> '00' or infst_tribicms <> 'S') Então
-- Buscar nos itens da nota infst_aliq_icms <> 0 e atribuir ao item encontrado inicialmente valor da infst_aliq_icms, caso não encoontra atribua 25.
-- --estb_cod = '00' 
-- --infst_tribicms = 'S'
-- Para não afetar o grupo com cst 20 não observar os campos estb_cod e infst_tribicms e nem atualizar os mesmos
	
   CURSOR c_inf
       IS
	SELECT  /*+ parallel(15) */
      CASE lead(nf.rowid, 1) over (order by nf.rowid)
			  WHEN nf.rowid THEN 'N'
			  ELSE 'S'
			END last_ref_nf, 	
	  nf.rowid              rowid_nf,
	  inf.rowid             rowid_inf,   
	  to_NUMBER(0)          update_reg,
	  CASE WHEN nvl(inf.infst_aliq_icms ,0) <> 0 
		   THEN 'A'
		   ELSE 'B'
	   END fl_aliq_icms,
	  CASE   
	       WHEN (nvl(inf.infst_base_icms,0) <> 0 or  nvl(inf.infst_val_icms,0) <> 0) and (
		   -- inf.estb_cod <> '00' or inf.infst_tribicms <> 'S' OR 
		   nvl(inf.infst_aliq_icms,0) =  0)
		   THEN 'S'
		   ELSE 'N' ---
	   END fl_err,
	  inf.*	  
    FROM openrisow.item_nftl_serv   PARTITION (${PARTICAO_INF}) inf,         
         openrisow.mestre_nftl_serv PARTITION (${PARTICAO_NF})  nf
    WHERE ${FILTRO}
	      --AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) in ('1','AA1')
		  AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ( 'AS1', 'AS2', 'AS3', 'T1') 
		AND (UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('ASS') OR nf.mnfst_dtemiss >= TO_DATE('01/04/2017','DD/MM/YYYY'))
	      AND nf.mnfst_dtemiss >= TO_DATE('01/01/2015','DD/MM/YYYY') AND nf.mnfst_dtemiss <= TO_DATE('31/12/2016','DD/MM/YYYY')		
		  AND inf.emps_cod = nf.emps_cod
          AND inf.fili_cod = nf.fili_cod
          AND inf.infst_serie = nf.mnfst_serie
          AND inf.infst_num = nf.mnfst_num
          AND inf.infst_dtemiss = nf.mnfst_dtemiss
          AND inf.mdoc_cod = nf.mdoc_cod
		  AND EXISTS (SELECT 1 FROM openrisow.item_nftl_serv PARTITION (${PARTICAO_INF}) inf1
					  WHERE inf1.emps_cod = nf.emps_cod
					  AND inf1.fili_cod = nf.fili_cod
					  AND inf1.infst_serie = nf.mnfst_serie
					  AND inf1.infst_num = nf.mnfst_num
					  AND inf1.infst_dtemiss = nf.mnfst_dtemiss
					  AND inf1.mdoc_cod = nf.mdoc_cod 		  
					  and (nvl(inf1.infst_base_icms,0) <> 0 or  nvl(inf1.infst_val_icms,0) <> 0)
                      and (nvl(inf1.infst_aliq_icms,0) =  0 ))-- or inf1.estb_cod <> '00' or inf1.infst_tribicms <> 'S'))
	ORDER BY rowid_nf, fl_aliq_icms desc;
    v_inf                 c_inf%ROWTYPE;
    v_infst_aliq_icms     openrisow.item_nftl_serv.infst_aliq_icms%type := null;
	v_rowid_inf           ROWID:=NULL;
	v_rowid_nf            ROWID:=NULL;
    v_ds_etapa            VARCHAR2(4000);
    PROCEDURE prc_tempo(p_ds_ddo IN VARCHAR2) AS 
    BEGIN
      v_ds_etapa := substr(p_ds_ddo || ' >> ' || v_ds_etapa,1,4000); 
      DBMS_OUTPUT.PUT_LINE(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' ||  p_ds_ddo ,1,2000));
    EXCEPTION
      WHEN OTHERS THEN
	    NULL;
    END;
   
BEGIN

   prc_tempo('inicio');

   OPEN c_inf;
   LOOP
      FETCH c_inf INTO v_inf;
      EXIT WHEN c_inf%NOTFOUND;
      :v_qtd_processados := :v_qtd_processados+1;	  
	  

	  if (v_rowid_nf is null ) or (v_rowid_nf != v_inf.rowid_nf) then
	    v_rowid_nf                     := v_inf.rowid_nf;
	    v_inf.update_reg               := 0;
		v_infst_aliq_icms              := null;--NULLIF(v_inf.infst_aliq_icms,0);--null;
		v_rowid_inf                    := null;
	  end if;	

	  IF v_inf.fl_aliq_icms = 'A' THEN
		v_infst_aliq_icms := v_inf.infst_aliq_icms;
	  END IF; 
	  
      IF v_inf.fl_err = 'S' THEN
	    v_rowid_inf       := v_inf.rowid_inf;
		IF NULLIF(v_infst_aliq_icms,0) IS NULL  THEN
			v_infst_aliq_icms := 25;
		END IF;
	  END IF;

	  IF v_rowid_inf IS NOT NULL THEN 	
		
		v_inf.update_reg               := 1;	
		UPDATE openrisow.item_nftl_serv inf  SET inf.var05             = substr('r2015_27u' ||':' || inf.infst_aliq_icms ||'>>'||inf.var05,1,150) 			
								               , inf.infst_aliq_icms   = nvl(v_infst_aliq_icms,0)
											   -- , inf.estb_cod          = '00'
                                               -- , inf.infst_tribicms    = 'S'
											   
		WHERE rowid                     = v_rowid_inf;
		:v_qtd_atu_inf                  := :v_qtd_atu_inf + 1;
		v_inf.update_reg               := 0;
		v_rowid_inf                    := NULL;
		${COMMIT};
		
	  END IF;
	  
   END LOOP;
   CLOSE c_inf;
   
   ${COMMIT};		
   prc_tempo('fim');
   prc_tempo('COMMIT >> ${COMMIT}');  
   prc_tempo('Processados:      ' || :v_qtd_processados);
   prc_tempo('Itens alterados:  ' || :v_qtd_atu_inf);
EXCEPTION
   WHEN OTHERS THEN
      ROLLBACK;
      prc_tempo('ERRO : ' || SUBSTR(SQLERRM,1,500) || ' - rowid_inf >> ' || v_inf.rowid_inf);
      :v_msg_erro := SUBSTR(v_ds_etapa || ' >> ' || :v_msg_erro,1,4000);
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
       cp.qt_atualizados_cli = NVL(cp.qt_atualizados_cli,0) + :v_qtd_atu_cli,
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

