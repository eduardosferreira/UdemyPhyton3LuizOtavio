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
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_9_AJUSTE_INF_0_DES_PRE.'
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
PROMPT MAP_2_REGRA_9_AJUSTE_INF_0_DES_PRE.
PROMPT ### Inicio do processo ${0} - ${SERIE}  ###
PROMPT

DECLARE

	
   CURSOR c_sanea
       IS
	SELECT  /*+ parallel(15) */
	  CASE lead(nf.rowid, 1) over (order by nf.rowid)
		  WHEN nf.rowid THEN 'N'
		  ELSE 'S'
	  END ultimo_item_nf,	
	  nf.rowid               rowid_nf,
      UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) serie,
	  nf.mnfst_serie,
	  nf.cnpj_cpf,
	  nf.mnfst_num,
	  inf.rowid             rowid_inf,   -- usado para a atualizacao do iten de desconto
	  inf.infst_val_icms    , 
	  inf.infst_isenta_icms ,
	  inf.infst_outras_icms ,
	  inf.infst_tribicms    ,
	  inf.infst_tip_isencao ,
	  inf.estb_cod          ,
	  inf.cfop              ,
	  inf.infst_base_icms   ,
	  inf.infst_val_desc    ,
	  inf.infst_val_cont    ,
	  inf.emps_cod          ,
	  inf.fili_cod          ,
	  inf.serv_cod          ,
	  inf.infst_dtemiss     ,
      inf.infst_serie       ,
	  inf.infst_num         ,
      inf.mdoc_cod          ,
      inf.infst_num_seq	 ,
      inf.infst_val_serv	  
    FROM openrisow.item_nftl_serv   PARTITION (${PARTICAO_INF}) inf,         
         openrisow.mestre_nftl_serv PARTITION (${PARTICAO_NF})  nf
    WHERE ${FILTRO}
	      AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) IN ('1', 'UT') 
		  AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ( 'AS1', 'AS2', 'AS3', 'T1')
	      AND (UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('ASS') OR nf.mnfst_dtemiss >= TO_DATE('01/04/2017','DD/MM/YYYY'))
	      AND nf.mnfst_dtemiss >= TO_DATE('01/01/2015','DD/MM/YYYY') AND nf.mnfst_dtemiss <= TO_DATE('31/12/2016','DD/MM/YYYY')		
	/*
	  AND nf.mnfst_dtemiss    >= TO_DATE('01/01/2015','DD/MM/YYYY')
	  AND nf.mnfst_dtemiss    <= TO_DATE('31/01/2015','DD/MM/YYYY')
	  AND nf.mnfst_serie       = 'U  T'
	  AND nf.emps_cod          = 'TBRA'
	  AND nf.fili_cod          = '0001'	
	  AND EXISTS (SELECT 1
							  FROM openrisow.item_nftl_serv   PARTITION (${PARTICAO_INF}) inf2
							  WHERE inf2.infst_dtemiss   = nf.mnfst_dtemiss
							  AND inf2.infst_serie       = nf.mnfst_serie
							  AND inf2.emps_cod          = nf.emps_cod
							  AND inf2.fili_cod          = nf.fili_cod
							  AND inf2.mdoc_cod          = nf.mdoc_cod
							  AND inf2.infst_num         = nf.mnfst_num							  
							  AND inf2.infst_outras_icms = 0
							  AND inf2.infst_isenta_icms = 0
							  AND inf2.infst_val_desc    = 0
							  AND inf2.infst_base_icms  <> 0
							  AND inf2.infst_val_serv   <> inf2.infst_base_icms
							  )
	  */
		  AND inf.emps_cod = nf.emps_cod
          AND inf.fili_cod = nf.fili_cod
          AND inf.infst_serie = nf.mnfst_serie
          AND inf.infst_num = nf.mnfst_num
          AND inf.infst_dtemiss = nf.mnfst_dtemiss
          AND inf.mdoc_cod = nf.mdoc_cod
		  AND (     nvl(inf.INFST_ISENTA_ICMS,0)   = 0
			    AND nvl(inf.INFST_OUTRAS_ICMS,0)   = 0
			    AND nvl(inf.INFST_VAL_DESC,0)     <> 0
			  )
   ORDER BY rowid_nf,ultimo_item_nf; 
   v_item      c_sanea%ROWTYPE;
   v_alterou_item    	 BOOLEAN := false;   
   
   v_pref                VARCHAR2(10) := 'r09';
   v_pref_var05          VARCHAR2(30);

   v_etapa    VARCHAR2(4000); 
   v_cnt      NUMBER     := 0;   
   PROCEDURE prc_tempo(pDDO IN VARCHAR2) AS 
   BEGIN
      v_etapa := substr(pDDO || ' >> ' || v_etapa,1,4000); 
      v_cnt := v_cnt + 1;
      DBMS_OUTPUT.PUT_LINE(substr(TO_CHAR(v_cnt) || ' >> ' || TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' ||  pDDO,1,2000));
   EXCEPTION
     WHEN OTHERS THEN
	   v_cnt := 51;
   END;
   
BEGIN

   prc_tempo('begin');

   OPEN c_sanea;
   LOOP
      FETCH c_sanea INTO v_item;
      EXIT WHEN c_sanea%NOTFOUND;

      :v_qtd_processados := :v_qtd_processados+1;

      -- Inicializacoes de INF
      v_pref_var05 := 'k' || lower(v_item.serie) || v_pref;	   
       
      -- Nro da Regra 9) Ajuste de Itens Zerados com Desconto Preenchido
 	  -- (*) Atribuir ao valor de outras, o valor do desconto, porém negativo (INFST_OUTRAS_ICMS := NFST_VAL_DESC * (-1))
	  v_item.infst_outras_icms := nvl(v_item.infst_val_desc,0) * -1;
	  -- (*) Zerar o valor de desconto
	  v_item.infst_val_desc := 0;		
	  -- (*) Resumarizar os valores do val_serv e val_cont
	  v_item.infst_val_serv  := nvl(v_item.infst_outras_icms,0);
	  v_item.infst_val_cont  := nvl(v_item.infst_outras_icms,0);

	  UPDATE openrisow.item_nftl_serv inf
	  SET inf.var05             = substr(v_pref_var05 ||':'|| inf.infst_outras_icms || '|' || inf.infst_val_desc || '|' || inf.infst_val_serv || '|' || inf.infst_val_cont  ||'>>'||inf.var05,1,150) 			
	    , inf.infst_outras_icms = v_item.infst_outras_icms
	    , inf.infst_val_desc    = v_item.infst_val_desc
	    , inf.infst_val_serv    = v_item.infst_val_serv 
	    , inf.infst_val_cont    = v_item.infst_val_cont
	  WHERE rowid = v_item.rowid_inf;
	  :v_qtd_atu_inf := :v_qtd_atu_inf + 1;
	 
   END LOOP;
   CLOSE c_sanea;
   
   ${COMMIT};		
   prc_tempo('END');
   prc_tempo('COMMIT >> ${COMMIT}');  
   prc_tempo('Processados:      ' || :v_qtd_processados);
   prc_tempo('Itens alterados:  ' || :v_qtd_atu_inf);
EXCEPTION
   WHEN OTHERS THEN
      ROLLBACK;
      prc_tempo('Erro : ' || SUBSTR(SQLERRM,1,500) || ' - rowid_inf >> ' || v_item.rowid_inf);
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

