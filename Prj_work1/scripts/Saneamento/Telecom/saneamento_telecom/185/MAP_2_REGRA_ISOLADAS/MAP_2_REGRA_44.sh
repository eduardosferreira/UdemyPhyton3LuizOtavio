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
var v_st_processamento    VARCHAR2(50)   = 'Em Processamento'
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_44'
var exit_code             NUMBER         = 0
var v_qtd_processados     NUMBER         = 0
var v_qtd_atu_nf          NUMBER         = 0
var v_qtd_atu_inf         NUMBER         = 0
var v_qtd_ins_inf         NUMBER         = 0
var v_qtd_atu_cli         NUMBER         = 0
var v_qtd_atu_comp        NUMBER         = 0
var v_qtd_reg_paralizacao NUMBER         = 0

WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_REGRA_44
PROMPT ### Inicio do processo ${0} - ${SERIE}  ###
PROMPT

DECLARE   
    CURSOR c_inf
       IS
	SELECT  /*+ parallel(15) */
			CASE lead(nf.rowid, 1) over (order by nf.rowid)
			  WHEN nf.rowid THEN 'N'
			  ELSE 'S'
			END last_ref_nf,  
			nf.rowid rowid_nf,
			nf.cnpj_cpf,
			nf.mnfst_num,
			UPPER(TRANSLATE(nf.mnfst_serie, 'x ', 'x')) serie,
			inf.rowid             rowid_inf,         
			(CASE 
              WHEN inf.CFOP = '0000' THEN
                CASE 
                   WHEN (SELECT  UPPER(TRIM(MAX(t1.SERV_COD))) FROM GFCADASTRO.TMP_ST_01 t1 WHERE UPPER(TRIM(t1.SERV_COD)) = UPPER(TRIM(inf.SERV_COD)) OR  UPPER(TRIM(t1.SERV_COD_ORIGINAL))   = UPPER(trim(substr(replace(replace(replace(inf.SERV_COD,UPPER(TRANSLATE(inf.INFST_SERIE, 'x ', 'x')) || 'ZP',''),UPPER(TRANSLATE(inf.INFST_SERIE, 'x ', 'x')) || 'ZN',''),UPPER(TRANSLATE(inf.INFST_SERIE, 'x ', 'x')) || 'Z',''),1,60)))
					  ) IS NOT NULL THEN
                      CASE
                        WHEN UPPER(TRIM(NVL(MAX(inf.CFOP) OVER ( PARTITION BY nf.rowid ),'0000')) ) = '0000' THEN
                           CASE 
                           WHEN  NVL(LENGTH(NVL(TRIM(inf.CGC_CPF),'9999')),0) > 11 THEN -- PJ 
                            -- Se cliente PJ CFOP = '6303'
                            '6303'
                           ELSE
                            -- Se cliente PF CFOP = '6307'
                            '6307'
                           END
                      ELSE
                        UPPER(TRIM(NVL(MAX(inf.CFOP) OVER ( PARTITION BY nf.rowid ),'0000')) )
                      END
                ELSE
                  NULL
                END
              ELSE
                NULL	  
            END) CFOP_NEW,
            to_NUMBER(0) update_reg,
			inf.*
	FROM    openrisow.item_nftl_serv      PARTITION (${PARTICAO_INF}) inf,  
			openrisow.mestre_nftl_serv    PARTITION (${PARTICAO_NF})  nf   
	WHERE   ${FILTRO}
	      AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ( 'AS1', 'AS2', 'AS3') 
		AND (UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('ASS') OR nf.mnfst_dtemiss >= TO_DATE('01/04/2017','DD/MM/YYYY'))
		  AND nf.mnfst_dtemiss >= TO_DATE('01/01/2015','DD/MM/YYYY') AND nf.mnfst_dtemiss <= TO_DATE('31/12/2016','DD/MM/YYYY')	
		  AND inf.emps_cod                                               = nf.emps_cod
		  AND inf.fili_cod                                               = nf.fili_cod
		  AND inf.infst_serie                                            = nf.mnfst_serie
		  AND inf.infst_num                                              = nf.mnfst_num
		  AND inf.infst_dtemiss                                          = nf.mnfst_dtemiss
		  AND inf.mdoc_cod                                               = nf.mdoc_cod	  
		  AND EXISTS (SELECT 1 FROM  openrisow.item_nftl_serv    PARTITION (${PARTICAO_INF}) inf1 
					  WHERE inf1.emps_cod                                               = nf.emps_cod
					  AND   inf1.fili_cod                                               = nf.fili_cod
					  AND   inf1.infst_serie                                            = nf.mnfst_serie
					  AND   inf1.infst_num                                              = nf.mnfst_num
					  AND   inf1.infst_dtemiss                                          = nf.mnfst_dtemiss
					  AND   inf1.mdoc_cod                                               = nf.mdoc_cod
					  AND   inf1.CFOP                                                   = '0000' 
					  AND   EXISTS (SELECT 1 FROM GFCADASTRO.TMP_ST_01 t1 
					  WHERE UPPER(TRIM(t1.SERV_COD)) = UPPER(TRIM(inf1.SERV_COD)) 
					  OR  UPPER(TRIM(t1.SERV_COD_ORIGINAL))   = UPPER(trim(substr(replace(replace(replace(inf1.SERV_COD,UPPER(TRANSLATE(inf1.INFST_SERIE, 'x ', 'x')) || 'ZP',''),UPPER(TRANSLATE(inf1.INFST_SERIE, 'x ', 'x')) || 'ZN',''),UPPER(TRANSLATE(inf1.INFST_SERIE, 'x ', 'x')) || 'Z',''),1,60)))
					  )
					 )    
    ORDER BY rowid_nf,   last_ref_nf ;
    v_inf                 c_inf%ROWTYPE;
    v_pref_servfz         VARCHAR2(10);
    v_pref_servfzp        VARCHAR2(10);
    v_pref_servfzn        VARCHAR2(10);	
	
	v_rowid_nf            ROWID:=NULL;
	v_infst_num_seq_max   openrisow.item_nftl_serv.infst_num_seq%TYPE := NULL;
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

   prc_tempo('INICIO');
  

   
   OPEN c_inf;
   LOOP
    FETCH c_inf INTO v_inf;
    EXIT WHEN c_inf%NOTFOUND;

	-- Inicializacoes  
	:v_qtd_processados := :v_qtd_processados+1;
	v_inf.update_reg   := 0;
	
	if ( v_rowid_nf is null ) or (v_rowid_nf != v_inf.rowid_nf) then
	   v_rowid_nf                     := v_inf.rowid_nf;
	end if;	

	
	IF     NVL(LENGTH(TRIM(NVL(v_inf.CFOP_NEW,''))),0) > 0				
	THEN		
		-- Altera o registro original
	    v_pref_servfz                         := TRANSLATE(v_inf.serie,'x ','x') || 'Z';
	    v_pref_servfzp                        := TRANSLATE(v_inf.serie,'x ','x') || 'ZP';
	    v_pref_servfzn                        := TRANSLATE(v_inf.serie,'x ','x') || 'ZN';
        v_inf.update_reg           			  := 1;
	    v_inf.VAR05                           := SUBSTR('r2015_44u:' || v_inf.CFOP || '|' || v_inf.SERV_COD || '>>'|| v_inf.VAR05,1,150);
        v_inf.CFOP                            := v_inf.CFOP_NEW;
		v_inf.SERV_COD                        := trim(substr(replace(replace(replace(v_inf.SERV_COD,v_pref_servfzp,''),v_pref_servfzn,''),v_pref_servfz,''),1,60));
   END IF;	
			
	IF v_inf.update_reg = 1 THEN

	  UPDATE openrisow.item_nftl_serv inf
		SET inf.var05              = substr(v_inf.VAR05,1,150) 	 			
		  , inf.CFOP               = v_inf.CFOP
		  , inf.SERV_COD           = v_inf.SERV_COD 
	  WHERE rowid = v_inf.rowid_inf;
	  v_inf.update_reg   := 0;
	  :v_qtd_atu_inf     := :v_qtd_atu_inf + 1;
	  
	END IF;	

	IF v_inf.last_ref_nf  = 'S' THEN 	
		${COMMIT};
	END IF;
			
   END LOOP;
   CLOSE c_inf;
   
   ${COMMIT};
   prc_tempo('FIM');
   prc_tempo('Processados ${COMMIT} : ' || :v_qtd_processados || ' >> INF : ' || :v_qtd_atu_inf );

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
   SET cp.dt_fim_proc          = SYSDATE,
       cp.st_processamento     = :v_st_processamento,
       cp.ds_msg_erro          = substr(substr(nvl(:v_msg_erro,' '),1,1000) || ' >> ' || cp.ds_msg_erro ,1,4000),
       cp.qt_atualizados_nf    = NVL(cp.qt_atualizados_nf,0)   + :v_qtd_atu_nf,
       cp.qt_atualizados_inf   = NVL(cp.qt_atualizados_inf,0)  + :v_qtd_atu_inf,
       cp.qt_atualizados_cli   = NVL(cp.qt_atualizados_cli,0)  + :v_qtd_atu_cli,
       cp.qt_atualizados_comp  = NVL(cp.qt_atualizados_comp,0) + :v_qtd_atu_comp--,
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

