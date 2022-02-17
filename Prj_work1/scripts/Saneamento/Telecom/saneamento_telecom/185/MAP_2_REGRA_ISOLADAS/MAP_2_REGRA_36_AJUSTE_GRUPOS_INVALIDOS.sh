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
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_36_AJUSTE_GRUPOS_INVALIDOS'
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
PROMPT MAP_2_REGRA_36_AJUSTE_GRUPOS_INVALIDOS
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
              WHEN (		(
						        -- 1 ) **Erro com Base ou ICMS e Isentas**
						            (NVL(inf.infst_base_icms,0) <> 0 OR NVL(inf.infst_val_icms,0) <> 0) AND   NVL(inf.infst_isenta_icms,0) <> 0 -- AND NVL(inf.infst_outras_icms,0)  = 0
                               AND  (NVL(inf.estb_cod,'99') <> '20' OR inf.infst_tribicms <> 'S' OR  inf.cfop = '0000' OR NVL(inf.infst_aliq_icms,0) = 0 or NVL(inf.infst_val_red,0) != NVL(inf.infst_isenta_icms,0))
					         )
						   OR 
						    (
							    -- 2 ) **Erro com Base ou ICMS**
							       (NVL(inf.infst_base_icms,0) <> 0   OR   NVL(inf.infst_val_icms,0) <> 0) AND NVL(inf.infst_outras_icms,0)  = 0 AND   NVL(inf.infst_isenta_icms,0) = 0
							   AND (inf.cfop = '0000'   OR   NVL(inf.estb_cod,'99') <> '00'   OR   inf.infst_tribicms <> 'S') 
						    ) 
				   ) 
			  THEN
				  CASE
					WHEN UPPER(TRIM(NVL(MAX(inf.CFOP) OVER ( PARTITION BY nf.rowid ),'0000')) ) = '0000' THEN
					   CASE  
					   WHEN  NVL(LENGTH(NVL(TRIM(inf.CGC_CPF),'9999')),0) > 11 THEN -- PJ 
						-- Se cliente PJ CFOP = '5303'
						'5303'
					   ELSE
						-- Se cliente PF CFOP = '5307'
						'5307'
					   END
				  ELSE
					UPPER(TRIM(NVL(MAX(inf.CFOP) OVER ( PARTITION BY nf.rowid ),'0000')) )
				  END
              ELSE
                inf.cfop	  
            END) CFOP_NEW,
            to_NUMBER(0) update_reg,
			inf.*
	FROM    openrisow.item_nftl_serv      PARTITION (${PARTICAO_INF}) inf,  
			openrisow.mestre_nftl_serv    PARTITION (${PARTICAO_NF})  nf   
	WHERE   ${FILTRO}
	      AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ( 'AS1', 'AS2', 'AS3') 
		AND (UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('ASS') OR nf.mnfst_dtemiss >= TO_DATE('01/04/2017','DD/MM/YYYY'))
		--  AND nf.mnfst_dtemiss >= TO_DATE('01/01/2015','DD/MM/YYYY') AND nf.mnfst_dtemiss <= TO_DATE('31/12/2019','DD/MM/YYYY')	
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
					  
					  AND (
						     (
						        -- 1 ) **Erro com Base ou ICMS e Isentas**
						            (NVL(inf1.infst_base_icms,0) <> 0 OR NVL(inf1.infst_val_icms,0) <> 0) AND   NVL(inf1.infst_isenta_icms,0) <> 0 -- AND NVL(inf1.infst_outras_icms,0)  = 0
                               AND  (NVL(inf1.estb_cod,'99') <> '20' OR inf1.infst_tribicms <> 'S' OR  inf1.cfop = '0000' OR NVL(inf1.infst_aliq_icms,0) = 0 or NVL(inf1.infst_val_red,0) != NVL(inf1.infst_isenta_icms,0))
					         )
						   OR 
						    (
							    -- 2 ) **Erro com Base ou ICMS**
							       (NVL(inf1.infst_base_icms,0) <> 0   OR   NVL(inf1.infst_val_icms,0) <> 0) AND NVL(inf1.infst_outras_icms,0)  = 0 AND   NVL(inf1.infst_isenta_icms,0) = 0
							   AND (inf1.cfop = '0000'   OR   NVL(inf1.estb_cod,'99') <> '00'   OR   inf1.infst_tribicms <> 'S') 
						    )
						   OR
						    (   -- 3 ) **Erro com Valor em Outras**      
							     (
								  (NVL(inf1.infst_outras_icms,0) <> 0 or NVL(inf1.infst_val_desc,0) <> 0 ) AND--Alteração 16/02/2020 Registros que possuem apenas desconto
								   NVL(inf1.infst_base_icms,0) =  0 AND 
								   NVL(inf1.infst_val_icms,0) =  0 AND 
								   NVL(inf1.infst_isenta_icms,0) =  0
								 )
							   AND (inf1.infst_tribicms  <> 'P'   OR   NVL(inf1.estb_cod,'99')  <>  '90' or NVL(inf1.infst_aliq_icms,0) <> 0)							
							)
							OR
							(
								-- 4 ) ** Erro com Valor em Isentas **   
							       (NVL(inf1.infst_isenta_icms,0) <> 0 AND NVL(inf1.infst_base_icms,0) = 0 AND NVL(inf1.infst_val_icms,0) =  0 AND NVL(inf1.infst_outras_icms,0) =  0)
							   AND (inf1.infst_tribicms  <> 'N'   OR   NVL(inf1.estb_cod,'99')  <>  '40')
							)
							OR
							(
							    -- 5 ) ** Erro no CFOP ‘0000’ **
								   (inf1.cfop   = '0000')
							   AND (inf1.infst_tribicms  <>  'P'   OR   NVL(inf1.estb_cod,'99')  <> '90')
							)	
							
							OR
							(
							    -- 6 ) *Erro Registro Zerado *

								   (NVL(inf1.infst_base_icms,0) = 0 AND NVL(inf1.infst_val_icms,0) =  0 AND NVL(inf1.infst_isenta_icms,0) = 0  AND NVL(inf1.infst_outras_icms,0) =  0
								   AND NVL(inf1.infst_val_serv,0) = 0  AND NVL(inf1.infst_val_cont,0) = 0   AND NVL(inf1.infst_val_desc,0) = 0)
							   AND (inf1.infst_tribicms  <>  'P'   OR   NVL(inf1.estb_cod,'99')  <> '90' OR inf1.cfop   != '0000'
							   or NVL(inf1.infst_aliq_icms,0) <> 0
							   )
							)	
					      )
					  )			
    ORDER BY rowid_nf,   last_ref_nf ;
    v_inf                 c_inf%ROWTYPE;

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

	IF 
	(
		-- 1 ) **Erro com Base ou ICMS e Isentas**
			(NVL(v_inf.infst_base_icms,0) <> 0 OR NVL(v_inf.infst_val_icms,0) <> 0) AND   NVL(v_inf.infst_isenta_icms,0) <> 0 -- AND NVL(v_inf.infst_outras_icms,0)  = 0
	   AND  (NVL(v_inf.estb_cod,'99') <> '20' OR v_inf.infst_tribicms <> 'S' OR  v_inf.cfop = '0000' OR NVL(v_inf.infst_aliq_icms,0) = 0  or NVL(v_inf.infst_val_red,0) != NVL(v_inf.infst_isenta_icms,0))
	)
	THEN
        v_inf.update_reg           			  := 1;
	    v_inf.VAR05                           := SUBSTR('r2015_36u<1>:' || v_inf.CFOP || '|' || NVL(v_inf.estb_cod,'99') || '|' || v_inf.infst_tribicms || '|' || v_inf.infst_val_red || '|' || v_inf.infst_aliq_icms|| '>>'|| v_inf.VAR05,1,150);
        v_inf.CFOP                            := v_inf.CFOP_NEW;
		v_inf.estb_cod                        := '20';
		v_inf.infst_tribicms    			  := 'S';
		v_inf.infst_val_red                   := v_inf.infst_isenta_icms;
		IF NVL(v_inf.infst_aliq_icms,0) = 0 THEN
			v_inf.infst_aliq_icms             := 25;  
		END IF;	
	END IF;	

	IF 
	(
		-- 2 ) **Erro com Base ou ICMS**
		   (NVL(v_inf.infst_base_icms,0) <> 0   OR   NVL(v_inf.infst_val_icms,0) <> 0) AND NVL(v_inf.infst_outras_icms,0)  = 0 AND   NVL(v_inf.infst_isenta_icms,0) = 0
	   AND (v_inf.cfop = '0000'   OR   NVL(v_inf.estb_cod,'99') <> '00'   OR   v_inf.infst_tribicms <> 'S') 
	)
	THEN
        v_inf.update_reg           			  := 1;
	    v_inf.VAR05                           := SUBSTR('r2015_36u<2>:' || v_inf.infst_val_red || '|' || v_inf.CFOP || '|' || NVL(v_inf.estb_cod,'99') || '|' || v_inf.infst_tribicms  || '|' || v_inf.infst_aliq_icms || '>>'|| v_inf.VAR05,1,150);
        v_inf.CFOP                            := v_inf.CFOP_NEW;
		v_inf.estb_cod                        := '00';
		v_inf.infst_tribicms    			  := 'S';	
		v_inf.infst_val_red                   := 0;
		IF NVL(v_inf.infst_aliq_icms,0) = 0 THEN
			v_inf.infst_aliq_icms             := 25;  
		END IF;			
	END IF;

	IF
	(   -- 3 ) **Erro com Valor em Outras**  ou todos valores zerados**    
		   (
		    (NVL(v_inf.infst_outras_icms,0) <> 0 or NVL(v_inf.infst_val_desc,0) <> 0 ) AND --Alteração 16/02/2020 Registros que possuem apenas desconto
		     NVL(v_inf.infst_base_icms,0) =  0 AND 
		     NVL(v_inf.infst_val_icms,0) =  0 AND 
		     NVL(v_inf.infst_isenta_icms,0) =  0
		   )
	   AND (v_inf.infst_tribicms  <> 'P'   OR   NVL(v_inf.estb_cod,'99')  <>  '90' or NVL(v_inf.infst_aliq_icms,0) <> 0) 							
	)
	THEN
        v_inf.update_reg           			  := 1;
	    v_inf.VAR05                           := SUBSTR('r2015_36u<3>:' || v_inf.infst_val_red || '|' || NVL(v_inf.estb_cod,'99') || '|' || v_inf.infst_tribicms  || '|' || v_inf.infst_aliq_icms || '>>'|| v_inf.VAR05,1,150);
		v_inf.estb_cod                        := '90';
		v_inf.infst_tribicms    			  := 'P';	
		v_inf.infst_aliq_icms                 := 0;
		v_inf.infst_val_red                   := 0;
		-- IF NVL(v_inf.infst_outras_icms,0) =  0 AND   NVL(v_inf.infst_val_desc,0) =  0 THEN
		--	v_inf.cfop                            := '0000';
		-- END IF;
	END IF;	

	IF
	(
		-- 4 ) ** Erro com Valor em Isentas **   
		   (NVL(v_inf.infst_isenta_icms,0) <> 0 AND NVL(v_inf.infst_base_icms,0) = 0 AND NVL(v_inf.infst_val_icms,0) =  0 AND NVL(v_inf.infst_outras_icms,0) =  0)
	   AND (v_inf.infst_tribicms  <> 'N'   OR   NVL(v_inf.estb_cod,'99')  <>  '40')
	)
	THEN
        v_inf.update_reg           			  := 1;
	    v_inf.VAR05                           := SUBSTR('r2015_36u<4>:' || v_inf.infst_val_red || '|' || NVL(v_inf.estb_cod,'99') || '|' || v_inf.infst_tribicms || '>>'|| v_inf.VAR05,1,150);
		v_inf.estb_cod                        := '40';
		v_inf.infst_tribicms    			  := 'N';
        v_inf.infst_val_red                   := 0;		
	END IF;	

	IF
	(
		-- 5 ) ** Erro no CFOP ‘0000’ **
		   (v_inf.cfop   = '0000')
	   AND (v_inf.infst_tribicms  <>  'P'   OR   NVL(v_inf.estb_cod,'99')  <> '90')
	)
	THEN
        v_inf.update_reg           			  := 1;
	    v_inf.VAR05                           := SUBSTR('r2015_36u<5>:' || v_inf.infst_val_red || '|' || NVL(v_inf.estb_cod,'99') || '|' || v_inf.infst_tribicms || '>>'|| v_inf.VAR05,1,150);
		v_inf.estb_cod                        := '90';
		v_inf.infst_tribicms    			  := 'P';		
		v_inf.infst_val_red                   := 0;
	END IF;	
	
	IF 
	(
	    -- 6 ) * Erro Registro Zerado *
			   (NVL(v_inf.infst_base_icms,0) = 0 
			   AND NVL(v_inf.infst_val_icms,0) =  0 
			   AND NVL(v_inf.infst_isenta_icms,0) = 0  
			   AND NVL(v_inf.infst_outras_icms,0) =  0)
			   AND NVL(v_inf.infst_val_serv,0) = 0
			   AND NVL(v_inf.infst_val_cont,0) = 0
			   AND NVL(v_inf.infst_val_desc,0) = 0
		   AND (v_inf.infst_tribicms  <>  'P'   OR   NVL(v_inf.estb_cod,'99')  <> '90' OR v_inf.cfop   != '0000'  or NVL(v_inf.infst_aliq_icms,0) <> 0)						
	)
	THEN
        v_inf.update_reg           			  := 1;
	    v_inf.VAR05                           := SUBSTR('r2015_36u<6>:' || v_inf.infst_val_red || '|' || v_inf.cfop || '|' || NVL(v_inf.estb_cod,'99') || '|' || v_inf.infst_tribicms || '|' || v_inf.infst_aliq_icms || '>>'|| v_inf.VAR05,1,150);
		v_inf.estb_cod                        := '90';
		v_inf.infst_tribicms    			  := 'P';	
		v_inf.cfop                            := '0000';		
		v_inf.infst_aliq_icms                 := 0;
		v_inf.infst_val_red                   := 0;
	END IF; 
			
	IF v_inf.update_reg = 1 THEN

	  UPDATE openrisow.item_nftl_serv inf
		SET inf.var05              = substr(v_inf.VAR05,1,150) 	 			
		  , inf.CFOP               = v_inf.CFOP
		  , inf.estb_cod           = v_inf.estb_cod 
		  , inf.infst_tribicms     = v_inf.infst_tribicms 
		  , inf.infst_val_red      = v_inf.infst_val_red 
		  , inf.infst_aliq_icms    = v_inf.infst_aliq_icms 
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

