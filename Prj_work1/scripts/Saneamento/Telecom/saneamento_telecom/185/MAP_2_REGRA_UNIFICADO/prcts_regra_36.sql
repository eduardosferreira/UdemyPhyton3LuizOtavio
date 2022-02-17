-- CREATE OR REPLACE 
PROCEDURE prcts_regra_36(
    p_inf          IN OUT NOCOPY c_inf%rowtype,
	p_sanea        IN c_sanea%ROWTYPE)
AS
  v_nr_add_inf   NUMBER := 0;
  -- MAP_2_REG RA_36_AJUSTE_GRUPOS_INVALIDOS
   v_cfop          openrisow.item_nftl_serv.cfop%type := p_inf.cfop;
BEGIN
    
	IF 
	(
		-- 1 ) **Erro com Base ou ICMS e Isentas**
		(NVL(p_inf.infst_base_icms,0) <> 0 OR NVL(p_inf.infst_val_icms,0) <> 0) AND   NVL(p_inf.infst_isenta_icms,0) <> 0 -- AND NVL(p_inf.infst_outras_icms,0)  = 0
	   AND  (NVL(p_inf.estb_cod,'99') <> '20' OR p_inf.infst_tribicms <> 'S' OR  p_inf.cfop = '0000' OR NVL(p_inf.infst_aliq_icms,0) = 0  or NVL(p_inf.infst_val_red,0) != NVL(p_inf.infst_isenta_icms,0))
		
	)
	THEN
	    v_nr_add_inf            := 1;
        p_inf.update_reg        := 1;
	    p_inf.VAR05                           := SUBSTR('prcts_regra_36u<1>:' || p_inf.CFOP || '|' || NVL(p_inf.estb_cod,'99') || '|' || p_inf.infst_tribicms || '>>'|| p_inf.VAR05,1,150);
		SELECT 
				(CASE 
				  WHEN (		(
									-- 1 ) **Erro com Base ou ICMS e Isentas**
										(NVL(p_inf.infst_base_icms,0) <> 0 OR NVL(p_inf.infst_val_icms,0) <> 0) AND   NVL(p_inf.infst_isenta_icms,0) <> 0 -- AND NVL(p_inf.infst_outras_icms,0)  = 0
								   AND  (NVL(p_inf.estb_cod,'99') <> '20' OR p_inf.infst_tribicms <> 'S' OR  p_inf.cfop = '0000' OR NVL(p_inf.infst_aliq_icms,0) = 0 or NVL(p_inf.infst_val_red,0) != NVL(p_inf.infst_isenta_icms,0))
								 )
							   OR 
								(
									-- 2 ) **Erro com Base ou ICMS**
									   (NVL(p_inf.infst_base_icms,0) <> 0   OR   NVL(p_inf.infst_val_icms,0) <> 0) AND NVL(p_inf.infst_outras_icms,0)  = 0 AND   NVL(p_inf.infst_isenta_icms,0) = 0
								   AND (p_inf.cfop = '0000'   OR   NVL(p_inf.estb_cod,'99') <> '00'   OR   p_inf.infst_tribicms <> 'S') 
								) 
					   ) 
				  THEN
					  CASE
						WHEN NVL(p_sanea.cfop_max,'0000') = '0000' THEN
						   CASE  
						   WHEN  NVL(LENGTH(NVL(TRIM(p_inf.CGC_CPF),'9999')),0) > 11 THEN -- PJ 
							-- Se cliente PJ CFOP = '5303'
							'5303'
						   ELSE
							-- Se cliente PF CFOP = '5307'
							'5307'
						   END
					  ELSE
						p_sanea.cfop_max
					  END
				  ELSE
					p_inf.cfop	  
				END) CFOP_NEW
				INTO v_cfop
		FROM DUAL;	
		p_inf.CFOP                            := v_cfop;
		p_inf.estb_cod                        := '20';
		p_inf.infst_tribicms    			  := 'S';
		p_inf.infst_val_red                   := p_inf.infst_isenta_icms;
		IF NVL(p_inf.infst_aliq_icms,0) = 0 THEN
			p_inf.infst_aliq_icms             := 25;  
		END IF;	
	END IF;	

	IF 
	(
		-- 2 ) **Erro com Base ou ICMS**
		   (NVL(p_inf.infst_base_icms,0) <> 0   OR   NVL(p_inf.infst_val_icms,0) <> 0) AND NVL(p_inf.infst_outras_icms,0)  = 0 AND   NVL(p_inf.infst_isenta_icms,0) = 0
	   AND (p_inf.cfop = '0000'   OR   NVL(p_inf.estb_cod,'99') <> '00'   OR   p_inf.infst_tribicms <> 'S') 
	)
	THEN
	    v_nr_add_inf            := 1;
        p_inf.update_reg        := 1;
	    p_inf.VAR05                           := SUBSTR('prcts_regra_36u<2>:' || p_inf.infst_val_red || '|' || p_inf.CFOP || '|' || NVL(p_inf.estb_cod,'99') || '|' || p_inf.infst_tribicms || '>>'|| p_inf.VAR05,1,150);
		SELECT 
				(CASE 
				  WHEN (		(
									-- 1 ) **Erro com Base ou ICMS e Isentas**
										(NVL(p_inf.infst_base_icms,0) <> 0 OR NVL(p_inf.infst_val_icms,0) <> 0) AND   NVL(p_inf.infst_isenta_icms,0) <> 0 -- AND NVL(p_inf.infst_outras_icms,0)  = 0
								   AND  (NVL(p_inf.estb_cod,'99') <> '20' OR p_inf.infst_tribicms <> 'S' OR  p_inf.cfop = '0000' OR NVL(p_inf.infst_aliq_icms,0) = 0)
								 )
							   OR 
								(
									-- 2 ) **Erro com Base ou ICMS**
									   (NVL(p_inf.infst_base_icms,0) <> 0   OR   NVL(p_inf.infst_val_icms,0) <> 0) AND NVL(p_inf.infst_outras_icms,0)  = 0 AND   NVL(p_inf.infst_isenta_icms,0) = 0
								   AND (p_inf.cfop = '0000'   OR   NVL(p_inf.estb_cod,'99') <> '00'   OR   p_inf.infst_tribicms <> 'S') 
								) 
					   ) 
				  THEN
					  CASE
						WHEN NVL(p_sanea.cfop_max,'0000') = '0000' THEN
						   CASE  
						   WHEN  NVL(LENGTH(NVL(TRIM(p_inf.CGC_CPF),'9999')),0) > 11 THEN -- PJ 
							-- Se cliente PJ CFOP = '5303'
							'5303'
						   ELSE
							-- Se cliente PF CFOP = '5307'
							'5307'
						   END
					  ELSE
						p_sanea.cfop_max
					  END
				  ELSE
					p_inf.cfop	  
				END) CFOP_NEW
				INTO v_cfop
		FROM DUAL;	    
		p_inf.CFOP                            := v_cfop;
		p_inf.estb_cod                        := '00';
		p_inf.infst_tribicms    			  := 'S';	
		p_inf.infst_val_red                   := 0;
		IF NVL(p_inf.infst_aliq_icms,0) = 0 THEN
			p_inf.infst_aliq_icms             := 25;  
		END IF;			
	END IF;
	

	IF
	(   -- 3 ) **Erro com Valor em Outras**  ou todos valores zerados**    
		   (
		    (NVL(p_inf.infst_outras_icms,0) <> 0 or NVL(p_inf.infst_val_desc,0) <> 0 ) AND --Alteração 16/02/2020 Registros que possuem apenas desconto
		   NVL(p_inf.infst_base_icms,0) =  0 AND 
		   NVL(p_inf.infst_val_icms,0) =  0 AND 
		   NVL(p_inf.infst_isenta_icms,0) =  0
		   )
	   AND (p_inf.infst_tribicms  <> 'P'   OR   NVL(p_inf.estb_cod,'99')  <>  '90' or NVL(p_inf.infst_aliq_icms,0) <> 0) 							
	)
	THEN
	    v_nr_add_inf                          := 1;
        p_inf.update_reg           			  := 1;
	    p_inf.VAR05                           := SUBSTR('r2015_36u<3>:' || p_inf.infst_val_red || '|' || NVL(p_inf.estb_cod,'99') || '|' || p_inf.infst_tribicms || '>>'|| p_inf.VAR05,1,150);
		p_inf.estb_cod                        := '90';
		p_inf.infst_tribicms    			  := 'P';	
		p_inf.infst_aliq_icms                 := 0;
		p_inf.infst_val_red                   := 0;
		-- IF NVL(p_inf.infst_outras_icms,0) =  0 AND   NVL(p_inf.infst_val_desc,0) =  0 THEN
		--	p_inf.cfop                            := '0000';
		-- END IF;
	END IF;	

	IF
	(
		-- 4 ) ** Erro com Valor em Isentas **   
		   (NVL(p_inf.infst_isenta_icms,0) <> 0 AND NVL(p_inf.infst_base_icms,0) = 0 AND NVL(p_inf.infst_val_icms,0) =  0 AND NVL(p_inf.infst_outras_icms,0) =  0)
	   AND (p_inf.infst_tribicms  <> 'N'   OR   NVL(p_inf.estb_cod,'99')  <>  '40')
	)
	THEN
	    v_nr_add_inf                          := 1;
        p_inf.update_reg           			  := 1;
	    p_inf.VAR05                           := SUBSTR('r2015_36u<4>:' || p_inf.infst_val_red || '|' || NVL(p_inf.estb_cod,'99') || '|' || p_inf.infst_tribicms || '>>'|| p_inf.VAR05,1,150);
		p_inf.estb_cod                        := '40';
		p_inf.infst_tribicms    			  := 'N';		
		p_inf.infst_val_red                   := 0;	
	END IF;	

	IF
	(
		-- 5 ) ** Erro no CFOP ‘0000’ **
		   (p_inf.cfop   = '0000')
	   AND (p_inf.infst_tribicms  <>  'P'   OR   NVL(p_inf.estb_cod,'99')  <> '90')
	)
	THEN
	    v_nr_add_inf                          := 1;
        p_inf.update_reg           			  := 1;
	    p_inf.VAR05                           := SUBSTR('r2015_36u<5>:' || p_inf.infst_val_red || '|' || NVL(p_inf.estb_cod,'99') || '|' || p_inf.infst_tribicms || '>>'|| p_inf.VAR05,1,150);
		p_inf.estb_cod                        := '90';
		p_inf.infst_tribicms    			  := 'P';
		p_inf.infst_val_red                   := 0;		
	END IF;	
	
	IF 
	(
	    -- 6 ) * Erro Registro Zerado *
			   (NVL(p_inf.infst_base_icms,0) = 0 
			   AND NVL(p_inf.infst_val_icms,0) =  0 
			   AND NVL(p_inf.infst_isenta_icms,0) = 0  
			   AND NVL(p_inf.infst_outras_icms,0) =  0)
			   AND NVL(p_inf.infst_val_serv,0) = 0
			   AND NVL(p_inf.infst_val_cont,0) = 0
			   AND NVL(p_inf.infst_val_desc,0) = 0
		   AND (p_inf.infst_tribicms  <>  'P'   OR   NVL(p_inf.estb_cod,'99')  <> '90' OR p_inf.cfop   != '0000'  or NVL(p_inf.infst_aliq_icms,0) <> 0)						
	)
	THEN
		v_nr_add_inf                          := 1;
        p_inf.update_reg           			  := 1;
	    p_inf.VAR05                           := SUBSTR('r2015_36u<6>:' || p_inf.infst_val_red || '|' || p_inf.cfop || '|' || NVL(p_inf.estb_cod,'99') || '|' || p_inf.infst_tribicms || '>>'|| p_inf.VAR05,1,150);
		p_inf.estb_cod                        := '90';
		p_inf.infst_tribicms    			  := 'P';	
		p_inf.cfop                            := '0000';		
		p_inf.infst_aliq_icms                 := 0;
		p_inf.infst_val_red                   := 0;
	END IF; 

	IF v_nr_add_inf > 0 AND NVL(p_inf.update_reg,0) != 0 AND p_inf.rowid_inf IS NOT NULL
	THEN
	  v_inf_rules      := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
										p_nm_var01      =>  p_inf.emps_cod,
										p_nm_var02      =>  p_inf.fili_cod,
										p_nm_var03      =>  p_inf.infst_serie,
										p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
										p_nm_var05      =>  '|R2015_36|',
										p_nr_var02      =>  1);	  	
      		
	END IF;


END;
--/