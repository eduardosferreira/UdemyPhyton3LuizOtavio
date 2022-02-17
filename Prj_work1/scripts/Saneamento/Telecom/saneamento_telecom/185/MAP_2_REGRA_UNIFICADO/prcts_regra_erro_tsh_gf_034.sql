-- 1514;
-- CREATE OR REPLACE 
PROCEDURE prcts_regra_erro_tsh_gf_034(p_nf  IN  c_nf%rowtype)
AS

BEGIN
  


	IF REGEXP_LIKE(p_nf.mnfst_serie, '^[0-9,A-Z][0-9,A-Z, ]*$') = FALSE  THEN
		raise_application_error (-20343, 'SERIE Invalida! ' || ' >> emps_cod: ' || p_nf.emps_cod || ' >> fili_cod: ' || p_nf.fili_cod || ' >> mnfst_num: ' || p_nf.mnfst_num || ' >> mnfst_dtemiss: ' || p_nf.mnfst_dtemiss || ' >> mnfst_serie: ' || p_nf.mnfst_serie || ' >> mdoc_cod: ' || p_nf.mdoc_cod);
	END IF;
	

  
END;
--/	