-- CREATE OR REPLACE 
PROCEDURE prcts_corrige_mdoc_cod (p_inf          IN OUT NOCOPY c_inf%rowtype,
                                  p_nf  IN c_nf%rowtype
)
AS
BEGIN
    IF  NVL(p_nf.mdoc_cod,'0') <> NVL(p_inf.mdoc_cod,'0') THEN
      p_inf.update_reg         := 1;
      p_inf.var05              := SUBSTR('prcts_corrige_mdoc_cod:' || p_inf.mdoc_cod  || '>>' ||p_inf.var05,1,150);
      p_inf.mdoc_cod           :=  p_nf.mdoc_cod;
	  v_inf_rules                 := fncts_add_var(p_ds_rules      =>  v_inf_rules, 
													 p_nm_var01      =>  p_inf.emps_cod,
													 p_nm_var02      =>  p_inf.fili_cod,
													 p_nm_var03      =>  p_inf.infst_serie,
													 p_nm_var04      =>  TO_CHAR(p_inf.infst_dtemiss,'YYYY-MM-DD'),
													 p_nm_var05      =>  '|CORRIGE_MDOC_COD|',
													 p_nr_var02      =>  1);  	  
    END IF;
END;
--/