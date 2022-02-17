#!/bin/sh
echo "INICIO"
wait
echo "Iniciando 2020 07"
wait
cd /TESHUVA/0001_BasesSaneamentoCadastro/REDUCAO_CADASTRO/kyros_reducao_dev2/frente_exec_mapas/sanea_nf/por_mes && nohup ./execute.sh checklist202007.sql "7" "  " >checklist202007.log 2>checklist202007.err &
wait
echo "Iniciando 2020 08"
wait
cd /TESHUVA/0001_BasesSaneamentoCadastro/REDUCAO_CADASTRO/kyros_reducao_dev2/frente_exec_mapas/sanea_nf/por_mes && nohup ./execute.sh checklist202008.sql "7" "  " >checklist202008.log 2>checklist202008.err &
wait
echo "Iniciando 2020 09"
wait
cd /TESHUVA/0001_BasesSaneamentoCadastro/REDUCAO_CADASTRO/kyros_reducao_dev2/frente_exec_mapas/sanea_nf/por_mes && nohup ./execute.sh checklist202009.sql "7" "  " >checklist202009.log 2>checklist202009.err &
wait
echo "FIM"
