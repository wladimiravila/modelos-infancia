import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_regex_extract
import gs_derived
import gs_concat

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1716657773666 = glueContext.create_dynamic_frame.from_catalog(database="sisben", table_name="sisben_families", transformation_ctx="AmazonS3_node1716657773666")

# Script generated for node Select Fields
SelectFields_node1716662008392 = SelectFields.apply(frame=AmazonS3_node1716657773666, paths=["ide_ficha_origen", "cod_dpto", "cod_mpio", "cod_clase", "cod_barrio", "cod_vereda", "cod_corregimiento", "cod_comuna", "tot_viviendas", "tot_hogares", "tip_vivienda", "tip_mat_paredes", "tip_mat_pisos", "ind_tiene_energia", "tip_estrato_energia", "ind_tiene_alcantarillado", "ind_tiene_gas", "ind_tiene_recoleccion", "ind_tiene_acueducto", "tip_estrato_acueducto", "num_cuartos_vivienda", "num_hogares_vivienda", "tip_ocupa_vivienda", "num_cuartos_exclusivos", "num_cuartos_dormir", "num_cuartos_unicos_dormir", "tip_sanitario", "tip_ubi_sanitario", "tip_uso_sanitario", "tip_origen_agua", "ind_agua_llega_7dias", "tip_uso_agua_beber", "tip_elimina_basura", "ind_tiene_cocina", "tip_prepara_alimentos", "tip_uso_cocina", "tip_energia_cocina", "ind_tiene_pc", "ind_tiene_internet", "vlr_gasto_alimento", "vlr_gasto_educacion", "vlr_gasto_salud", "vlr_gasto_serv_publicos", "vlr_gasto_arriendo", "num_habita_vivienda", "ind_evento_inundacion", "num_evento_inundacion", "ind_evento_avalancha", "num_personas_hogar", "ide_nacional", "ide_persona", "sexo_persona", "edad_calculada", "tip_parentesco", "tip_estado_civil", "ind_conyuge_vive_hogar", "ind_padre_vive_hogar", "tip_seg_social", "ind_enfermo_30", "ind_acudio_salud", "ind_fue_atendido_salud", "ind_esta_embarazada", "ind_tuvo_hijos", "tip_cuidado_niños", "ind_recibe_comida", "ind_leer_escribir", "ind_estudia", "niv_educativo", "grado_alcanzado", "ind_fondo_pensiones", "tip_actividad_mes", "num_sem_buscando", "tip_empleado", "vlr_ingr_salario", "vlr_ingr_fam_accion", "vlr_ingr_col_mayor", "vlr_ingr_otro_subsidio", "ide_hogar", "clasificacion"], transformation_ctx="SelectFields_node1716662008392")

# Script generated for node Regex Extractor
RegexExtractor_node1716668181409 = SelectFields_node1716662008392.gs_regex_extract(colName="clasificacion", regex="^.{1}", newCols="clasificacion_letter")

# Script generated for node Sum Gastos
SumGastos_node1716671379634 = RegexExtractor_node1716668181409.gs_derived(colName="sum_gastos", expr="vlr_gasto_alimento+ vlr_gasto_educacion+ vlr_gasto_salud+vlr_gasto_serv_publicos+ vlr_gasto_arriendo")

# Script generated for node Sum Ingresos
SumIngresos_node1716673211943 = SumGastos_node1716671379634.gs_derived(colName="sum_ingresos", expr="vlr_ingr_salario+ vlr_ingr_fam_accion+ vlr_ingr_col_mayor+ vlr_ingr_otro_subsidio")

# Script generated for node Concatenate Columns
ConcatenateColumns_node1716674547025 = SumIngresos_node1716673211943.gs_concat(colName="ide_ficha_origen_hogar", colList=["ide_ficha_origen", "ide_hogar"], spacer="-")

# Script generated for node Drop Fields
DropFields_node1716673369170 = DropFields.apply(frame=ConcatenateColumns_node1716674547025, paths=["vlr_gasto_alimento", "vlr_gasto_educacion", "vlr_gasto_salud", "vlr_gasto_serv_publicos", "vlr_gasto_arriendo", "vlr_ingr_salario", "vlr_ingr_fam_accion", "vlr_ingr_col_mayor", "vlr_ingr_otro_subsidio", "ide_ficha_origen", "ide_hogar"], transformation_ctx="DropFields_node1716673369170")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1716677117570 = glueContext.write_dynamic_frame.from_catalog(frame=DropFields_node1716673369170, database="sisben", table_name="sisben_etl", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1716677117570")

job.commit()