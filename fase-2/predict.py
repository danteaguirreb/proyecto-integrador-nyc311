"""Inferencia desde entradas sin etiquetas, usando la decisión final congelada."""
from nyc311_phase2 import *
p=argparse.ArgumentParser(); p.add_argument('--run',required=True); p.add_argument('--input',required=True); p.add_argument('--output',required=True); a=p.parse_args()
root=Path(a.run).resolve(); c=json.loads((root/'config.resolved.json').read_text()); decision=json.loads((root/'decision.lock.json').read_text())
if not (root/'models/final').exists(): raise FileNotFoundError('No existe modelo final entrenado; ejecutar fase final con datos reales primero.')
c['output_root']=str(root)
local_encoder=root.parent/('resources/minilm-'+c['encoder_revision'])
if local_encoder.exists(): c['encoder_path']=str(local_encoder)
s=session(c); df=s.read.parquet(a.input)
# Entrada: unique_key, created_ts, categorías y descriptor. Sin fechas de cierre.
for col in CATS+['descriptor']:
 if col not in df.columns: df=df.withColumn(col,F.lit(None).cast('string'))
for name,angle in [('hour',F.hour('created_ts')*2*np.pi/24),('dow',(F.dayofweek('created_ts')-1)*2*np.pi/7),('month',(F.month('created_ts')-1)*2*np.pi/12)]:
 df=df.withColumn(name+'_sin',F.sin(angle)).withColumn(name+'_cos',F.cos(angle))
df=df.withColumn('text',F.concat(F.coalesce(F.trim('complaint_type'),F.lit('[MISSING]')),F.lit(' | '),F.coalesce(F.trim('descriptor'),F.lit('[MISSING]'))))
if decision['family']=='E4': df=with_embeddings(df,c,root)
model=PipelineModel.load(str(root/'models/final'))
result=model.transform(df).withColumn('risk_score',vector_to_array('probability')[1])
th=s.read.parquet(str(root/'thresholds/2024')).select('complaint_type','threshold_valid')
result=result.join(th,'complaint_type','left').withColumn('supported_type',F.coalesce('threshold_valid',F.lit(False)))
# Representación disponible no implica etiqueta histórica definida: abstenerse de alertar ese tipo.
result=result.withColumn('alert',F.when(F.col('supported_type'),F.col('risk_score')>=decision['threshold']))
result.select('unique_key','risk_score','supported_type','alert').write.mode('errorifexists').parquet(a.output)
s.stop()
