"""Prueba técnica de las cuatro rutas. No evalúa NYC311."""
from nyc311_phase2 import *
def main():
 c=json.loads(Path('config.sample.json').read_text()); c.update(output_root='../../work/integration',cores=1,driver_memory='1g')
 root=Path(c['output_root']).resolve(); root.mkdir(parents=True,exist_ok=True)
 s=session(c); s.sparkContext.setLogLevel('ERROR')
 rows=[(str(i),float(i%2),'Noise','PHONE','BRONX','Street','Music' if i%2 else 'Party','Noise | Music' if i%2 else 'Noise | Party',*[float((i+j)%5) for j in range(6)]) for i in range(60)]
 df=s.createDataFrame(rows,'unique_key string, label double, complaint_type string, open_data_channel_type string, borough string, location_type string, descriptor string, text string, '+', '.join(x+' double' for x in NUMS))
 emb=with_embeddings(df,c,root); assert emb.count()==60; assert emb.select(F.size('embedding')).first()[0]==384
 results=[]
 mlflow.set_tracking_uri('sqlite:///'+str(root/'mlflow.db')); mlflow.set_experiment('IMPLEMENTATION_TESTS_NOT_NYC311_RESULTS')
 for family,params in [('E1',{'regParam':.01,'elasticNetParam':0.}),('E2',{'regParam':.1,'elasticNetParam':0.}),('E3',{'maxDepth':3,'maxIter':2}),('E4',{'regParam':.01,'elasticNetParam':0.})]:
  with mlflow.start_run(run_name=family):
   tr=emb if family=='E4' else df
   model=pipeline(family,params).fit(tr); pred,seconds=materialize(model,tr); m=evaluate(pred); assert m['n']==60
   err=reload_check(model,tr,root/family)
   mlflow.log_params(params); mlflow.log_metric('reload_max_error',err); mlflow.log_artifacts(str(root/family),'pipeline_model')
   results.append({'family':family,'test_rows':60,'prediction_count_pass':True,'save_reload_pass':True,'max_error':err}); pred.unpersist()
 unseen=df.limit(1).withColumn('text',F.lit('Noise | A previously unseen descriptor'))
 e=with_embeddings(unseen,c,root); assert e.count()==1
 runs=mlflow.search_runs(experiment_names=['IMPLEMENTATION_TESTS_NOT_NYC311_RESULTS']); assert len(runs)>=4
 save_json('evidencia/integration_tests.json',{'artificial_fixture':True,'not_business_results':True,'encoder_revision':c['encoder_revision'],'dimension':384,'new_text_encoded':True,'mlflow_search_runs_pass':True,'families':results})
 print('PASS four pipeline routes, embeddings, save/reload, MLflow search'); s.stop()
if __name__=='__main__': main()
