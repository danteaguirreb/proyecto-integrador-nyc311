"""Comparaciones derivadas exclusivamente de runs reales MLflow terminados."""
from pathlib import Path
import json
import mlflow,pandas as pd,numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
root=Path(__file__).resolve().parent/'run_complete'
mlflow.set_tracking_uri('sqlite:///'+str(root/'mlflow.db'))
runs=mlflow.search_runs(experiment_names=['NYC311_Fase2']);runs.to_csv(root/'evidence/mlflow_runs.csv',index=False)
if 'tags.kind' not in runs:raise SystemExit('Aún sin candidatos')
x=runs[(runs['tags.kind']=='tuning_candidate')&(runs.status=='FINISHED')].sort_values('start_time').drop_duplicates(['tags.candidate','tags.fold'],keep='last')
if x.empty:raise SystemExit('Aún sin candidatos')
cols=['tags.family','tags.candidate','tags.fold','metrics.train_n','metrics.n','metrics.prevalence','metrics.areaUnderPR','metrics.areaUnderROC','metrics.precision','metrics.recall','metrics.f1','metrics.alert_fraction','metrics.fit_seconds','metrics.infer_seconds','metrics.embeddings_seconds','metrics.reload_max_error']
x[[a for a in cols if a in x]].to_csv(root/'evidence/experiment_folds.csv',index=False)
summary=x.groupby(['tags.family','tags.candidate']).agg(folds=('tags.fold','nunique'),pr_mean=('metrics.areaUnderPR','mean'),roc_mean=('metrics.areaUnderROC','mean'),f1_at_05=('metrics.f1','mean'),fit_mean=('metrics.fit_seconds','mean'),infer_mean=('metrics.infer_seconds','mean')).reset_index()
summary.to_csv(root/'evidence/experiment_summary.csv',index=False)
a=x[x['tags.candidate']=='E1_0'].set_index('tags.fold');b=x[x['tags.candidate']=='E4_0'].set_index('tags.fold')
rows=[]
for fold in a.index.intersection(b.index):
 rows.append({'fold':fold,'pr_E1':a.loc[fold,'metrics.areaUnderPR'],'pr_E4':b.loc[fold,'metrics.areaUnderPR'],'delta_pr':b.loc[fold,'metrics.areaUnderPR']-a.loc[fold,'metrics.areaUnderPR'],'n_E1':a.loc[fold,'metrics.n'],'n_E4':b.loc[fold,'metrics.n']})
pd.DataFrame(rows).to_csv(root/'evidence/e4_vs_e1.csv',index=False)
plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False,'figure.dpi':160})
fig,axes=plt.subplots(1,2,figsize=(10,3.8))
for year,color in [('2022','#235b78'),('2023','#9b502d')]:
 t=x[x['tags.fold']==year].set_index('tags.candidate').sort_index();pos=np.arange(len(t))
 axes[0].barh(pos+(-.18 if year=='2022' else .18),t['metrics.areaUnderPR'],height=.35,label=year,color=color)
 axes[0].set_yticks(pos,t.index)
 axes[1].barh(pos+(-.18 if year=='2022' else .18),t['metrics.fit_seconds'],height=.35,label=year,color=color)
 axes[1].set_yticks(pos,t.index)
axes[0].set(xlabel='Área PR',title='Validación por corte');axes[0].set_xlim(left=0,right=.43)
axes[0].legend(loc='lower left',bbox_to_anchor=(0,1.12),ncol=2,frameon=False)
axes[1].set(xlabel='Entrenamiento (s)',title='Costo de ajuste por corte');axes[1].set_xlim(left=0)
fig.tight_layout();fig.savefig(root/'evidence/validation_comparison.png',bbox_inches='tight',pad_inches=.15);plt.close(fig)
f=runs[(runs['tags.kind']=='final')&(runs.status=='FINISHED')]
if len(f):
 r=f.iloc[0]
 observed=json.loads((root/'evidence/final_result.json').read_text())
 exported={k:float(r['metrics.'+k]) for k in observed['metrics']}
 for k,v in observed['metrics'].items():
  assert abs(float(v)-exported[k])<1e-9,(k,v,exported[k])
 (root/'evidence/final_mlflow_metrics.json').write_text(json.dumps({'run_id':r.run_id,'metrics':exported,'verified_against_final_result':True},indent=2))
 matrix=np.array([[r['metrics.tn'],r['metrics.fp']],[r['metrics.fn'],r['metrics.tp']]],dtype=int)
 fig,ax=plt.subplots(figsize=(5.2,3.5));ax.imshow(matrix,cmap='Blues')
 for (i,j),v in np.ndenumerate(matrix):ax.text(j,i,f'{v:,}',ha='center',va='center',color='white' if v>matrix.max()*.6 else 'black')
 ax.set(xticks=[0,1],yticks=[0,1],xticklabels=['Sin alerta','Alerta'],yticklabels=['No prolongada','Prolongada'],xlabel='Decisión',ylabel='Clase real',title='Prueba final 2024');fig.tight_layout();fig.savefig(root/'evidence/final_confusion.png',bbox_inches='tight',pad_inches=.15);plt.close(fig)
print(summary.to_string(index=False))
