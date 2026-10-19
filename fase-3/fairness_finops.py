from pathlib import Path
import json,calendar
import pandas as pd
B=Path(__file__).resolve().parent;E=B/'evidence';E.mkdir(exist_ok=True)
g=pd.read_csv(E/'test_subgroups.csv');final=json.loads((E/'final_mlflow_metrics.json').read_text())['metrics'];out=[]
for var in ['borough','open_data_channel_type']:
 t=g[g.group_variable.eq(var)].copy()
 if t.empty:raise ValueError(f'Missing grouping: {var}; available {g.group_variable.unique()}')
 for col,key in [('n','n'),('positives','positives'),('tp','tp'),('alerts','tp')]:
  expected=final[key] if col!='alerts' else final['tp']+final['fp'];assert t[col].sum()==expected,(var,col)
 t['fn']=t.positives-t.tp;t['fp']=t.alerts-t.tp;t['tn']=t.n-t.positives-t.fp
 assert (t[['n','tp','fp','fn','tn']]>=0).all().all()
 t['selection_rate']=t.alerts/t.n;t['prevalence']=t.positives/t.n;t['tpr']=t.tp/t.positives;t['fpr']=t.fp/(t.n-t.positives)
 # Largest group chosen without optimizing disparities; borough resolves to Brooklyn.
 ref=t.sort_values(['n','group_value'],ascending=[False,True]).iloc[0];t['reference']=ref.group_value
 t['SPD']=t.selection_rate-ref.selection_rate;t['DI']=t.selection_rate/ref.selection_rate;t['TPR_gap']=t.tpr-ref.tpr;t['FPR_gap']=t.fpr-ref.fpr;t['label_gap']=t.prevalence-ref.prevalence
 t['no_alert_DI']=(1-t.selection_rate)/(1-ref.selection_rate)
 t['gap_magnitude_change']=t.SPD.abs()-t.label_gap.abs();t['reading']=t.gap_magnitude_change.map(lambda x:'mayor en predicciones' if x>.02 else ('menor en predicciones' if x<-.02 else 'similar dentro de 2 pp'))
 t.loc[t.group_value.eq(ref.group_value),'reading']='referencia';t['diagnostic_DI_below_08']=t.DI<.8;t['group_variable']=var;out.append(t)
r=pd.concat(out,ignore_index=True);r.to_csv(E/'fairness.csv',index=False)
(E/'fairness_protocol.json').write_text(json.dumps({'cohort_n':int(final['n']),'threshold':.2,'favorable_assumption':'Alert means selection for additional human review, a potential benefit only if service capacity exists. No operational benefit demonstrated.','reference':'Largest observed group within each grouping, with lexicographic tie break; no search for largest disparity','SPD':'selection_rate(group) - selection_rate(reference)','DI':'selection_rate(group) / selection_rate(reference)','TPR':'TP / actual positives','FPR':'FP / actual negatives','gap_reading':'Absolute prediction disparity minus absolute label disparity; 0.02 tolerance is a declared descriptive convention, not a causal test','protected_attributes':'Unavailable. Borough and channel are operational dimensions, not race/sex proxies assigned to individuals. No protected-class fairness certification.','four_fifths':'DI<0.8 is a diagnostic heuristic from class, not a legal determination for this service','limitations':['Closed and historically labelable requests only','Thresholds differ by complaint type; case mix affects comparisons','No intersectional breakdown available in retained sufficient statistics','No retuning or mitigation selected with test2024','No confidence claim about future requests from a full snapshot']},indent=2,ensure_ascii=False))
# Public list prices consulted 2026-10-01. No resource provisioned.
scenarios=[]
for name,vm,ram,cpu,storage in [('Piloto 8GB',44,8,2,160),('Escalamiento vertical 16GB',84,16,4,320)]:
 for backup in [20,50,100]:
  infra=vm+backup*.05;ops=2*20
  scenarios.append({'scenario':name,'vcpus':cpu,'ram_gb':ram,'disk_gb':storage,'vm_usd_month':vm,'snapshot_stored_gb':backup,'snapshot_usd_gb_month':.05,'snapshot_usd_month':backup*.05,'infra_usd_month':infra,'contingency_20pct':infra*.2,'budget_infra_usd_month':infra*1.2,'ops_hours_assumed':2,'ops_usd_hour_assumed':20,'total_with_ops_usd_month':infra*1.2+ops})
pd.DataFrame(scenarios).to_csv(E/'finops_scenarios.csv',index=False)
(E/'finops_assumptions.json').write_text(json.dumps({'as_of':'2026-10-01','currency':'USD','provider':'AWS Lightsail Linux public IPv4','region_assumed':'us-east-1; verify availability and transfer allowance before any deployment','runtime':'one VM continuously provisioned; Kafka and Spark co-located; no high availability','snapshot_model':'50 GB retained snapshot storage in base case, not 50 GB copied every day; sensitivity20/100GB','network':'Within included plan transfer allowance, no public API consumers or inter-region transfer assumed','other_costs':'Managed monitoring, taxes, excess egress, new data collection, compliance operations and high availability excluded; local logs retained within disk budget','ops':'2 hours/month at USD20/hour is an illustrative labor assumption, not provider price','billing':'Stopping an allocated instance does not imply removing its bill; do not scale monthly fixed cost by Spark active minutes','capacity':'Mac results do not prove equivalent AWS performance. Benchmark chosen VM before production. Training and streaming scheduled separately to avoid resource competition.','scale_trigger':'Consider16GB only if a measured pilot shows persistent memory pressure or backlog; sustained streaming input exceeding processing throughput requires investigation before scaling. No proof of distributed scaling.','prices_sources':['https://docs.aws.amazon.com/lightsail/latest/userguide/amazon-lightsail-bundles.html','https://aws.amazon.com/lightsail/pricing/']},indent=2,ensure_ascii=False))
print(r[['group_variable','group_value','n','reference','SPD','DI','TPR_gap','reading']].to_string(index=False));print(pd.DataFrame(scenarios).query('snapshot_stored_gb==50').to_string(index=False))
