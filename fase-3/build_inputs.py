from pathlib import Path
import argparse,json,hashlib,shutil,calendar
import pyarrow.dataset as ds
import pandas as pd
p=argparse.ArgumentParser();p.add_argument('--silver',required=True);p.add_argument('--output',default=str(Path(__file__).resolve().parent));p.add_argument('--phase2-run',default=str(Path(__file__).resolve().parent/'phase2/run_complete'));a=p.parse_args()
B=Path(a.output);(B/'data').mkdir(parents=True,exist_ok=True);(B/'evidence').mkdir(exist_ok=True)
# Spark parquet timestamps are UTC instants, displayed below in NYC local time.
d=ds.dataset(a.silver,format='parquet',partitioning='hive')
start=pd.Timestamp('2024-01-01',tz='America/New_York').tz_convert('UTC').tz_localize(None)
end=pd.Timestamp('2024-01-03',tz='America/New_York').tz_convert('UTC').tz_localize(None)
t=d.to_table(columns=['unique_key','created_ts','borough'],filter=(ds.field('created_ts')>=start.to_pydatetime())&(ds.field('created_ts')<end.to_pydatetime())).to_pandas()
t['event_ts']=pd.to_datetime(t.created_ts,utc=True).dt.tz_convert('America/New_York');t=t.sort_values(['event_ts','unique_key']);day=t[t.event_ts.dt.strftime('%Y-%m-%d').eq('2024-01-01')].copy()
assert day.unique_key.is_unique and len(day)>1000
base_hour=day.event_ts.dt.hour;day['emission_hour']=base_hour;day['delay_kind']='on_time'
early=day[base_hour.eq(0)].index[0];mild=day[base_hour.eq(6)].index[0]
day.loc[early,['emission_hour','delay_kind']]=[8,'extreme'];day.loc[mild,['emission_hour','delay_kind']]=[7,'mild']
# A real request at >=03:00 next day advances watermark past the end of Jan1.
frontier=t[(t.event_ts.dt.strftime('%Y-%m-%d')=='2024-01-02')&(t.event_ts.dt.hour>=3)].head(1).copy();assert len(frontier)==1
frontier['emission_hour']=27;frontier['delay_kind']='frontier'
events=pd.concat([day,frontier]).sort_values(['emission_hour','event_ts','unique_key'])
records=[]
for r in events.itertuples():records.append({'event_id':str(r.unique_key),'event_ts':r.event_ts.isoformat(),'borough':r.borough if pd.notna(r.borough) else '[MISSING]','emission_hour':int(r.emission_hour),'delay_kind':r.delay_kind})
with (B/'data/replay_events.jsonl').open('w') as f:
 for row in records:f.write(json.dumps(row)+'\n')
meta={'source':str(Path(a.silver).resolve()),'day':'2024-01-01','day_rows':len(day),'total_messages':len(records),'delayed_mild':str(day.loc[mild,'unique_key']),'delayed_extreme':str(day.loc[early,'unique_key']),'frontier_rows':1,'timestamps_changed':False,'sampled_within_day':False,'selection':'all requests created on Jan1, plus one real next-day event to close Jan1 windows','sha256':hashlib.sha256((B/'data/replay_events.jsonl').read_bytes()).hexdigest()}
(B/'evidence/replay_input.json').write_text(json.dumps(meta,indent=2))
P=Path(a.phase2_run)
monthly=pd.read_csv(P/'evidence/real_queries/monthly_borough_local2.csv');monthly=monthly[monthly.created_month.str.startswith('2023')].copy();monthly['hours']=monthly.created_month.map(lambda x:calendar.monthrange(2023,int(x[-2:]))[1]*24);monthly['hourly']=monthly.requests/monthly.hours
baseline=monthly.groupby('borough').hourly.mean().reset_index(name='baseline_hourly');baseline.to_csv(B/'data/baseline_2023.csv',index=False)
for name in ['test_subgroups.csv','final_result.json','final_mlflow_metrics.json','quality_summary.json','experiment_summary.csv']:
 shutil.copy2(P/'evidence'/name,B/'evidence'/name)
shutil.copy2(P/'evidence/real_queries/summary.csv',B/'evidence/batch_query_benchmark.csv')
print(json.dumps(meta,indent=2),flush=True)
