"""NYC311: Kafka real -> ventanas de 1 h, watermark 2 h -> Parquet append.
Reproduce un día completo; cambia orden de emisión, nunca el tiempo del evento.
"""
from pathlib import Path
import argparse,json,time,os,sys,uuid,threading
import pandas as pd
import psutil
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient,NewTopic
from pyspark.sql import SparkSession,functions as F,types as T

def main():
 p=argparse.ArgumentParser();p.add_argument('--root',default=str(Path(__file__).resolve().parent));p.add_argument('--jars',required=True);p.add_argument('--bootstrap',default='127.0.0.1:19092');p.add_argument('--run-name',default='stream_run');a=p.parse_args()
 B=Path(a.root);R=B/a.run_name;R.mkdir(parents=True,exist_ok=False);E=R/'evidence';E.mkdir();topic='nyc311-'+uuid.uuid4().hex[:12]
 admin=KafkaAdminClient(bootstrap_servers=a.bootstrap);admin.create_topics([NewTopic(topic,num_partitions=3,replication_factor=1)]);admin.close()
 os.environ['PYSPARK_PYTHON']=sys.executable
 t0=time.perf_counter();s=(SparkSession.builder.appName('NYC311-Fase3').master('local[2]').config('spark.driver.memory','2g').config('spark.sql.shuffle.partitions','4').config('spark.sql.session.timeZone','America/New_York').config('spark.ui.enabled','false').config('spark.ui.showConsoleProgress','false').config('spark.jars',','.join(str(p) for p in Path(a.jars).glob('*.jar'))).getOrCreate());s.sparkContext.setLogLevel('ERROR');startup=time.perf_counter()-t0
 schema=T.StructType([T.StructField('event_id',T.StringType()),T.StructField('event_ts',T.StringType()),T.StructField('borough',T.StringType()),T.StructField('emission_hour',T.IntegerType()),T.StructField('delay_kind',T.StringType())])
 events=(s.readStream.format('kafka').option('kafka.bootstrap.servers',a.bootstrap).option('subscribe',topic).option('startingOffsets','earliest').option('maxOffsetsPerTrigger',50000).option('failOnDataLoss','true').load().select(F.from_json(F.col('value').cast('string'),schema).alias('e')).select('e.*').withColumn('event_time',F.to_timestamp('event_ts')))
 windows=(events.withWatermark('event_time','2 hours').groupBy(F.window('event_time','1 hour'),'borough').agg(F.count('*').alias('requests'),F.sum(F.when(F.col('delay_kind')=='mild',1).otherwise(0)).alias('mild'),F.sum(F.when(F.col('delay_kind')=='extreme',1).otherwise(0)).alias('extreme')).select(F.date_format('window.start','yyyy-MM-dd HH:mm:ss').alias('window_start'),F.date_format('window.end','yyyy-MM-dd HH:mm:ss').alias('window_end'),'borough','requests','mild','extreme'))
 def start():return windows.writeStream.format('parquet').outputMode('append').option('path',str(R/'windows')).option('checkpointLocation',str(R/'checkpoint')).trigger(processingTime='1 second').start()
 q=start();producer=KafkaProducer(bootstrap_servers=a.bootstrap,value_serializer=lambda x:json.dumps(x).encode(),key_serializer=lambda x:x.encode(),acks='all',linger_ms=5)
 data=[json.loads(x) for x in (B/'data/replay_events.jsonl').read_text().splitlines()];df=pd.DataFrame(data);progress={};timings=[];mem=[];halt=threading.Event();proc=psutil.Process()
 def monitor():
  while not halt.wait(.5):
   total=0
   for child in [proc]+proc.children(recursive=True):
    try:total+=child.memory_info().rss
    except psutil.Error:pass
   mem.append({'elapsed_s':time.perf_counter()-starttime,'python_spark_rss_bytes':total})
 def capture():
  for x in q.recentProgress:progress[int(x['batchId'])]=x
 def count_output():
  return s.read.parquet(str(R/'windows')).count() if list((R/'windows').glob('part-*.parquet')) else 0
 starttime=time.perf_counter();thread=threading.Thread(target=monitor,daemon=True);thread.start();restart={}
 for h,batch in df.groupby('emission_hour',sort=True):
  t=time.perf_counter()
  for event in batch.to_dict('records'):producer.send(topic,key=event['borough'],value=event)
  producer.flush();published=time.perf_counter()-t;q.processAllAvailable();capture();elapsed=time.perf_counter()-t
  timings.append({'emission_hour':int(h),'messages':len(batch),'publish_seconds':published,'complete_seconds':elapsed});print(json.dumps(timings[-1]),flush=True)
  if h==12:
   before=count_output();q.stop();q=start();q.processAllAvailable();capture();after=count_output();assert before==after
   restart={'checkpoint_reused':True,'output_rows_before':before,'output_rows_after':after,'duplicate_rows_on_restart':after-before}
 q.processAllAvailable();capture();total_seconds=time.perf_counter()-starttime;q.stop();producer.close();halt.set();thread.join()
 result=s.read.parquet(str(R/'windows')).where(F.col('window_start').startswith('2024-01-01')).toPandas().sort_values(['window_start','borough']).reset_index(drop=True)
 # Batch oracle: all Jan1 input minus the one deliberately expired message.
 accepted=df[(df.delay_kind!='extreme')&(df.delay_kind!='frontier')].copy();accepted['window_start']=pd.to_datetime(accepted.event_ts,utc=True).dt.tz_convert('America/New_York').dt.floor('h').dt.strftime('%Y-%m-%d %H:%M:%S')
 expected=accepted.groupby(['window_start','borough']).agg(requests=('event_id','size'),mild=('delay_kind',lambda x:int(x.eq('mild').sum()))).reset_index().sort_values(['window_start','borough']).reset_index(drop=True)
 pd.testing.assert_frame_equal(result[['window_start','borough','requests','mild']],expected,check_dtype=False)
 assert not result.duplicated(['window_start','borough']).any();assert int(result.mild.sum())==1 and int(result.extreme.sum())==0
 prog=list(progress.values());dropped=sum(int(op.get('numRowsDroppedByWatermark',0)) for x in prog for op in x.get('stateOperators',[]));input_n=sum(int(x['numInputRows']) for x in prog);assert dropped==1,(dropped,prog[-1]);assert input_n==len(df),(input_n,len(df))
 result.to_csv(E/'windows.csv',index=False);pd.DataFrame(timings).to_csv(E/'timings.csv',index=False);pd.DataFrame(mem).to_csv(E/'resources.csv',index=False);(E/'progress.json').write_text(json.dumps(prog,indent=2));expected.to_csv(E/'batch_oracle.csv',index=False)
 baseline=pd.read_csv(B/'data/baseline_2023.csv');alerts=result.merge(baseline,on='borough',how='left',validate='many_to_one');alerts['volume_alert']=(alerts.requests>2*alerts.baseline_hourly)&(alerts.requests>=20);alerts.to_csv(E/'volume_alerts.csv',index=False)
 summary={'engine':'Kafka 3.7.1 + Spark '+s.version,'topic':topic,'bootstrap':a.bootstrap,'partitions':3,'replication_factor':1,'cores':2,'driver_memory':'2g','window':'1 hour','watermark':'2 hours','messages_sent':len(df),'messages_consumed':input_n,'jan1_expected':len(df)-1,'jan1_retained':int(result.requests.sum()),'late_rows_dropped':dropped,'mild_late_retained':int(result.mild.sum()),'finalized_window_borough_rows':len(result),'oracle_exact_match':True,'restart':restart,'spark_startup_seconds':startup,'replay_wall_seconds':total_seconds,'effective_messages_per_second':len(df)/total_seconds,'max_python_spark_rss_bytes':max(x['python_spark_rss_bytes'] for x in mem),'max_state_rows':max((op.get('numRowsTotal',0) for x in prog for op in x.get('stateOperators',[])),default=0),'final_state_rows':sum(op.get('numRowsTotal',0) for op in prog[-1].get('stateOperators',[])),'final_watermark':prog[-1].get('eventTime',{}).get('watermark'),'volume_alert_windows':int(alerts.volume_alert.sum()),'frontier_note':'One real next-day request remains in state; Jan1 windows are complete. Watermark advances with event time, not wall clock.','memory_note':'Sampled Python+Spark RSS sum, excludes standalone Kafka, not unique physical memory.','scope_note':'Full Jan1 replay; not the full four-year streaming workload. All fairness test counts use the complete Phase2 test.'}
 (E/'summary.json').write_text(json.dumps(summary,indent=2));s.stop();print(json.dumps(summary,indent=2),flush=True)
if __name__=='__main__':main()
