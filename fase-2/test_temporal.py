"""Pruebas unitarias con casos construidos; NO son resultados NYC311."""
from nyc311_phase2 import *

def main():
 c={'cores':1,'driver_memory':'1g','partitions':2,'min_type_n':2}
 s=session(c); s.sparkContext.setLogLevel('ERROR')
 rows=[('1','2021-01-01','2021-01-02','A',1.,'eligible_duration'),('2','2021-02-01','2021-02-02','A',2.,'eligible_duration'),('3','2021-03-01','2021-03-02','A',3.,'eligible_duration'),('4','2021-04-01','2021-04-02','A',4.,'eligible_duration'),('late','2021-12-31','2022-01-02','A',999.,'eligible_duration'),('open','2021-05-01',None,'A',None,'open'),('negative','2021-05-01','2021-04-01','A',-1.,'negative_duration'),('boundary','2021-12-31','2022-01-01','A',900.,'eligible_duration')]
 df=s.createDataFrame(rows,'unique_key string, created_ts string, closed_ts string, complaint_type string, resolution_hours double, quality_reason string')
 df=df.withColumn('created_ts',F.to_timestamp('created_ts')).withColumn('closed_ts',F.to_timestamp('closed_ts'))
 train=eligible(df,'2021-01-01','2022-01-01','2022-01-01')
 assert {x.unique_key for x in train.collect()}=={'1','2','3','4'}
 th=historical_thresholds(train,c); assert th.first().p75==3.
 labeled=label_with(train,th); assert labeled.where('label=1').count()==1
 future=s.createDataFrame([('x','NEW',2.),('y','A',3.),('z','A',4.)],'unique_key string, complaint_type string, resolution_hours double')
 labels={r.unique_key:r.label for r in label_with(future,th).collect()}; assert labels=={'x':None,'y':0.,'z':1.}
 small=historical_thresholds(train.where("unique_key='1'"),c); assert not small.first().threshold_valid
 # Entradas distintas no deben perderse al codificar categorías desconocidas.
 rows2=[]
 for i in range(40): rows2.append((str(i),float(i%2),'A' if i%3 else None,'PHONE','BRONX','Street',*[float(i%5)]*6))
 tr=s.createDataFrame(rows2,'unique_key string, label double, complaint_type string, open_data_channel_type string, borough string, location_type string, '+', '.join(x+' double' for x in NUMS))
 model=pipeline('E1',{'regParam':.01,'elasticNetParam':0.}).fit(tr)
 va=tr.withColumn('complaint_type',F.lit('UNKNOWN')); assert model.transform(va).count()==40
 result={'eligibility_and_cutoff':True,'strict_percentile_comparison':True,'open_not_negative':True,'unknown_type_no_label':True,'rare_type_abstention':True,'unseen_feature_no_row_loss':True,'synthetic_test_rows':8,'note':'Casos de borde artificiales; no métricas del proyecto'}
 save_json('evidencia/unit_tests.json',result); print(json.dumps(result)); s.stop()
if __name__=='__main__': main()
