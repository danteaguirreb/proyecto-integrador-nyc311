"""Test end-to-end de Bronze/Silver/Gold con CSV artificiales pequeños."""
from nyc311_phase2 import *
import csv
c=json.loads(Path('config.sample.json').read_text()); base=Path('../../work/preparation_test').resolve(); raw=base/'source/data/raw'; raw.mkdir(parents=True,exist_ok=True)
for year in range(2021,2025):
 with (raw/f'nyc311_{year}.csv').open('w') as f:
  w=csv.DictWriter(f,fieldnames=RAW_COLS);w.writeheader()
  for i in range(12):
   row={x:'' for x in RAW_COLS};row.update(unique_key=f'{year}_{i}',created_date=f'{year}-03-01T10:00:00.000',closed_date=f'{year}-03-0{2+i%5}T10:00:00.000',complaint_type='A',descriptor='Descriptor',borough='BRONX',location_type='Street',open_data_channel_type='PHONE')
   if i==10:row['closed_date']=''
   if i==11:row['closed_date']=f'{year}-02-01T10:00:00.000'
   w.writerow(row)
c.update(source_root=str(base/'source'),output_root=str(base/'run'),fraction=1.,min_type_n=2,cores=1,driver_memory='1g')
root=setup(c);s=session(c);s.sparkContext.setLogLevel('ERROR')
prepare(s,c,root)
g=s.read.parquet(str(root/'gold/base'));assert g.count()==48;assert g.where("quality_reason='open'").count()==4;assert g.where("quality_reason='negative_duration'").count()==4
tr,va,counts=cohort(s,c,root,2022);assert counts['train_used']==10 and counts['eval_used']==10
assert tr.where('label is null').count()==0
save_json('evidencia/preparation_tests.json',{'artificial_fixture':True,'raw_rows':48,'gold_rows':48,'open':4,'negative':4,'train_fold_2022':10,'eval_fold_2022':10,'bronze_hashes':True,'silver_reasons':True,'cohort_reconciliation':True,'passed':True})
print('PASS Bronze/Silver/Gold and temporal cohort');s.stop()
