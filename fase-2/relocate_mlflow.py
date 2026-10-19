"""Reubica las URI de artefactos locales de la copia entregada de MLflow."""
from pathlib import Path
import sqlite3,argparse
p=argparse.ArgumentParser();p.add_argument('--run',default='run_sample');a=p.parse_args()
root=Path(a.run).resolve();db=root/'mlflow.db'
if not db.exists():raise FileNotFoundError(db)
with sqlite3.connect(db) as con:
 for exp_id,old in con.execute('SELECT experiment_id,artifact_location FROM experiments').fetchall():
  if not old or not old.endswith('/mlartifacts'):continue
  new=(root/'mlartifacts').as_uri()
  if old==new:continue
  con.execute('UPDATE experiments SET artifact_location=? WHERE experiment_id=?',(new,exp_id))
  for run_id,uri in con.execute('SELECT run_uuid,artifact_uri FROM runs WHERE experiment_id=?',(exp_id,)).fetchall():
   if uri.startswith(old+'/'):con.execute('UPDATE runs SET artifact_uri=? WHERE run_uuid=?',(new+uri[len(old):],run_id))
print('URI de artefactos locales verificadas:',root)
