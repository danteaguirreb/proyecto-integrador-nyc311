from pathlib import Path
import os,tarfile,urllib.request,subprocess,time,socket,hashlib,json
import argparse
p=argparse.ArgumentParser();p.add_argument('--runtime',required=True);a=p.parse_args()
W=Path(a.runtime).resolve();W.mkdir(parents=True,exist_ok=True);j=W/'jars';j.mkdir(exist_ok=True)
coords=[('org/apache/spark','spark-sql-kafka-0-10_2.12','3.5.4'),('org/apache/spark','spark-token-provider-kafka-0-10_2.12','3.5.4'),('org/apache/kafka','kafka-clients','3.4.1'),('org/apache/commons','commons-pool2','2.11.1')]
manifest=[]
for group,name,version in coords:
 url=f'https://repo.maven.apache.org/maven2/{group}/{name}/{version}/{name}-{version}.jar';dest=j/f'{name}-{version}.jar'
 if not dest.exists():urllib.request.urlretrieve(url,dest)
 manifest.append({'url':url,'file':dest.name,'sha256':hashlib.sha256(dest.read_bytes()).hexdigest()})
(W/'jars_manifest.json').write_text(json.dumps(manifest,indent=2))
archive=W/'kafka_2.13-3.7.1.tgz'
if not archive.exists():
 url='https://archive.apache.org/dist/kafka/3.7.1/kafka_2.13-3.7.1.tgz'
 urllib.request.urlretrieve(url,archive)
 if archive.stat().st_size<50000000:raise RuntimeError('Kafka download incomplete')
k=W/'kafka_2.13-3.7.1'
if not k.exists():
 with tarfile.open(W/'kafka_2.13-3.7.1.tgz') as t:t.extractall(W,filter='data')
config=W/'kafka.properties';config.write_text(f'''process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:19093
listeners=PLAINTEXT://127.0.0.1:19092,CONTROLLER://127.0.0.1:19093
advertised.listeners=PLAINTEXT://127.0.0.1:19092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs={W}/kafka-data
num.partitions=3
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
log.retention.hours=24
log.segment.bytes=16777216
''')
env=dict(os.environ,JAVA_HOME=os.environ['JAVA_HOME'],KAFKA_HEAP_OPTS='-Xms256m -Xmx512m',LOG_DIR=str(W/'kafka-server-logs'))
if not (W/'kafka-data/meta.properties').exists():
 ident=subprocess.check_output([str(k/'bin/kafka-storage.sh'),'random-uuid'],env=env,text=True).strip();subprocess.run([str(k/'bin/kafka-storage.sh'),'format','-t',ident,'-c',str(config)],env=env,check=True)
try:
 with socket.create_connection(('127.0.0.1',19092),timeout=1):raise RuntimeError('Port already occupied; inspect before starting an isolated broker')
except OSError:pass
f=(W/'broker.log').open('w');p=subprocess.Popen([str(k/'bin/kafka-server-start.sh'),str(config)],env=env,stdout=f,stderr=subprocess.STDOUT,start_new_session=True);(W/'broker.pid').write_text(str(p.pid))
for _ in range(60):
 if p.poll() is not None:raise RuntimeError('Kafka stopped: '+(W/'broker.log').read_text()[-1500:])
 try:
  with socket.create_connection(('127.0.0.1',19092),timeout=1):print('KAFKA OK',p.pid,flush=True);break
 except OSError:time.sleep(1)
else:raise RuntimeError('Kafka timeout')
