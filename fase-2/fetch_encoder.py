"""Descarga explícita gratuita del codificador fijado; no entrena ni usa API de pago."""
import json
from pathlib import Path
from huggingface_hub import snapshot_download
c=json.loads(Path('config.sample.json').read_text())
print(snapshot_download(c['encoder'],revision=c['encoder_revision'],allow_patterns=['*.json','*.safetensors','*.txt','1_Pooling/*']))
