"""Write a public compatibility fixture from one successful measured encoder."""
import argparse
import hashlib
import json
import struct
from pathlib import Path
from small_embedding_metrics import normalize


def vector_hash(vectors):
    flat=[float(x) for vector in vectors for x in vector]
    return hashlib.sha256(struct.pack('<'+'f'*len(flat),*flat)).hexdigest()


def main():
    p=argparse.ArgumentParser();p.add_argument('result',type=Path);p.add_argument('destination',type=Path);a=p.parse_args()
    result=json.loads(a.result.read_text());assert result['status']=='success'
    c=result['compatibility'];assert len(c['strings'])==10
    dimension={'e5':384,'gemma':768,'potion':256}[result['candidate']]
    assert c['contract']['dimension']==dimension
    for vector in c['vectors']+c['document_vectors']:
        assert len(vector)==dimension
        assert max(abs(a-b) for a,b in zip(vector,normalize(vector)))<1e-6
    manifest=result['model']
    output={'schema_version':1,'candidate':result['candidate'],'runtime':result['runtime'],'model':manifest['repo'],'revision':manifest['revision'],'file_hashes':{f['path']:f['sha256'] for f in manifest['files'] if f['path'].endswith(('.onnx','.onnx_data','tokenizer.json'))},'contract':c['contract'],'reference_batch_size':1,'normalization':'L2; float32 little-endian for reference byte hashes','comparison':c['comparison'],'query_vector_sha256':vector_hash(c['vectors']),'document_vector_sha256':vector_hash(c['document_vectors']),'cases':[]}
    assert output['query_vector_sha256']==c['float32_sha256']
    for i,text in enumerate(c['strings']):
        case={'text':text,'query_vector':c['vectors'][i],'document_title':'none','document_vector':c['document_vectors'][i]}
        if 'query_token_ids' in c:case.update(query_token_ids=c['query_token_ids'][i],document_token_ids=c['document_token_ids'][i])
        output['cases'].append(case)
    # Keep each vector on one line for readable fixture diffs.
    cases=output.pop('cases');head=json.dumps(output,ensure_ascii=False,indent=2)
    a.destination.write_text(head[:-2]+',\n  "cases": [\n'+',\n'.join('    '+json.dumps(case,ensure_ascii=False,separators=(',',':')) for case in cases)+'\n  ]\n}\n')
    assert json.loads(a.destination.read_text())['cases']==cases
    print(a.destination)

if __name__=='__main__':main()
