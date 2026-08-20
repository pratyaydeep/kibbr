#!/usr/bin/env python3
"""Regenerate the multi-MB hostile inputs deleted to save disk. Run from repo root."""
import os
d = "testdata/hostile"
G1 = b'{"event":"start","id":1}\n'; G2 = b'{"event":"end","id":2}\n'
def w(n, mid): open(os.path.join(d, n), "wb").write(G1 + mid + G2)

w("bigline.ndjson",    b'{"pad":"' + b'x'*(10*1024*1024) + b'"}\n')          # 10 MB line
w("nest200k.ndjson",   b'{"a":'*200000 + b'1' + b'}'*200000 + b'\n')          # 200k object levels
w("arr100k.ndjson",    b'{"a":' + b'['*100000 + b']'*100000 + b'}\n')         # 100k array levels
w("manykeys.ndjson",   b'{' + b','.join(b'"k%d":%d'%(i,i) for i in range(100000)) + b'}\n')
w("hugekey.ndjson",    b'{"' + b'k'*1000000 + b'":1}\n')                      # 1 MB key name
w("escape_bomb.ndjson",b'{"a":"' + b'\\u0041'*500000 + b'"}\n')               # 500k escapes
print("regenerated 6 large inputs in", d)
