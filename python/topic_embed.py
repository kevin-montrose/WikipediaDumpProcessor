from sentence_transformers import SentenceTransformer
import fileinput
import sys
from pathlib import Path

# 1. Load a pretrained Sentence Transformer model
model = SentenceTransformer("all-MiniLM-L6-v2")

sys.stdout.write("Ready\r\n")
sys.stdout.flush()

for filename in fileinput.input():
    filename = filename.strip()

    contents = Path(filename).read_text(encoding='utf-8')
    
    embedding = model.encode(contents)
    sys.stdout.write(str(embedding))
    sys.stdout.flush()
    sys.stdout.write("\r\n")
    sys.stdout.flush()
