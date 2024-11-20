from scipy.special import softmax
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
import torch
import fileinput
import sys
from pathlib import Path

def get_t5_logprob(prompt):
    input_ids = tokenizer(prompt, return_tensors="pt").input_ids
    
    outputs = model.generate(
        input_ids,
        output_scores=True,
        return_dict_in_generate=True
    )
    
    result = int(tokenizer.decode(outputs.sequences[0], skip_special_tokens=True))
    final_score = float('nan')

    if result == 0:
        assert outputs.scores[1].argmax() == 632, "Wrong 0 token index"

    elif result == 1:
        assert outputs.scores[0].argmax() == 209, "Wrong 1 token index"

    else:
        raise Exception

    contra = outputs.scores[1][0][632].item()
    entail = outputs.scores[0][0][209].item()

    return softmax([contra, entail])
    
tokenizer = AutoTokenizer.from_pretrained("google/t5_xxl_true_nli_mixture")

model = AutoModelForSeq2SeqLM.from_pretrained("google/t5_xxl_true_nli_mixture", low_cpu_mem_usage=True)

sys.stdout.write("Ready\r\n")
sys.stdout.flush()

for filename in fileinput.input():
    filename = filename.strip()

    sys.stderr.write('reading ===> ' + filename + '\r\n')

    contents = Path(filename).read_text(encoding='utf-8')
    
    index = 0
    premise = ''
    hypothesis = ''
    for line in contents.splitlines():
        if index == 0:
            premise = line
            index = 1
        else:
            hypothesis = line
            index = 0
            
            txt = 'premise: ' + premise + ' hypothesis: ' + hypothesis
            
            sys.stderr.write('classifying ===> ' + txt + '\r\n')
            
            probs = get_t5_logprob(txt)
            
            sys.stdout.write(str(probs))
            sys.stdout.flush()
            sys.stdout.write("\r\n")
            sys.stdout.flush()