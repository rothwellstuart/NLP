
# coding: utf-8

# In[ ]:

###################################################################################

### Input parameters: file_in, file_out_suffix, date, lookup_file
### file_in: name of file to run process on
### file_out_suffix: short name identifier to append to file name: Hansard_v1_2_%s.txt
### date: date to apply to debate; appears as field in output dataset
###
###
### N.B. Location of MP lookup file and Stanfrod libraries are hard coded!!!
###
###
### Example of how to call:
### > python Hansard_process_v1_2.py "Motor Neurone Disease-Gordon Aikman 2017-02-20.txt" "motorneurone" "20/02/2017"

####################################################################################


### Imports

import nltk
from nltk.corpus import stopwords
import pandas as pd
import numpy as np
import re
from collections import Counter
import itertools



# Stanford entity extraction full run - code taken from Four_entity_process_stanford_NER_v1_0.ipynb

# Stanford imports
from nltk.tag.stanford import StanfordNERTagger
from nltk.tokenize.stanford import StanfordTokenizer


# Other imports
import os
import nltk
from collections import defaultdict


# NLTK imports
from nltk import pos_tag
from nltk.chunk import conlltags2tree
from nltk.tree import Tree

import tkinter
import operator
import sys

#####################################################################################



### Stanford paths ###

#Set core path for Stanford NLP packages
main_path = os.path.join("C:\\Users\\rothw\\Documents\\Python Scripts\\Python NLP\\StanfordNLP\\",
                         "stanford-corenlp-full-2016-10-31\\")
# Set paths where the Standford NLP .jar files are located
pathlist = [os.path.join(main_path,"stanford-corenlp-3.7.0"),
            os.path.join(main_path,"ner\\stanford-ner.jar"),
            os.path.join(main_path,"postagger\\stanford-postagger.jar")]
###            os.path.join(main_path,"parser\\stanford-parser.jar"),
###            os.path.join(main_path,"parser\\stanford-parser-3.6.0-models.jar"),
# Set path to Stanford models
mpath = [os.path.join(main_path,"postagger\\models"),
         os.path.join(main_path,"ner\\classifiers")]
# Set path to java.exe
javapath = "C:\\Program Files\\Java\\jre1.8.0_121\\bin\\java.exe"

# Add paths to the CLASSPATH environmental variable (as instructed by NLTK)
os.environ['CLASSPATH'] = os.pathsep.join(pathlist)
os.environ['STANFORD_MODELS'] = os.pathsep.join(mpath)
os.environ['JAVAHOME'] = javapath


############################################################################################



# Define function to tag NER sentence with BIO tags
def stanfordNE2BIO(tagged_sent):
    bio_tagged_sent = []
    prev_tag = "O"
    for token, tag in tagged_sent:
        if tag == "O": #O
            bio_tagged_sent.append((token, tag))
            prev_tag = tag
            continue
        if tag != "O" and prev_tag == "O": # Begin NE
            bio_tagged_sent.append((token, "B-"+tag))
            prev_tag = tag
        elif prev_tag != "O" and prev_tag == tag: # Inside NE
            bio_tagged_sent.append((token, "I-"+tag))
            prev_tag = tag
        elif prev_tag != "O" and prev_tag != tag: # Adjacent NE
            bio_tagged_sent.append((token, "B-"+tag))
            prev_tag = tag
    # Return BIO tagged sentence
    return bio_tagged_sent



########################################################################################


# Read in the lookup
import csv
with open('c:\\Users\\rothw\\Documents\\Python Scripts\\Python NLP\\mps.csv', 'r') as f:
    reader = csv.reader(f)
    mp_list = list(reader)


    
# Split imported lookup into MP name and Consituency

mp_names = []
mp_consts = []

for row in mp_list[1:]:
#    mp_names.append(list(itertools.chain.from_iterable(row[1:3])))
    temp = [row[1] + ' ' + row[2], 'MP']
    mp_names.append(temp)
    temp2 = [row[4], 'Constituency']
    mp_consts.append(temp2)
    

# Append the two lists into one, tagging the type
ents_lookup = mp_names + mp_consts

########################################################################################


# Main function


file_in=sys.argv[1]
file_out_suffix=sys.argv[2]
date=sys.argv[3]


# Read in file
sample = open(file_in, 'r', encoding='utf8').read()


# Get title (first line) of doc - processed for stopwords and normalised
title = sample.split('\n', 1)[0]

# Remove stopwords
title_trimmed = []

title_tokenized=nltk.word_tokenize(title)
title_trimmed = ' '.join([word for word in title_tokenized if word not in (stopwords.words('english'))])

print('\n Title (title_trimmed): \n')
print(title_trimmed)
    
    
# Cycle through the rows of the doc, to find MP names (speaking) and Constituency names (in the text)
## N.b. only take Constituency names if they are in the conversation, not in announcing the speaker


 # Split the whole text by newline
split_by_line = sample.split('\n')

ents_identified = []
speaker=''


for row in split_by_line:
    # only look for MPs, not Constituencies, in the speaker announcement
    if len(row)<80:
        for item in mp_names:
            if item[0] in row:
                speaker = item
                speaker_spoken = [speaker, ['Speaking']]
                ents_identified.append(speaker_spoken)

    # For all other rows, look for both MPs and Constituencies...
    else:
        for item in ents_lookup:
            if item[0] in row:
                speaker_spoken = [speaker, item]
                ents_identified.append(speaker_spoken)
            
        ### ... and (v1.2) Stanford extracted entities ###
        # Tokenize sentence with stanford NLP
        tkn_sent = StanfordTokenizer().tokenize(row)
        # Named entity tagging with stanford NLP
        tag_sent = StanfordNERTagger('english.conll.4class.distsim.crf.ser.gz').tag(tkn_sent) 
        # Apply BIO tags to the tagged sentence
        bio_tagged_sent = stanfordNE2BIO(tag_sent)
        # Collate BIO parts of entities together
        sent_tokens, sent_ne_tags = zip(*bio_tagged_sent)
        sent_pos_tags = [pos for token, pos in pos_tag(sent_tokens)]

        sent_conlltags = [(token, pos, ne) for token, pos, ne in zip(sent_tokens, sent_pos_tags, sent_ne_tags)]
        ne_tree = conlltags2tree(sent_conlltags)

    
        # Get entities from the trees
        # ne_in_sent = []
        for subtree in ne_tree:
            if type(subtree) == Tree: # If subtree is a noun chunk, i.e. NE != "O"
                ne_label = subtree.label()
                ne_string = " ".join([token for token, pos in subtree.leaves()])
                if ne_label in ['PERSON', 'LOCATION', 'ORGANIZATION']:
                    speaker_spoken = [speaker, [ne_string, ne_label]]
                    ents_identified.append(speaker_spoken)
        

# Flatten the rows in the list
flattened = []

for row in ents_identified:
    row_flattened = list(itertools.chain.from_iterable(row))
    flattened.append(row_flattened)
    
    
# Pull together into a dataframe
output_df = pd.DataFrame(flattened, columns=["Speaker_name", "Speaker_type", "Subject_name", "Subject_type"])
output_df['Debate'] = title_trimmed
output_df['Date'] = date

# Change column order
output_df = output_df[['Debate', 'Date', 'Speaker_name', 'Speaker_type', 'Subject_name', 'Subject_type']]

# Dedupe
output_dedup_df = output_df.drop_duplicates()        
    
    
# Export to file
output_dedup_df.to_csv('Hansard_v1_2_%s.txt' %file_out_suffix, sep=',', index=False) 
    
    

print('Hansard_process_v1_2 completed!')

