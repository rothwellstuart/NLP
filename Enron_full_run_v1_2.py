

# ### Full run - Loop through all files

######################################################################

# Imports

import os, re, arrow, email, csv, json, fnmatch, csv
from collections import Counter
import pandas as pd

# Import spacy and English models
import spacy, numpy

# Load English Spacy module
nlp = spacy.load('en')

######################################################################



# Reset output lists to empty
emails_ref_list=[]
subjects_list=[]
people_list=[]
entities_list=[]
edges_from_to_list=[]
edges_subject_entity_list=[]
edges_subject_target_list=[]
emails_not_processed=[]
urn = 0


# Switch for whether to only use sent directories or not
sent_only="N"


# Specify target entities or phrases to find
targets = ['dynegy', 'apache', 'steele', 'cochise', 'chewco', 'whitewing', 'death star', 'deathstar', 'shorty', 'get shorty', 'getshorty']

# allen-p sent subdirectory took <1 min to run
# allen-p directory took c. 10 mins to run
# rootdir = 'c:\\Users\\rothw\\Documents\\Enron database\\maildir\\allen-p\\sent'


### *** PICK UP HERE - NEED TO MODIFY THE FILE LOOP TO PICK UP SUB-DIRECTORIES ***
rootdir = 'c:\\Users\\rothw\\Documents\\Enron database\\maildir'

for subdir, dirs, files in os.walk(rootdir):
    for file in files:
        
        # URN records unique email processed
        urn += 1
        
        # Handle the sent only switch to select the right subdirectories
        if (not (sent_only =="Y" or sent_only =="y")) or ("sent" in subdir):
            filepath = os.path.join(subdir, file)
            # print("dirs= ", dirs, " -- subdir= ", subdir, " -- file=", file, " -- filepath= ", filepath)
            
            with open(filepath) as f:
                print("Processing: ", filepath)
                # Create email message object from file
                message = email.message_from_file(f)

            # print("Message: ", message)

            # Get message From, To's and Subject        
            message_from = message['from']
            message_to = message['to']
            message_subject = message['subject']
            # Tidy up the message subject and remove "Re: " if its there
            message_subject_handled = re.sub('\s+',' ',str(message_subject).rstrip())
            message_subject_handled = re.sub('Re: ', '', message_subject_handled)
            message_subject_handled = re.sub('RE: ', '', message_subject_handled)
            message_subject_handled = re.sub('FW: ', '', message_subject_handled)

            message_date = message['date']

            # Log message date, from and subject against the urn
            emails_ref_list.append([urn, filepath, message_date, message_from, message_subject_handled])

            # Get message body
            if message.is_multipart():
                for part in message.walk():
                    ctype = part.get_content_type()
                    cdispo = str(part.get('Content-Disposition'))

                    # skip any text/plain (txt) attachments
                    if ctype == 'text/plain' and 'attachment' not in cdispo:
                        message_body = part.get_payload(decode=True)  # decode
                        break
            else:
                message_body = message.get_payload()



            # Split To's into a list (if exist) and store in a dataset
            # N.b. Should only ever be 1 Subject and 1 From, but can be many To's
            if message_to:
                message_to_list = re.sub('\s+','',message_to.rstrip()).split(',')

                # Run entity extraction on email body
                message_body_nlpd = nlp(message_body)

                # Ony retain entities of types we are interested in
                message_body_nlpd.ents = [ent for ent in message_body_nlpd.ents if ent.label_ in ('PERSON', 'FACILITY', 'ORG', 'GPE', 'LOC', 'PRODUCT')]


                # Store Subject, From and To's and extracted entities in a DataFrame
                # N.b. Should only ever be 1 Subject and 1 From, but can be many To's

                # Store Subject, From's, To's and Extracted Entities in reference lists
                if  message_subject_handled not in subjects_list:
                    subjects_list.append( message_subject_handled)
                if message_from not in people_list:
                    people_list.append(message_from) 


                # Loop on people sent To
                for to_item in message_to_list:
                    # Store in referemce list
                    if to_item not in people_list:
                        people_list.append(to_item)
                    # Record relationships - From -> To
                    # edges_from_to_list.append([urn, message_date, message_from, to_item]) 
                    edges_from_to_list.append([urn, to_item]) 

                # Loop on Entitites Extracted
                for ent in message_body_nlpd.ents:
                    ent_entry = [re.sub('\s+',' ',str(ent).rstrip()), ent.label_]
                    # Store in referemce list
                    if ent_entry not in entities_list:
                        entities_list.append(ent_entry)
                    # Record relationships - Email Containds Entity
                    # edges_subject_entity_list.append([urn, message_date, message_from, ent, ent.label_])
                    edges_subject_entity_list.append([urn, ent, ent.label_])
                    
                # Loop on target entities or phrases to find    
                for target in targets:
                    if target in message_subject_handled.lower() or target in message_body.lower():
                        # edges_subject_target_list.append([urn, message_date, message_from, target])
                        edges_subject_target_list.append([urn, target])

            # If no message_to then log file in emails_not_processed list
            else:
                emails_not_processed.append([urn, filepath])

            
#############################################################################
            
# Volume checks
print("emails_ref_list = ", len(emails_ref_list))
print("subjects_list = ", len(subjects_list))
print("people_list = ", len(people_list))
print("entities_list = ", len(entities_list))
print("edges_from_to_list = ", len(edges_from_to_list))
print("edges_subject_entity_list = ", len(edges_subject_entity_list))
print("edges_subject_target_list = ", len(edges_subject_target_list))
print("emails_not_processed = ", len(emails_not_processed))

##############################################################################



### Output to CSVs ###



# Email Reference List
pd.DataFrame(emails_ref_list).to_csv("c:\\Users\\rothw\\Documents\\Python Scripts\\Python NLP\\Enron data output\\enron_emails_reference.csv"
                                   , index=False, header=False)


# Email Subjects
pd.DataFrame(subjects_list).to_csv("c:\\Users\\rothw\\Documents\\Python Scripts\\Python NLP\\Enron data output\\enron_subjects.csv"
                                   , index=False, header=False)

    
# People (who either sent or received emails)
pd.DataFrame(people_list).to_csv("c:\\Users\\rothw\\Documents\\Python Scripts\\Python NLP\\Enron data output\\enron_people.csv"
                                   , index=False, header=False)


# Extracted Entities
with open("c:\\Users\\rothw\\Documents\\Python Scripts\\Python NLP\\Enron data output\\enron_extracted_entities.csv", 'w', newline="") as outfile3:
    writer = csv.writer(outfile3)
    writer.writerows(entities_list)


# Edges From -> To 
with open("c:\\Users\\rothw\\Documents\\Python Scripts\\Python NLP\\Enron data output\\enron_edges_from_to.csv", 'w', newline="") as outfile4:
    writer = csv.writer(outfile4)
    writer.writerows(edges_from_to_list)

    
# Edges Subject -> Entity mentioned 
with open("c:\\Users\\rothw\\Documents\\Python Scripts\\Python NLP\\Enron data output\\enron_edges_subject_entity.csv", 'w', newline="") as outfile5:
    writer = csv.writer(outfile5)
    writer.writerows(edges_subject_entity_list)


# Edges Subject -> Target mentioned 
with open("c:\\Users\\rothw\\Documents\\Python Scripts\\Python NLP\\Enron data output\\enron_edges_subject_target.csv", 'w', newline="") as outfile6:
    writer = csv.writer(outfile6)
    writer.writerows(edges_subject_target_list)

    
# Emails not processed (did not have a To person)
pd.DataFrame(emails_not_processed).to_csv("c:\\Users\\rothw\\Documents\\Python Scripts\\Python NLP\\Enron data output\\emails_not_processed.csv"
                                   , index=False, header=False)


########################################################################################



