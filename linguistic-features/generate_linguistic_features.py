import tarfile
import csv
import os.path
import json
import re
from bz2 import BZ2File
from urllib import request
from io import BytesIO
import collections as C
import pickle
import numpy as np

import nltk
from collections import Counter
import textstat # pip install textstat
from lexicalrichness import LexicalRichness # pip install lexicalrichness
import math, string, sys, fileinput
from nltk.corpus import stopwords 
from nltk.tokenize import word_tokenize 
from timeit import default_timer as timer

nltk.download('stopwords')
nltk.download('punkt')

# import utils.bigquery as BQ

fname = "cmv.tar.bz2"
url = "https://chenhaot.com/data/cmv/" + fname

# Instantiate output file
g = open('linguistic_results.csv', 'w')
gwriter = csv.writer(g)
gwriter.writerow([
    'op',
    'post_id',
    # 'post_text',
    'challenger',
    'comment_id',
    # 'challenger_text',
    'delta_awarded',
    'num_words',
    'definite_article_count', 'indefinite_article_count', 'frac_definite_articles',
    'positive_word_count', 'negative_word_count', 'frac_pos_words',
    'first_person_count', 'first_person_plural_count', 'second_person_count',
    'num_links',
    # 'urls', 'urls_com', 'urls_pdf', 'urls_edu', 'frac_com_urls', 
    'num_question_marks',
    'num_quotations',
    'num_examples',
    'valence',
    'arousal',
    'dominance',
    'flesch_kincaid_grade',
    'sentence_count',
    'type_token_ratio',
    'paragraph_count',
    'word_entropy',
    'num_bold',
    'num_italics',
    'numbered_words',
    'num_common_words_all', 'reply_frac_all', 'OP_frac_all', 'jaccard_all', 'num_common_words_stop', 'reply_frac_stop', 
        'OP_frac_stop', 'jaccard_stop', 'num_common_words_content', 'reply_frac_content', 'OP_frac_content', 'jaccard_content'  
])

# download if not exists
if not os.path.isfile(fname):
    f = BytesIO()
    with request.urlopen(url) as resp, open(fname, 'wb') as f_disk:
        data = resp.read()
        f_disk.write(data)  # save to disk too
        f.write(data)
        f.seek(0)
else:
    f = open(fname, 'rb')

print('Opening tar file...')
tar = tarfile.open(fileobj=f, mode="r")


# EXTRACT AND DESERIALIZE THE RAW JSON FILE

# Extract the file we are interested in

train_fname = "pair_task/train_pair_data.jsonlist.bz2"
# test_fname = "pair_task/heldout_pair_data.jsonlist.bz2"

print('Extracting tar file...')
train_bzlist = tar.extractfile(train_fname)

# Deserialize the JSON list
print('Deserializing JSON list...')
original_posts_train = [
    json.loads(line.decode('utf-8'))
    for line in BZ2File(train_bzlist)
]
print(len(original_posts_train))

op_ids_seen = set()



# DEFINE LINGUISTIC FEATURE EXTRACTORS

# Number of words
def get_num_words(input_string):
    return len(input_string.split())

### WORD CATEGORY BASED FEATURES

# Get definite('the') and indefinite('a', 'an') articles 
def get_num_articles(input_string):
    input_string = input_string.lower()
    count_the = sum(1 for _ in re.finditer(r'\b%s\b' % re.escape('the'), input_string))
    count_a = sum(1 for _ in re.finditer(r'\b%s\b' % re.escape('a'), input_string))
    count_an = sum(1 for _ in re.finditer(r'\b%s\b' % re.escape('an'), input_string))
    count_some = sum(1 for _ in re.finditer(r'\b%s\b' % re.escape('some'), input_string))
    definite_article_count = count_the
    indefinite_article_count = count_a+count_an+count_some
    if definite_article_count+indefinite_article_count == 0:
        return definite_article_count, indefinite_article_count, 0
    else:
        frac_definite_articles = definite_article_count/(definite_article_count+indefinite_article_count)
    return definite_article_count, indefinite_article_count, frac_definite_articles


# Get positive and negative words (positive and negative bags obtained from LIWC paper - 
# J. W. Pennebaker, M. E. Francis, and R. J. Booth. Linguisticinquiry and word count: LIWC 2007. Technical report, 2007.) 
'''  
f_neg_path = './LIWC/liu-negative-words.txt'
f_pos_path = './LIWC/liu-positive-words.txt'
with open(f_pos_path) as f:
    positive_words_list = f.readlines()
positive_words_list = [x.strip() for x in positive_words_list] 

with open(f_neg_path) as f:
    negative_words_list = f.readlines()
negative_words_list = [x.strip() for x in negative_words_list] 
'''
def get_num_positive_words(input_string, positive_words_list, negative_words_list):
    positive_word_count = len(list((Counter(input_string) & Counter(positive_words_list)).elements()))
    negative_word_count = len(list((Counter(input_string) & Counter(negative_words_list)).elements()))
    if positive_word_count+negative_word_count == 0:
        return positive_word_count, negative_word_count, 0
    else:
        frac_pos_words = positive_word_count/(positive_word_count+negative_word_count)
    return positive_word_count, negative_word_count, frac_pos_words

# Get 1st person and 2nd person pronouns 
def get_pronouns(input_string):
    input_string = input_string.lower()
    
    first_person_pronouns = re.compile(r'\bI\b | \bme\b | \bmy\b | \bmine\b', flags=re.I | re.X)
    first_person_plural_pronouns = re.compile(r'\bwe\b | \bus\b | \bour\b | \bours\b', flags=re.I | re.X)
    second_person_pronouns = re.compile(r'\byou\b | \byour\b | \byours\b ', flags=re.I | re.X)
    
    first_person_count = first_person_pronouns.findall(input_string)
    first_person_plural_count = first_person_plural_pronouns.findall(input_string)
    second_person_count = second_person_pronouns.findall(input_string)

    return len(first_person_count), len(first_person_plural_count), len(second_person_count)

# Get number of links in comments
def get_num_links(input_string): 
    urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', input_string) 
    return len(urls) 

# Get number of .com links in comments
def get_num_com_links(input_string): 
    urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', input_string) 
    urls_com = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+.com', input_string) 
    urls_pdf = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+.pdf', input_string) 
    urls_edu = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+.edu', input_string) 
    if len(urls) == 0:
        return len(urls), len(urls_com), len(urls_pdf), len(urls_edu), 0
    frac_com_urls = len(urls_com)/len(urls)
    return len(urls), len(urls_com), len(urls_pdf), len(urls_edu), frac_com_urls 

# Number of '?'
def get_num_question_marks(input_string):
    return input_string.count("?")

# Number of " "
def get_num_quotations(input_string):
    matches=re.findall(r'\"(.+?)\"', input_string)
    return len(matches)

# Number of examples
def get_num_examples(input_string):
    input_string = input_string.lower()
    return input_string.count("for example") + input_string.count("for instance") + input_string.count("e.g")

## WORD SCORE BASED FEATURES - 
'''
path = './Arousal_Valence_Dominance.csv'

with open(path) as csvfile:
    readCSV = csv.reader(csvfile, delimiter=',')
    next(readCSV, None)  # skip the headers
    valence = {}
    arousal = {}
    dominance = {}
    for row in readCSV:
        word = row[0]
        valence[word]   = float(row[1])
        arousal[word]   = float(row[2])
        dominance[word] = float(row[3])   

'''

# TODO: scaling between [0, 1] before training
def get_valence(input_string, valence):
    word_regex = r'\b\w+\b'
    words = re.findall(word_regex, input_string)
    total_valence = 0
    for word in words:
        if word in valence.keys():
            total_valence += valence[word]
            
    return total_valence

def get_arousal(input_string, arousal):
    word_regex = r'\b\w+\b'
    words = re.findall(word_regex, input_string)
    total_arousal = 0
    for word in words:
        if word in arousal.keys():
            total_arousal += arousal[word]
            
    return total_arousal
    
def get_dominance(input_string, dominance):
    word_regex = r'\b\w+\b'
    words = re.findall(word_regex, input_string)
    total_dominance = 0
    for word in words:
        if word in arousal.keys():
            total_dominance += dominance[word]
            
    return total_dominance


## ENTIRE ARGUMENT FEATURES -

# Flesch-Kincaid grade levels 
def get_flesch_kincaid_grade_level(input_string):
    return textstat.flesch_kincaid_grade(input_string)

def get_sentence_count(input_string):
    return textstat.sentence_count(input_string)

def get_type_token_ratio(input_string):
    lex = LexicalRichness(input_string)
    return lex.ttr

def get_paragraph_count(input_string):
    lines = input_string.split('\n')
    count = 0
    if not lines == '':
        count += 1
    return count

def range_bytes (): return range(256)
def range_printable(): return (ord(c) for c in string.printable)
def get_word_entropy(data, iterator=range_bytes):
    if not data:
        return 0
    entropy = 0
    for x in iterator():
        p_x = float(data.count(chr(x)))/len(data)
        if p_x > 0:
            entropy += - p_x*math.log(p_x, 2)
    return entropy


# MARKDOWN FORMATTING FEATURES -

def get_num_bold(input_string):
    matches=re.findall(r'[<b>][\w\W]*[<\/b>]', input_string)
    return len(matches)

def get_num_italics(input_string):
    matches=re.findall(r'[<i>][\w\W]*[<\/i>]', input_string)
    return len(matches)

def get_numbered_words(input_string):
    matches=re.findall(r'[\w\W]*[0-9][\w\W]*', input_string)
    return len(matches)

# TODO: Check the format of bullet in the data
def get_num_bullet_list(input_string):
    matches=re.findall(r'[\u2022,\u2023,\u25E6,\u2043,\u2219]\s\d\.\s[A-z]', input_string)
    return len(matches)



## INTERPLAY FEATURES - 

def interplay_feats(original_post, argument):

    stop_words = set(stopwords.words('english')) 
    word_regex = r'\b\w+\b'
    
    all_words_OP = re.findall(word_regex, original_post)
    stop_words_OP = [w for w in all_words_OP if w in stop_words]
    content_words_OP = [w for w in all_words_OP if not w in stop_words] 
    
    all_words_A = re.findall(word_regex, original_post)
    stop_words_A = [w for w in all_words_A if w in stop_words]
    content_words_A = [w for w in all_words_A if not w in stop_words]
    
    num_common_words_all, reply_frac_all, OP_frac_all, jaccard_all = set_intersections(all_words_OP, all_words_A)
    num_common_words_stop, reply_frac_stop, OP_frac_stop, jaccard_stop = set_intersections(stop_words_OP, stop_words_A)
    num_common_words_content, reply_frac_content, OP_frac_content, jaccard_content = set_intersections(content_words_OP, content_words_A)
    return [num_common_words_all, reply_frac_all, OP_frac_all, jaccard_all, num_common_words_stop, reply_frac_stop, 
        OP_frac_stop, jaccard_stop, num_common_words_content, reply_frac_content, OP_frac_content, jaccard_content]

def set_intersections(O, A):
    O = set(O)
    A = set(A)
    num_common_words = len(list(set(O)&set(A)))
    if len(A) == 0:
        reply_frac = 0
    else:
        reply_frac = num_common_words / len(A)
        
    if len(O) == 0:
        OP_frac = 0
    else:
        OP_frac = num_common_words / len(O)
    
    if num_common_words == 0 or len(list(set(O)|set(A))) == 0:
        jaccard = 0
    else:
        jaccard = num_common_words/ len(list(set(O)|set(A)))
    
    return num_common_words, reply_frac, OP_frac, jaccard


# LINGUISTIC FEATURE WRAPPER FUNCTION

all_funcs = [
    get_num_words,
    get_num_articles,
    get_num_positive_words,
    get_pronouns,
    get_num_links,
    # get_num_com_links,
    get_num_question_marks,
    get_num_quotations,
    get_num_examples,
    get_valence,
    get_arousal,
    get_dominance,
    get_flesch_kincaid_grade_level,
    get_sentence_count,
    get_type_token_ratio,
    get_paragraph_count,
    get_word_entropy,
    get_num_bold,
    get_num_italics,
    get_numbered_words,
    interplay_feats
]

# READ IN POS/NEG WORD LOOKUPS
f_neg_path = './lookups/LIWC/liu-negative-words.txt'
f_pos_path = './lookups/LIWC/liu-positive-words.txt'
with open(f_pos_path) as f:
    positive_words_list = f.readlines()
positive_words_list = [x.strip() for x in positive_words_list] 

with open(f_neg_path) as f:
    negative_words_list = f.readlines()
negative_words_list = [x.strip() for x in negative_words_list] 

# READ IN AROUSAL VALENCE DOMINANCE
path = './lookups/Arousal_Valence_Dominance.csv'
with open(path) as csvfile:
    readCSV = csv.reader(csvfile, delimiter=',')
    next(readCSV, None)  # skip the headers
    valence = {}
    arousal = {}
    dominance = {}
    for row in readCSV:
        word = row[0]
        valence[word]   = float(row[1])
        arousal[word]   = float(row[2])
        dominance[word] = float(row[3]) 


def gen_all_ling_features(op_text, comment_text, funcs):
    line_ling_features = list()
    run_times = dict()

    for f in funcs:
        func_name = f.__name__
        start = timer()
        
        if func_name == 'get_num_positive_words':
            line_ling_features.extend([x for x in f(comment_text, positive_words_list, negative_words_list)])
        
        elif func_name == 'get_valence':
            line_ling_features.append(f(comment_text, valence))
        
        elif func_name == 'get_arousal':
            line_ling_features.append(f(comment_text, arousal))
        
        elif func_name == 'get_dominance':
            line_ling_features.append(f(comment_text, dominance))
        
        elif func_name == 'interplay_feats':
            line_ling_features.extend([x for x in f(op_text, comment_text)])
        
        else:
            line_features = f(comment_text)
            if type(line_features) is tuple:
                line_ling_features.extend(list(line_features))
            else:
                line_ling_features.append(line_features)
            
        end = timer()

    
    return line_ling_features


# ITERATE THROUGH DATA LINE BY LINE
# GENERATE LINGUISTIC FEATURES
# WRITE TO OUTPUT FILE
t = 0

for post in original_posts_train:
    print("Generating from line "+str(t)+" ...")
    start = timer()
    output_line = list()

    op = post['op_author']
    output_line.append(op)
    
    if op == '[deleted]':
        continue

    link_id = post['op_name'][3:]
    output_line.append(link_id)
    post_text = post['op_text']
    print("len(post_text):", len(post_text))
    # output_line.append(post_text)

    common_output = output_line.copy() # common output fields for both pos and neg

    if link_id in op_ids_seen:
        continue
    
    op_ids_seen.add(link_id)

    #  write posts

    # write neg awarded
    author = post['negative']['author']
    comment_id = post['negative']['comments'][0]['id']
    if author == '[deleted]':
        continue

    if author != '[deleted]':
        neg_text = post['negative']['comments'][0]['body']
        print("len(neg_text):", len(neg_text))

        output_line.append(author)
        output_line.append(comment_id)
        # output_line.append(neg_text)
        output_line.append(0)
        output_line.extend(gen_all_ling_features(post_text, neg_text, all_funcs))

        gwriter.writerow(output_line)
        output_line = common_output
        pass
    
    
    # write pos awarded
    author = post['positive']['author']
    comment_id = post['positive']['comments'][0]['id']
    if author == '[deleted]':
        continue

    if author != '[deleted]':    
        pos_text = post['positive']['comments'][0]['body']
        print("len(pos_text):", len(pos_text))

        output_line.append(author)
        output_line.append(comment_id)
        # output_line.append(pos_text)
        output_line.append(1)
        output_line.extend(gen_all_ling_features(post_text, pos_text, all_funcs))
        
        gwriter.writerow(output_line)
        output_line = common_output
        pass
    
    t += 1
    # if t == 10:
    #     break
    end = timer()
    print(end - start)

f.close()