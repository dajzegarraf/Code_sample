#----------------------------------------------------------------------------------
# Project: Reading historical papers
# Version: 1.0
# Author: David J. Zegarra
#----------------------------------------------------------------------------------
#----------------------------------------------------------------------------------

import pandas as pd
import numpy as np
import os

import pdfminer
from pdfminer.high_level import extract_text

import nltk
from nltk.corpus import stopwords

import re
from unidecode import unidecode

project_dir = "path/to/project"
os.makedirs(project_dir, exist_ok=True)

down_direc= r"D:\2024-1\puc24\data\input" 
save_direc= r"D:\2024-1\puc24\data\output"
#----------------------------------------------------------------------------------
# Code to read PDF
# Specify the file path of the PDF file
file_pdfs = r"D:\2024-1\puc24\literatura"
# Specify the file path of the PDF file
pdf_file_path = file_pdfs + "/HSAA_CH31_35_36.pdf"

# Extract the text from the PDF
text = extract_text(pdf_file_path)

# Split the text into paragraphs based on newline characters
paragraphs = text.split("\n\n")

# Remove empty paragraphs
paragraphs = [p for p in paragraphs if p.strip()]

#paragraphs
df_00 = pd.DataFrame({"paragraphs": paragraphs})

#----------------------------------------------------------------------
# Stopwords
nltk.download('stopwords')
stopwords_list = stopwords.words('english')
#---------------------------------------------------------------------------
# Aplying the cleaning function to the dataframe
df = df_00
def clean_data(df, column_name, new_para ):
    df[column_name] = df[column_name].astype(str)
    df[new_para] = df[column_name].apply(lambda x: unidecode(x)).str.lower()
    df[new_para] = df[new_para].apply(lambda x: re.sub(r'\d+', '', x))
    df[new_para] = df[new_para].apply(lambda x: ' '.join([word for word in x.split() if word not in stopwords_list]))
    df[new_para] = df[new_para].str.replace('- ', '')
    df[new_para] = df[new_para].apply(lambda x: re.sub(r'[^\w\s]', '', x))
    df[new_para] = df[new_para].apply(lambda x: re.sub(r'\s+', ' ', x))
    return df
 
df = clean_data(df, 'paragraphs', "new_para")

#--------------------------------------------------------------------------------------
# Add column with the chapter number --------------------------------------
df['chapter'] = df['paragraphs'].where(df['new_para'] == 'chapter')
df['chapter'] = df['chapter'].fillna(method='ffill')

# Drop References  --------------------------------------
# Group the dataframe by the values in the "new_col" column
grouped_df = df.groupby('chapter')

# Iterate over each group and create a separate dataframe for each group
separated_dfs = [group for _, group in grouped_df]

# Initialize an empty list to store the dataframes
dataframes_list = []
for df2 in separated_dfs:
    # Find the position of the observation "references" in the column "new_para"
    df2=df2.reset_index(drop=True)
    position_references = df2[df2['new_para'] == 'references'].index[0]

    # Drop all the observations after the position of "references"
    df2.drop(df2.index[position_references+1:], inplace=True)
       
    # Append the dataframe to the list
    dataframes_list.insert(0, df2)

# Concatenate the dataframes vertically
combined_df = pd.concat(dataframes_list, ignore_index=True)
#--------------------------------------------------------------------------------------
# Add page number --------------------------------------

combined_df['paragraphs_v2'] = combined_df['paragraphs'].str.replace(r'\s+', '', regex=True)
combined_df['page'] = combined_df['paragraphs_v2'].where(combined_df['paragraphs_v2'].str.isnumeric(), np.nan)
combined_df['page'] = combined_df['page'].fillna(method='ffill')
combined_df['page'] = combined_df['page'].fillna(method='bfill')

# to keep the original dataframe
combined_df = combined_df.drop(columns=['paragraphs_v2'])

#drop rows with words
word_list2 = ['chapter', 'references', 'introduction','conclusion']  
# Replace with your list of words
for word in word_list2:
    combined_df = combined_df[~combined_df['new_para'].str.contains(word)]

#drop duplicates and the original with no information
duplicates_mask = combined_df.duplicated(subset=['new_para'], keep=False)

# Invert the mask to keep only the non-duplicate rows
filtered_df = combined_df[~duplicates_mask]
# ------------------------------------------------
df = filtered_df
# Tokenize each observation in the 'new_para' column
import nltk
nltk.download('punkt')
# Tokenize each observation in the 'new_para' column
df['tokenized_obs'] = df['new_para'].apply(lambda x: nltk.word_tokenize(x))

#--------------------------------------------------------------------------------
# Specify the file path of the Excel file
file_laura = down_direc + "\sites_desc_laura.xlsx"

# Read the Excel file into a dataframe
df2 = pd.read_excel(file_laura, sheet_name='roots')

# Form a list from the observations in the "Translation" column
translation_list = df2["root"].tolist()

new_words = ['money', 'fiat', 'market', 'exchange', 'trade', 'gold', 'silver', 'bronze', 'store', 'copper']
concatenated_list = translation_list + new_words

ylist = concatenated_list  # Replace with your list of words

df['found_words'] = df['tokenized_obs'].apply(lambda x: [word for word in x if word in ylist])

df['period_words'] = df.apply(lambda row: [word for word in period_list if word in row['new_para']] if row['found_words'] else [], axis=1)
df['site_name_words'] = df.apply(lambda row: [word for word in site_name_list if word in row['new_para']] if row['found_words'] else [], axis=1)
df['culture_words'] = df.apply(lambda row: [word for word in culture_list if word in row['new_para']] if row['found_words'] else [], axis=1)

df['period_words'] = df['period_words'].apply(lambda x: list(set(x)))
df['site_name_words'] = df['site_name_words'].apply(lambda x: list(set(x)))
df['culture_words'] = df['culture_words'].apply(lambda x: list(set(x)))

#--------------------------------------------------------------------------------
#--------------------------------------------------------------------------------
# Creating dummies for the words in the list
word_list = ['centraliz', 'administrat', 'govern', 'capital', 'regional center', 'provinc', 'district', 'jurisdiction', 'tax', 'rule', 'law', 'war', 'warfare', 'warrior', 'political', 'stateless', 'state', 'chief', 'paramount chiefdom', 'band', 'local community', 'bureaucra', 'audience', 'audiencia', 'office', 'court', 'monument', 'mound', 'platform', 'complexes', 'complex mound', 'complex platform', 'palace', 'pyramid', 'temple', 'plaza', 'patio', 'citadel', 'public', 'public center', 'public building', 
             'urban', 'market', 'civic', 'complex settlement', 'compact settlement', 'halmet', 'village', 'town', 'city', 'llacta', 'llaqta', 'nucleated', 'sedentary', 'semi-sedentary', 'semisedentary', 'nomadic', 'semi-nomadic',
               'seminomadic', 'residen', 'habitation', 'domestic', 'hous', 'leader', 'authority', 'headman', 'elite', 'royal', 'aristocracy', 'dynasty', 'ceremon', 'ritual', 'religious', 'god', 'huaca', 'shrine', 'agricultur', 'farm', 'cultivat', 'agrarian', 'terrace', 'plow', 'plant', 'crop', 'grain', 'seed', 'maize', 'cereal', 'quinoa', 'quinua', 'kiwicha', 'kañiwa', 'tuber', 'potato', 'squash', 'peanut', 
               'vegetable', 'tree fruits', 'cotton', 'horticulture', 'storage', 'warehouse', 'storeroom', 'qollqa', 'colca', 'qullqa', 'fish', 'hunt', 'gather', 'pastoral', 'cattle', 'grazing', 'pasture', 'llama', 'alpaca', 'alpaqa', 'vicuña', 'guanaco', 'camelid', 'sheep', 'domestic animal', 'livestock', 'ceramic', 'pottery', 'textile', 'weave', 'weaving', 'metal', 'leather', 'craft', 'manufacture', 'occupation', 'specialization', 'stone', 'adobe', 'walls', 'walled', 'fort', 'hillfort', 'defensive', 
               'pukara', 'pucara', 'puccara', 'militar', 'cave', 'tambo', 'tampu', 'burial', 'mortuary', 'cemetery', 'funerary', 'tomb', 'chullpa', 'irrigation', 'canal', 'road', 'money', 'fiat', 'market', 'exchange', 'trade', 'gold', 'silver', 'bronze', 'store']

#centraliz	administrat	govern	capital	regional center	provinc	district	jurisdiction	tax	rule	law	war (no warfare ni warrior)	warfare	warrior	political	stateless	state	chief	paramount chiefdom	band	local community	bureaucra	audience	audiencia	office	court	monument	mound	platform	complexes	complex mound	complex platform	palace	pyramid	temple	plaza	patio	citadel	public	public center	public building	urban	market	civic	complex settlement	compact settlement	halmet	village	town	city	llacta	llaqta	nucleated	sedentary	semi-sedentary	semisedentary	nomadic	semi-nomadic	seminomadic	residen	habitation	domestic	hous	leader	authority	headman	elite	royal	aristocracy	dynasty	ceremon	ritual	religious	god	huaca	shrine	agricultur	farm	cultivat	agrarian	terrace	plow	plant	crop	grain	seed	maize	cereal	quinoa	quinua	kiwicha	kañiwa	tuber	potato	squash	peanut	vegetable	tree fruits	cotton	horticulture	storage	warehouse	storeroom	qollqa	colca	qullqa	fish	hunt	gather	pastoral	cattle	grazing	pasture	llama	alpaca	alpaqa	vicuña	guanaco	camelid	sheep	domestic animal	livestock	ceramic	pottery	textile	weave	weaving	metal	leather	craft	manufacture	occupation	specialization	stone	adobe	walls	walled	fort	hillfort	defensive	pukara	pucara	puccara	militar	cave	tambo	tampu	burial	mortuary	cemetery	funerary	tomb	chullpa	irrigation	canal	road
for word in word_list:
    df[word] = df['found_words'].apply(lambda x: 1 if word in x else 0)

#--------------------------------------------------------------------------------
#--------------------------------------------------------------------------------
#Export to excel file
df = filtered_df 
df = df.applymap(lambda x: x.lstrip() if isinstance(x, str) else x)

output_file1 = save_direc + "\output1_4.xlsx"
df.to_excel( output_file1, index=False)
df