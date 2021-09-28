import re
import os
import unicodedata
from glob import glob
from tika import parser
import datetime
import pandas as pd 

start = datetime.datetime.now()

# 0. Vokabellisten laden

df_neg = pd.read_excel('C:/Users/test/Dropbox/HHU FACC Lehrstuhl SHK-WHB/Looser/Tone/BPW_Dictionary.xlsx', sheet_name='NEG_BPW', header= None)
df_pos = pd.read_excel('C:/Users/test/Dropbox/HHU FACC Lehrstuhl SHK-WHB/Looser/Tone/BPW_Dictionary.xlsx', sheet_name='POS_BPW', header= None)
df_unc = pd.read_excel('C:/Users/test/Dropbox/HHU FACC Lehrstuhl SHK-WHB/Looser/Tone/BPW_Dictionary.xlsx', sheet_name='UNC_BPW', header= None)

pos_list = []
neg_list = []
unc_list = []

for idx, row in df_neg.iterrows():
    neg_list.append(row[0].lower().strip())

for idx, row in df_pos.iterrows():
    pos_list.append(row[0].lower().strip())

for idx, row in df_unc.iterrows():
    unc_list.append(row[0].lower().strip())

# 0. ISIN-dict laden:
df = pd.read_excel('C:/Users/test/Dropbox/HHU FACC Lehrstuhl SHK-WHB/Looser/CDAX-Unternehmen CSR Report Informationen 2008-2018_DL.xlsx')
df = df.fillna(method='ffill')
isin_dict = {}
for index, row in df.iterrows():
    isin_dict[row['Company'].lower().strip()] = row['ISIN'].strip()

list_of_isin_comp_tuples = []

for i in isin_dict.keys():
    list_of_isin_comp_tuples.append((i, isin_dict[i]))

# 1. Pfad-Liste zur Bearbeitung erstellen
# Alle deutschen Dateipfade sammeln.
PATH = 'C:\\Users\\test\\sciebo\\FACC SHK-WHB Berichte' # PATH = 'C:\\Users\\test\\Documents\\GitHub\\hhu_whk_project_restatements\\test_data'
paths = [y for x in os.walk(PATH) for y in glob(os.path.join(x[0], '*.pdf'))]
paths_de = [pfad for pfad in paths if 'eng.' not in pfad.lower()]
paths_de_SR = [pfad for pfad in paths_de if not ('ar.' in pfad.lower()) | ('gb' in pfad.lower()) | ('ea.' in pfad.lower()) | ('_ar' in pfad.lower()) | ('abschlu' in pfad.lower()) | ('jb' in pfad.lower()) | ('annual' in pfad.lower())]

for pfad in paths:
    with open("alle_pfade.txt", mode="a", encoding="utf-8") as file:
        file.write(pfad + '\n')

for pfad in paths_de:
    with open("alle_pfade_de.txt", mode="a", encoding="utf-8") as file:
        file.write(pfad + '\n')

for pfad in paths_de_SR:
    with open("alle_pfade_SR_de.txt", mode="a", encoding="utf-8") as file:
        file.write(pfad + '\n')


# Davon die in eine Liste ablegen, die noch nicht bearbeitet wurden. 
ist_eingelesen = []

with open("eingelesen.txt", mode="r", encoding="utf-8") as eingelesen:
    for line in eingelesen:
        ist_eingelesen.append(line.strip())

nicht_eingelesen = [pfad for pfad in paths_de_SR if pfad not in ist_eingelesen]

# 2. Schleife initiieren

eingelesene_pfade  = 0
pfad_counter = 0

with open("eingelesen.txt", mode="r", encoding="utf-8") as file:
    for line in file:
        eingelesene_pfade += 1
if eingelesene_pfade >= pfad_counter:
    pfad_counter = eingelesene_pfade


for pfad in nicht_eingelesen:

    # file laden
    parsedPDF = parser.from_file(pfad, requestOptions={'timeout': 300}) 

    # Ausgabe-Variablen initiieren und ggf direkt füllen. 
    Company = os.path.basename(pfad).split('20')[0].lower().strip()

    if (Company == '') | (Company == ' '):

        for element in pfad.split('\\'):
            if ('AG' in element) | ('SE' in element):
                Company = element.lower().strip()

    # Company für spätere analyse ablegen
    companies = []
    with open('companies.txt', 'r+', encoding="utf-8") as file:

        for line in file:
            companies.append(line.strip())

        if Company not in companies:
            file.write(Company + '\n')


    Year = re.findall(r'\d{4}', pfad)[-1]
    SR = 0
    NFE = 0

    # SR wird ggf in der Meta-Daten-Loop weiter unten nochmal behandelt. 
    if ('SR' in pfad) | ('crb' in pfad.lower()) | ('nhb' in pfad.lower()) | ('nb' in pfad.lower()) | ('nachhaltigkeit' in pfad.lower()):
        SR = 1

    if ('nfb' in pfad.lower()) | ('nfe' in pfad.lower()) | ('nichtfinanziell' in pfad.lower()):
        NFE = 1
    
    ISIN = 'fehlt'
    for k,v in list_of_isin_comp_tuples:
        if Company in k:
            ISIN = v
    if ISIN == 'fehlt':
        for k,v in list_of_isin_comp_tuples:
            firm = Company.split(' ')[0]
            if firm in k:
                ISIN = v

    # Datum SRNFE:
    try:
        Date_SRNFE = parsedPDF['metadata']['Creation-Date'][:10]
        if Date_SRNFE == '':
            Date_SRNFE = parsedPDF['metadata']['created'][:10]
        if Date_SRNFE == '':
            Date_SRNFE = 'fehlt'
    except:
        Date_SRNFE = 'fehlt'

    report_size_SRNFE = 0 
    report_sentence_SRNFE = 0
    report_words_SRNFE = 0 
    Date_AR = 0
    report_size_AR = 0 
    report_sentence_AR = 0
    report_words_AR = 0 


    is_gri = 0                              # 1, wenn match mit " gri " oder "(gri)" irgendwo im Text ist. 
    page_number = 0                         # wo ist aktuell angezeigter Satz
    satz = 'a'                              # Platzhalter für einen Satz

    # Bericht als Liste von Seiten speichern.
    try:
        pages_raw = unicodedata.normalize("NFKD", parsedPDF['content']).strip().split('\n\n\n') # löst ein uni-encode-problem. Da stand vor jedem Wort "\xa0". Löst evtl. auch andere Problem mit komischen Fragmenten im Output.
        pages = [page for page in pages_raw if len(page)] # löscht alle leeren pages. 

        # report_size zuweisen
        report_size_SRNFE = len(pages)
        
        # SCHLEIFE FÜR META-DATEN SR:
        for page in pages: 

            # Spaltennamen von Tabellen standen immer hinter \n\n. Das durch Punkt ersetzt um Satzlänge künstlich zu kürzen. Tabellen werden jetzt nicht mehr als (wirre) Sätze gelesen. 
            text_SR = page.replace('\n\n', ".").replace('\n', "").replace('*','.').replace('..','.').replace('\t', "").replace(";", '').strip().lower()
            satz_liste_SR = text_SR.split('.')
            report_sentence_SRNFE += len(satz_liste_SR)

            # report_words aufaddieren
            report_words_SRNFE += len(text_SR.split(' '))

            # gri
            regex_gri = r'\W+gri\W|^gri\W*|global reporting initiative'
            for satz in satz_liste_SR:
                if re.search(regex_gri, satz):
                    is_gri = 1
                
            # SR
        for page in pages[:3]:
            # Spaltennamen von Tabellen standen immer hinter \n\n. Das durch Punkt ersetzt um Satzlänge künstlich zu kürzen. Tabellen werden jetzt nicht mehr als (wirre) Sätze gelesen. 
            text_SR = page.replace('\n\n', ".").replace('\n', "").replace('*','.').replace('..','.').replace('\t', "").replace(";", '').strip().lower()
            satz_liste_SR = text_SR.split('.')
            report_sentence_SRNFE += len(satz_liste_SR)

            if ('nachhaltigkeitsbericht' in satz_liste_SR) | ('sustainability report' in satz_liste_SR) | ('corporate responsibility' in satz_liste_SR):
                SR = 1


        if SR|NFE:  
            # AR zu SRNFE finden
            pfad_AR = 'a'
            for kandidat in paths_de:
                if ' ' in Company:
                    firm = Company.split(' ')[0]
                else: 
                    firm = Company

                if ((firm in kandidat.lower()) & (Year in kandidat) & ('ar' in kandidat.lower())) | ((firm in kandidat.lower()) & (Year in kandidat) & ('gb' in kandidat.lower())):
                    pfad_AR = kandidat

            # AR einlesen und Metadaten sammeln
            try:
                AR = parser.from_file(pfad_AR) 
                Date_AR = AR['metadata']['Creation-Date'][:10]
                if Date_AR == '':
                    Date_AR = AR['metadata']['created'][:10]
                if Date_AR == '':
                    Date_AR = 'fehlt'
                pages_raw_AR = unicodedata.normalize("NFKD", AR['content']).strip().split('\n\n\n') # löst ein uni-encode-problem. Da stand vor jedem Wort "\xa0". Löst evtl. auch andere Problem mit komischen Fragmenten im Output.
                pages_AR = [page for page in pages_raw_AR if len(page)] # löscht alle leeren pages. 
                report_size_AR = len(pages)



                # SCHLEIFE FÜR META-DATEN AR:
                for page in pages_AR: 

                    # Spaltennamen von Tabellen standen immer hinter \n\n. Das durch Punkt ersetzt um Satzlänge künstlich zu kürzen. Tabellen werden jetzt nicht mehr als (wirre) Sätze gelesen. 
                    text_AR = page.replace('\n\n', ".").replace('\n', "").replace('*','.').replace('..','.').replace('\t', "").replace(";", '').strip().lower()
                    satz_liste_AR = text_AR.split('.')
                    report_sentence_AR += len(satz_liste_AR)

                    # report_words aufaddieren
                    report_words_AR += len(text_AR.split(' '))

            except:
                Date_AR = 'fehlt'
                pages_raw_AR = 'fehlt'
                pages_AR = 'fehlt'
                report_size_AR = 'fehlt'
                report_sentence_AR = 'fehlt'
                report_words_AR = 'fehlt'


            # Ausgabe-Variablen initiieren:
            pos_words = 0
            neg_words = 0
            unc_words = 0
            
            # Seite für Seite durchgehen
            for page in pages: 
                page_number += 1
                print(u'Datei {} / {}. Seite {} / {}. Next Pfad: {}'.format(pfad_counter, len(paths_de_SR), page_number, report_size_SRNFE, nicht_eingelesen[pfad_counter +1]))

                # Spaltennamen von Tabellen standen immer hinter \n\n. Das durch Punkt ersetzt um Satzlänge künstlich zu kürzen. Tabellen werden jetzt nicht mehr als (wirre) Sätze gelesen. 
                text_SR = page.replace('\n\n', ".").replace('\n', "").replace('*','.').replace('..','.').replace('\t', "").replace(";", '').strip().lower()

                satz_liste_raw = text_SR.split('.')
                satz_liste = [satz.strip() for satz in satz_liste_raw if len(satz.strip()) > 4] 

                # Satzweise die gesamte KWL checken lassen. Wenn Match, dann Satz mit Nummer in Match_Liste ablegen
                for satz in satz_liste:
                    for word in satz.split(' '):
                        if word in pos_list:
                            pos_words += 1
                        if word in neg_list:
                            neg_words += 1
                        if word in unc_list:
                            unc_words += 1
            with open('output.csv', mode='a', encoding="utf-8") as file:
                file.write(u'{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{};{}\n'.format(Company, Year, pos_words, neg_words, unc_words, SR, NFE, ISIN, Date_SRNFE, Date_AR, report_size_SRNFE, report_sentence_SRNFE, report_words_SRNFE, is_gri, report_size_AR, report_sentence_AR, report_words_AR, pfad))
            with open("eingelesen.txt", mode="a", encoding="utf-8") as file:
                file.write(pfad + '\n')
    except:
        with open("fehlerhafte_dateien.txt", mode="a", encoding="utf-8") as file:
            file.write(pfad + '\n')
    pfad_counter +=1


stop = datetime.datetime.now()
print(u'\nBenötigte Zeit: {}\n'.format(stop - start))
