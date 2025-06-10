import re


#######################################################################################
#######################################################################################
#######################################################################################

def return_longest(elem):
    temp = []
    for x in elem:
        if type(x) != list and type(x) != tuple:
            temp.append(x)
        else:
            temp.extend(list(x))
    return max(temp, key=len)


def clean_text(text):
    text = re.sub('-\s?[0-9]+\s?-', '', text)
    text = re.sub('</?\w+>', '', text)
    text = re.sub('\xa0', ' ', text)

    text = re.sub('table of contents?', ' ', text, flags=re.I).strip()
    text = re.sub('\t', ' ', text).strip()
    text = re.sub('\n+', '\n', text).strip()

    return text.strip()


#######################################################################################
#######################################################################################
#######################################################################################

def extract_date(filename):
    regex = '^CONFORMED PERIOD OF REPORT:([0-9\s]+)$'
    count = 0
    try:
        with open(filename, encoding='utf-8') as f:
            for line in f:
                if 'CONFORMED PERIOD OF REPORT' in line:
                    return filename, re.findall(regex, line, flags=re.I | re.DOTALL | re.M)[0].strip()
                    break
                elif count == 20:
                    return filename, 'No Date'
                    break
                count += 1
    except:
        return filename, 'Error'


#######################################################################################

def extract_sections(text):
    text = clean_text(text)

    # Attempt to clean the table of content, just in case
    item1 = r'item[^a-zA-Z\d]*1\.?[^a-zA-Z\d]*business(\s?description)?'
    item1a = r'item[^a-zA-Z\d]*1a\.?[^a-zA-Z\d]*risk\sfactors'
    item1b = r'item[^a-zA-Z\d]*1b\.?'
    between = r'[0-9\s\.]*'
    delete = '(' + item1 + between + item1a + between + item1b + ')'
    text = re.sub(delete, ' ', text, flags=re.I)

    # Either starts with beginning of the line or with Part I?
    new_section_starts = r'(^[0-9]{0,3}|^part\si[^a-zA-Z\d]{0,10})i(\s)?t(\s)?e(\s)?m[^a-zA-Z\d]*'

    description_regex_start = new_section_starts + r'1[^a-zA-Z\d]'
    risk_regex_start = new_section_starts + r'1[\.\s]?a[^a-zA-Z\d]'
    staff_regex_start = new_section_starts + r'1[\.\s]?b[^a-zA-Z\d]'
    properties_regex_start = new_section_starts + r'2[^a-zA-Z\d]'

    regex_middle = '.*?'

    description_regex_old = '(' + description_regex_start + regex_middle + properties_regex_start + ')'
    description_regex_new = '(' + description_regex_start + regex_middle + risk_regex_start + ')'

    risk_regex = '(' + risk_regex_start + regex_middle + staff_regex_start + ')'
    risk_regex_no_1b = '(' + risk_regex_start + regex_middle + properties_regex_start + ')'

    # Note that there can be a 1a in old forms: don't use it. Plus, may not be a 1b
    out = []
    for regex in [description_regex_old, description_regex_new, risk_regex, risk_regex_no_1b]:
        sections = re.findall(regex, text, flags=re.I | re.DOTALL | re.M)
        if sections:
            # Return the maximum lenght you got:
            section = return_longest(sections)
            out.append(section)
        else:
            out.append("")

    # Output:
    if out[1] != '':
        business = out[1]
    else:
        business = out[0]
    if out[2] != '':
        risk = out[2]
    else:
        risk = out[3]

    return business, risk


#######################################################################################

def extract_MDA(text):
    text = clean_text(text)

    # Attempt to clean the table of content, just in case
    item7 = 'item[^a-zA-Z\d]*7\.?[^a-zA-Z\d]*management'
    item7A = 'item[^a-zA-Z\d]*7a\.?[^a-zA-Z\d]*quantitative'  # See Nike 10K 2019
    item8 = 'item[^a-zA-Z\d]*8\.?[^a-zA-Z\d]*financial'
    between = '[0-9\s\.\-\,]*'
    delete = f'({item7}{between}{item7A}{between}{item8})'
    text = re.sub(delete, ' ', text, flags=re.I)

    # Either starts with beginning of the line or with Part I?
    new_section_starts = '(^[0-9]{0,3}|^part\si[^a-zA-Z\d]{0,10})i(\s)?t(\s)?e(\s)?m[^a-zA-Z\d]*'

    MDA_regex_start = new_section_starts + '7[^a-zA-Z\d]'
    quantitative_regex_start = new_section_starts + '7[\.\s]?a[^a-zA-Z\d]'
    financial_regex_start = new_section_starts + '8[^a-zA-Z\d]'

    regex_middle = '.*?'

    MDA_Regex_1 = f'({MDA_regex_start}{regex_middle}{quantitative_regex_start})'  # With 7A
    MDA_Regex_2 = f'({MDA_regex_start}{regex_middle}{financial_regex_start})'  # No 7A

    # Note that there can be a 1a in old forms: don't use it. Plus, may not be a 1b
    out = []
    for regex in [MDA_Regex_1, MDA_Regex_2]:
        sections = re.findall(regex, text, flags=re.I | re.DOTALL | re.M)
        print(len(sections))
        if sections != []:
            # Return the maximum lenght you got:
            section = return_longest(sections)
            out.append(section)
        else:
            out.append("")

    # Output:
    if out[0] != '':
        MDA = out[0]
    else:
        MDA = out[1]

    return MDA


#######################################################################################
#######################################################################################
#######################################################################################


