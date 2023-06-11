import os

from hapaxes_1tM import remove_tei_lines_from_text


def get_project_name():
    with open('./.current_project', 'r') as current_project_file:
        return current_project_file.readline().strip()

def getListOfFiles(dirName):
    filelist = []
    
    for root, dirs, files in os.walk(dirName):
        for file in files:
            if (file == "./projects/{dirName}/splits/SampleFiles/.DS_Store"):
                pass
            if file.endswith(('npy','compressed','.DS_Store')):
                pass
            else:
                filelist.append(os.path.join(root,file))

    return(filelist)

def getCountOfFiles(dirName):
    filelist = []
    
    for root, dirs, files in os.walk(dirName):
        for file in files:
            if (file == f"./projects/{dirName}/splits/SampleFiles/.DS_Store"):
                pass
            if file.endswith(('npy','compressed','.DS_Store')):
                pass
            else:
                filelist.append(os.path.join(root,file))

    return len(filelist)

def get_dir_lengths_for_processing():
    project_name = get_project_name()
    counts_dict = {}
    for root, dirs, files in os.walk(f'./projects/{project_name}/splits/'):
        for dir in dirs:
            counts_dict[dir] = len(os.listdir(f'./projects/{project_name}/splits/{dir}'))
    return counts_dict

def get_word_count_for_text(text):
    the_text = remove_tei_lines_from_text(text)
    words = the_text.split()

    length_file = len(words)
    return length_file

def get_author_from_tei_header(line):
    line = line.split('<author>')[1]
    line = line.split('</author>')[0]
    ###NOTE: This fix would remove the garbarge from the Eltec headers that come from sequence aligns,
    ###      but this causes other problems. For the love of God, standardize this data!!!
    if '(' in line:
         line = line.split('(')[0].strip()
    ###NOTE: These alignments <persNames> occasionally have \n in them.  Just. Kill. Me.
    line = line.strip()
    reconstituted_line = ""
    for sub in line:
        reconstituted_line += sub.replace('\n', '')
    return reconstituted_line

def fix_the_gd_author_name_from_aligns(name):
    ###NOTE: This fix removes the garbarge from the Eltec headers that come from sequence aligns,
    ###      but this causes other problems. For the love of God, standardize this data!!!
    if '(' in name:
         name = name.split('(')[0].strip()
    ###NOTE: These alignments <persNames> occasionally have \n in them.  Just. Kill. Me.
    name = name.strip()
    reconstituted_line = ""
    for sub in name:
        reconstituted_line += sub.replace('\n', '')
    return reconstituted_line

def fix_alignment_file_names(name):
    name = name.replace('.txt', '')
    if 'lovelace-transcription' in name:
        target = '-'
        payload = '_'
        return payload.join(name.rsplit(target, 1))
    else:
        return name

def create_author_pair_for_lookups(author_a, author_b):
    ordered_already = str(author_a) + " " + str(author_b)
    disorded_already = str(author_b) + " " + str(author_a)
    if int(author_a) <= int(author_b):
        return ordered_already
    elif int(author_a) > int(author_b):
        return disorded_already