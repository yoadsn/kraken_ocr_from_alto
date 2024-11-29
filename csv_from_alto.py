import os
import xml.etree.ElementTree as ET
import csv
import sys




def get_mets_path(year, month, day) -> str:
    return f'{year}{month}{day}{"-METS.xml"}'

def build_mets(mets_root):
    mets_data = []
    for child in mets_root:
        if child.tag == f"{namespace_mets}structMap" and child.get('TYPE') == 'LOGICAL':
            divl3 = child.find(f".//{namespace_mets}div[@ID='DIVL3']")
            for div in divl3:
                if div.get('TYPE') == 'CONTENT':
                    for d in div:
                        d_type = d.get('TYPE')
                        title = d.get('LABEL')
                        article_id = d.get('ID')
                        d_body_content = d.find(f".//{namespace_mets}div[@TYPE='BODY_CONTENT']")
                        begins = []
                        if d_body_content is not None:
                            for body_content in d_body_content:
                                areas = body_content.findall(f".//{namespace_mets}area[@BETYPE='IDREF']")
                                for area in areas:
                                    begin = area.get('BEGIN')
                                    begins.append(begin)
                            mets_data.append({
                                "article_id" : article_id,
                                "begins" : begins,
                                "title" : title,
                                "type" : d_type,
                                "page" : begins[0][1],
                                "text": ""
                            })
                        else:
                            print(f"Article {article_id} does not have content and was skipped.")
    return mets_data

def build_texts_blocks(alto_path):
    texts_blocks = []
    for alto_file in os.listdir(alto_path):
        alto_file_path = os.path.join(alto_path, alto_file)
        with open(alto_file_path, 'r', encoding='utf-8') as alto:
            tree = ET.parse(alto)
            alto_root = tree.getroot()
            #layout = next((alto_root.find(f".//{ns}PrintSpace") for ns in namespace_alto if alto_root.find(f".//{ns}Layout") is not None), None)
            for namespace_alto in namespaces_alto:
                layout = alto_root.find(f".//{namespace_alto}Layout")
                if layout is None:
                    continue
                printSpace = layout.find(f".//{namespace_alto}PrintSpace")
                for textBlock in printSpace:
                    if textBlock.tag == f"{namespace_alto}TextBlock":
                        texts_blocks.append(textBlock)
                        
    if len(texts_blocks) == 0:
        raise Exception("Error find TextBlocks. Verify that the namespaces is inserted to namespaces array.")
    return texts_blocks

def find_text_block(begin, texts_blocks):
    for text_block in texts_blocks:
        if text_block.get("ID") == begin:
            return text_block
        
    return None

def build_text(text_block) -> str:
    def last_iteration(text_block_idx, text_lines_idx):
        return text_block_idx == len(text_block) - 1 and text_lines_idx == len(text_block[text_block_idx]) - 1
    
    text = ""
    for text_block_idx, text_lines in enumerate(text_block):
        for text_lines_idx, text_line in enumerate(text_lines):
            for namespace_alto in namespaces_alto:
                if text_line.tag == f"{namespace_alto}String":
                    text += text_line.get("CONTENT")
                elif text_line.tag == f"{namespace_alto}SP":
                    text += " "
                if last_iteration(text_block_idx, text_lines_idx):
                    if not text_line.tag == f"{namespace_alto}HYP":
                        text += " "
        text += " "
    return text

def write_to_csv(data, csv_name):
    output_file_path = f'{csv_name}.csv'
    header_to_remove = "begins"
    with open(output_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [key for key in data[0].keys() if key != header_to_remove]

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for item in data:
            if header_to_remove in item:
                del item[header_to_remove]
            writer.writerow(item)

def break_code():
    print("Error: Missing required argument.")
    print("Usage: python run.py <folder_name>")
    sys.exit(1)



if __name__ == '__main__':
    directory_path = 'Alto_Samples'
    if len(sys.argv) > 1:
        directory_path = sys.argv[1]
    else:
        break_code()

    namespaces = {'mets': 'http://www.loc.gov/METS/'}
    namespace_mets = "{http://www.loc.gov/METS/}"
    namespaces_alto = [
        "{http://www.loc.gov/standards/alto/ns-v3#}",
        "{http://schema.ccs-gmbh.com/ALTO}"
        ]

    csv_data = []
    for newspaper_folder in os.listdir(directory_path):
        newspaper_folder_path = os.path.join(directory_path, newspaper_folder)
        if os.path.isdir(newspaper_folder_path):
            for year in os.listdir(newspaper_folder_path):
                year_path = os.path.join(newspaper_folder_path, year)
                if os.path.isdir(year_path):
                    for month in os.listdir(year_path):
                        month_path = os.path.join(year_path, month)
                        if os.path.isdir(month_path):
                            for day in os.listdir(month_path):
                                day_path = os.path.join(month_path, day)
                                if os.path.isdir(day_path):
                                    mets_name = get_mets_path(year, month, day)
                                    mets_path = os.path.join(day_path, mets_name)
                                    print(day_path)
                                    with open(mets_path, 'r', encoding='utf-8') as mets:
                                        tree = ET.parse(mets)
                                        mets_root = tree.getroot()
                                        mets_data = build_mets(mets_root)
                                        alto_path = os.path.join(day_path, "ALTO")                                
                                        texts_blocks = build_texts_blocks(alto_path)
                                    
                                        for m in mets_data:
                                            text = ""
                                            for begin in m["begins"]:
                                                text_block = find_text_block(begin, texts_blocks)
                                                if text_block is not None:
                                                    text += build_text(text_block)
                                            m["text"] = text

                                        csv_name = f"output/{newspaper_folder}-{year}-{month}-{day}"
                                        write_to_csv(mets_data, csv_name)
                                        

                                    

