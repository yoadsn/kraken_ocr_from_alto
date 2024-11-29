import alto
from alto import parse_file
import os
import xml.etree.ElementTree as ET
import xml.etree as etree
from PIL import Image, ImageDraw, ImageFont
import numpy as np
import re
import tempfile


DEFAULT_FONT = ImageFont.truetype("./FreeMono.ttf", 40)

ALTO_TAG_TYPES_REGEX = re.compile('<LayoutTag ID="(.+)" LABEL="(.+)"/>')

class AltoReader:        
    def __init__(self, page_xml_path):
        self.page_xml_path = page_xml_path
        self.page_image = None

        # Calculate downsample factor from page dimensions vs. image dimensions
        img = self.get_page_image()

        # Replace ALTO namespace name instead of changing the code
        with open(self.page_xml_path) as original_xml:
            with tempfile.NamedTemporaryFile('w') as fp:
                fp.write(original_xml.read().replace('http://schema.ccs-gmbh.com/ALTO', 'http://www.loc.gov/standards/alto/ns-v3#'))
            
                self.alto_root = parse_file(fp.name)
        page_height = self.alto_root.layout.pages[0].height
        page_width = self.alto_root.layout.pages[0].width
        image_width, image_height = img.size
        self.downsample_factor = page_height / image_height
        

        self.tag_types = self.get_tag_types()

    def get_tag_types(self):
        """
        Parse the tags section of the ALTO file to extract the labels for each tag type (headline, text block etc.).

        For example, if the file contains:
        `<Tags>
    		<LayoutTag ID="LAYOUT_TAG_000" LABEL="Textblock"/>
    		<LayoutTag ID="LAYOUT_TAG_001" LABEL="Headline"/>
    		...
    	</Tags>`

        This will return {"LAYOUT_TAG_000": "Textblock", "LAYOUT_TAG_001": "Headline", ...}
        """
        with open(self.page_xml_path, "r", encoding="utf-8") as xml_file:
            xml_text = xml_file.read()
            tag_types = ALTO_TAG_TYPES_REGEX.findall(xml_text)
        return {tt[0]: tt[1] for tt in tag_types}
    def get_page_number(self):
        return int(os.path.basename(self.page_xml_path.replace(".xml", "")))
    def get_text_blocks(self):
        """
        Get all text blocks in page XML file (e.g. Pg001.xml)
        """
        all_blocks = self.alto_root.extract_text_blocks()
        blocks = [self.extract_block_data(b) for b in all_blocks]
        self.group_blocks(blocks)
        return blocks

    def get_page_image(self):
        if self.page_image is None:
            page_number = self.get_page_number()

            possible_formats = [f'{page_number:04d}', f'{page_number:05d}']
            for possible_format in possible_formats:
                full_path = os.path.join(os.path.dirname(os.path.dirname(self.page_xml_path)), f'MASTER/{possible_format}.jp2')
                if os.path.exists(full_path):
                    break
            else:
                raise Exception(f'Page file naming format unknown, tried: {possible_formats}. See if another format is in MASTER/xxx.jp2')
            
            image = Image.open(full_path)
            # Convert image from 0-128 range to 0-255 range. Otherwise the conversion to RGB doesn't work
            arr = np.array(image)
            arr[arr==0]=1
            image = Image.fromarray((arr-1)+arr).convert("RGB")
            self.page_image = image
        return self.page_image
    
    def get_image_for_block(self, block, padding=0):
        image = self.get_page_image()
        res_downsample = self.downsample_factor
        crop_coordinates = [int(c / res_downsample) for c in block['position']]
        return image.crop([crop_coordinates[0] - padding, crop_coordinates[1] - padding, crop_coordinates[2] + padding, crop_coordinates[3] + padding])
    
    def extract_block_data(self, block):
        return {
            # "parent_id": parent,
            "position": (block.hpos, block.vpos,  block.hpos+block.width, block.vpos+block.height,),
            "block_id": block.id,
            "tagrefs": block.tagrefs,
            "type": self.tag_types.get(block.tagrefs, "Unknown"),
            "text": "\n".join([" ".join(t.extract_words()) for t in block.text_lines]),
        }

    def group_blocks(self, blocks):
        current_group = 0
        for b in blocks:
            if b["type"] == "Headline" or b["type"] == "ContinuationHeadline":
                current_group += 1
            b["group"] = current_group

    def get_debug_image(self):
        reader = self
        blocks = reader.get_text_blocks()
        image = reader.get_page_image().copy()
        draw = ImageDraw.Draw(image)
        res_downsample = reader.downsample_factor
        for i, block in enumerate(blocks):
            draw.text((int(block["position"][0] / res_downsample), int(block["position"][1] / res_downsample)-40), f"{i} {block['group']} {block['type']}", (255,100,100), font=DEFAULT_FONT)
            draw.rectangle([int(c / res_downsample) for c in block["position"]], outline=(255, 0, 0), width=3)
        return image


class TextBox(dict):
    pass
    
class NewAltoParser:
    def __init__(self, xml_string):
        self.root = ET.fromstring(self.strip_namespace(xml_string))

        
    def strip_namespace(self, xml_string):
        """
        Remove namespaces and other redundant attributes from the <alto> root element
        Args:
            xml_string (str): XML data as a string.
        Returns:
            str: XML string without namespaces.
        """
        xml_string = re.sub('<alto [^>]+>', '<alto>', xml_string, count=1)
        return xml_string
    
    
    def extract_text(self, xml_element):
        """
        Recursively extracts and joins the 'CONTENT' attributes from all children of the given XML element.
    
        Args:
            xml_element (xml.etree.ElementTree.Element): The XML element to process.
    
        Returns:
            str: A string containing the joined 'CONTENT' attributes of all child elements.
        """
        texts = []
    
        # If the current element has a 'CONTENT' attribute, add it to the list
        if 'CONTENT' in xml_element.attrib:
            texts.append(xml_element.attrib['CONTENT'])
    
        # Recursively process all child elements
        for child in xml_element:
            texts.append(self.extract_text(child))
    
        # Join all collected texts with spaces
        return ' '.join(filter(None, texts))
        
    def extract_text_blocks(self):
        """
        Extract all TextBlock elements from the given XML and return a list of dictionaries.
        
        Each dictionary contains:
        - attributes: Dictionary of the TextBlock element's attributes.
        - text_lines: List of dictionaries representing TextLine children and their attributes.
        
        Args:
            xml_string (str): XML data as a string.
        
        Returns:
            list: List of dictionaries representing TextBlock elements.
        """
        root = self.root
        text_blocks = []
    
        for text_block in root.findall(".//TextBlock"):
            data = {'tagrefs': None} # Default values
            data.update({key.lower(): value for key, value in text_block.attrib.items()})
            block_data = TextBox(data)
            setattr(block_data, 'text_lines', [])
            for key in block_data:
                setattr(block_data, key, block_data[key])
            
            # Collect all TextLine children and their attributes
            for text_line in text_block.findall(".//TextLine"):
                line_data = {key.lower(): value for key, value in text_line.attrib.items()}
                line_data['text'] = self.extract_text(text_line)
                block_data.text_lines.append(line_data)
    
            text_blocks.append(block_data)
    
        return text_blocks
        
    def get_page_size(self):
        page = self.root.findall(".//Page")[0]
        return float(page.attrib['WIDTH']), float(page.attrib['HEIGHT'])

class NewAltoReader:    
    """
    Alto reader that is not based on the alto library, should work better but in case of bugs check the old version
    """
    def __init__(self, page_xml_path):
        self.page_xml_path = page_xml_path
        self.page_image = None

        # Calculate downsample factor from page dimensions vs. image dimensions
        img = self.get_page_image()

        # Replace ALTO namespace name instead of changing the code
        with open(self.page_xml_path) as original_xml:
            self.alto_root = NewAltoParser(original_xml.read())
        page_width, page_height = self.alto_root.get_page_size()
        image_width, image_height = img.size
        self.downsample_factor = page_height / image_height
        

        self.tag_types = self.get_tag_types()

    def get_tag_types(self):
        """
        Parse the tags section of the ALTO file to extract the labels for each tag type (headline, text block etc.).

        For example, if the file contains:
        `<Tags>
    		<LayoutTag ID="LAYOUT_TAG_000" LABEL="Textblock"/>
    		<LayoutTag ID="LAYOUT_TAG_001" LABEL="Headline"/>
    		...
    	</Tags>`

        This will return {"LAYOUT_TAG_000": "Textblock", "LAYOUT_TAG_001": "Headline", ...}
        """
        with open(self.page_xml_path, "r", encoding="utf-8") as xml_file:
            xml_text = xml_file.read()
            tag_types = ALTO_TAG_TYPES_REGEX.findall(xml_text)
        return {tt[0]: tt[1] for tt in tag_types}
    def get_page_number(self):
        return int(os.path.basename(self.page_xml_path.replace(".xml", "")))
    def get_text_blocks(self):
        """
        Get all text blocks in page XML file (e.g. Pg001.xml)
        """
        all_blocks = self.alto_root.extract_text_blocks()
        blocks = [self.extract_block_data(b) for b in all_blocks]
        self.group_blocks(blocks)
        return blocks

    def get_page_image(self):
        if self.page_image is None:
            page_number = self.get_page_number()

            possible_formats = [f'{page_number:04d}', f'{page_number:05d}']
            for possible_format in possible_formats:
                full_path = os.path.join(os.path.dirname(os.path.dirname(self.page_xml_path)), f'MASTER/{possible_format}.jp2')
                if os.path.exists(full_path):
                    break
            else:
                raise Exception(f'Page file naming format unknown, tried: {possible_formats}. See if another format is in MASTER/xxx.jp2')
            
            image = Image.open(full_path)
            
            # If needed, convert image from 0-128 range to 0-255 range. Otherwise the conversion to RGB doesn't work
            arr = np.array(image)
            if arr.max() <= 128:
                arr[arr==0]=1
                image = Image.fromarray((arr-1)+arr).convert("RGB")

            self.page_image = image
        return self.page_image
    
    def get_image_for_block(self, block, padding=0):
        image = self.get_page_image()
        res_downsample = self.downsample_factor
        crop_coordinates = [int(c / res_downsample) for c in block['position']]
        return image.crop([crop_coordinates[0] - padding, crop_coordinates[1] - padding, crop_coordinates[2] + padding, crop_coordinates[3] + padding])
    
    def extract_block_data(self, block):
        return {
            # "parent_id": parent,
            "position": (float(block.hpos), float(block.vpos),  float(block.hpos)+float(block.width), float(block.vpos)+float(block.height),),
            "block_id": block.id,
            "tagrefs": block.tagrefs,
            "type": self.tag_types.get(block.tagrefs, "Unknown"),
            "text": "\n".join([" ".join(t['text']) for t in block.text_lines]),
        }

    def group_blocks(self, blocks):
        current_group = 0
        for b in blocks:
            if b["type"] == "Headline" or b["type"] == "ContinuationHeadline":
                current_group += 1
            b["group"] = current_group

    def get_debug_image(self):
        reader = self
        blocks = reader.get_text_blocks()
        image = reader.get_page_image().copy()
        draw = ImageDraw.Draw(image)
        res_downsample = reader.downsample_factor
        for i, block in enumerate(blocks):
            draw.text((int(block["position"][0] / res_downsample), int(block["position"][1] / res_downsample)-40), f"{i} {block['group']} {block['type']}", (255,100,100), font=DEFAULT_FONT)
            draw.rectangle([int(c / res_downsample) for c in block["position"]], outline=(255, 0, 0), width=3)
        return image



class OliveReader:
    RESOLUTION_MAP = {0: ("", 4.2), 1: ("_150", 1.68), 2: ("_252", 1)}
    
    def __init__(self, page_xml_path):
        self.page_xml_path = page_xml_path
        self.page_images = {}
    def get_page_number(self):
        return int(os.path.basename(self.page_xml_path.replace("Pg", "").replace(".xml", "")))
    def get_text_blocks(self):
        """
        Get all text blocks in page XML file (e.g. Pg001.xml)
        """
        olive_root = ET.parse(self.page_xml_path).getroot()
        all_blocks = []
        for child in olive_root:
            try:
                new_root = ET.parse(os.path.join(os.path.dirname(self.page_xml_path), f'{child.attrib["ID"]}.xml')).getroot()
                for w in list(new_root.findall("HedLine_hl1")) + list(new_root.findall("Content")):
                    if w.tag == "HedLine_hl1":
                        block_type = "headline"
                    elif w.tag == "Content":
                        block_type = "paragraph"
                    else:
                        block_type = "unknown"
                    
                    blocks = self.get_blocks(w)
                    for block in blocks:
                        block_data = self.extract_block_data(block, block_type=block_type, parent=child.attrib["ID"])
                        all_blocks.append(block_data)
            except Exception as e:
                pass
        return all_blocks

    def get_page_image(self, resolution):
        res_suffix, res_downsample = self.RESOLUTION_MAP[resolution]
        if self.page_images.get(resolution) is None:
            page_number = self.get_page_number()
            self.page_images[resolution] = Image.open(os.path.join(os.path.dirname(os.path.dirname(self.page_xml_path)), f'{page_number}/Img/Pg{page_number:03d}{res_suffix}.png')).convert('RGB')
        return self.page_images[resolution]
    
    def get_image_for_block(self, block, resolution=2, padding=0):
        image = self.get_page_image(resolution)
        res_suffix, res_downsample = self.RESOLUTION_MAP[resolution]
        crop_coordinates = [int(c / res_downsample) for c in block['position']]
        return image.crop([crop_coordinates[0] - padding, crop_coordinates[1] - padding, crop_coordinates[2] + padding, crop_coordinates[3] + padding])

    def get_blocks(self, parent_el):
        return parent_el.findall("Primitive")
    
    def extract_block_data(self, block, parent, block_type):
        box = block.attrib["BOX"]
        sequence = block.attrib["SEQ_NO"]
        a, b, c, d = box.split()
        words = self.extract_words(block)
        return {
            "parent_id": parent,
            "position": [int(int(x)) for x in (a, b, c, d)],
            "block_type": block_type,
            "sequence_number": int(sequence),
            "text": " ".join(words),
        }
    
    def extract_words(self, parent_el):
        words = []
        for child in parent_el:
            words += self.extract_words(child)
        if parent_el.tag in ["W", "QW"]:
            words.append(parent_el.text)
        return words





def build_mets(mets_root):
    namespace_mets = "{http://www.loc.gov/METS/}"
    
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
                                "page" : begins[0].split('_')[0][1:], # 'P12_Ar0120502' -> '12'
                                "text": ""
                            })
                        else:
                            print(f"Article {article_id} does not have content and was skipped.")
    return mets_data