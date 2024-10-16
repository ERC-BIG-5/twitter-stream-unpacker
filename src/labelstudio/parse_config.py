import xml.etree.ElementTree as ET

from src.annot_analysis.label_studio import ResultStruct
from src.consts import BASE_LABELSTUDIO_DATA_PATH


def parse_label_config_xml(xml_string) -> ResultStruct:
    root = ET.fromstring(xml_string)

    choices = {}
    free_text = []
    variable_text_fields = []  # New list for text fields with "$" values

    for view in root.findall('.//View'):
        choices_element = view.find('./Choices')
        if choices_element is not None:
            name = choices_element.get('name')
            options = [choice.get('value') for choice in choices_element.findall('./Choice')]
            choices[name] = options

        textarea = view.find('./TextArea')
        if textarea is not None:
            free_text.append(textarea.get('name'))

        # New: Find all Text elements with values starting with "$"
        for text_element in view.findall('.//Text'):
            value = text_element.get('value')
            if value and value.startswith('$'):
                variable_text_fields.append(text_element.get('name'))

    return ResultStruct(choices=choices, free_text=free_text, inputs=variable_text_fields)


if __name__ == '__main__':
    file = BASE_LABELSTUDIO_DATA_PATH / "label_configs/annotation_nature1.xml"
    print(parse_label_config_xml(file.read_text()))
    # example
    """
    <View>
    <View>
        <Style>
            .lsf-richtext__line: { font-size: 18px; }
        </Style>
        <Header value="Choose a relevance class"/>
        <Text name="post_text" value="$post_text"/>
        <Choices name="relevant" toName="post_text" choice="single" showInline="true">
            <Choice value="Relevant"/>
            <Choice value="Not relevant"/>
            <Choice value="Uncertain"/>
        </Choices>
    </View>
    <View visibleWhen="choice-selected" whenTagName="relevant" whenChoiceValue="Relevant">
        <Header value="Choose a landscape type (corine-classification)"/>
        <Choices name="landscape" toName="post_text" choice="single" showInline="true">
            <Choice value="artificial surfaces"/>
            <Choice value="agricultural"/>
            <Choice value="Uncertain"/>
            <Choice value="forest and seminatural areas"/>
            <Choice value="wetlands"/>
            <Choice value="water bodies"/>
            <Choice value="not identifiable"/>
            <Choice value="ambigious"/>
        </Choices>
    </View>
    <View visibleWhen="choice-selected" whenTagName="relevant" whenChoiceValue="Relevant,Uncertain">
        <Header value="Dimension 1"/>
        <Choices name="non_human" toName="post_text" choice="single" showInline="true">
            <Choice value="non-human"/>
            <Choice value="human"/>
        </Choices>
    </View>


    <View visibleWhen="choice-selected" whenTagName="relevant" whenChoiceValue="Relevant,Uncertain">
        <Header value="Dimension 2"/>
        <Choices name="material" toName="post_text" choice="single" showInline="true">
            <Choice value="material"/>
            <Choice value="ideel"/>
        </Choices>
    </View>

    <View visibleWhen="choice-selected" whenTagName="relevant" whenChoiceValue="Relevant,Uncertain">
        <Header value="Dimension 3"/>
        <Choices name="life" toName="post_text" choice="single" showInline="true">
            <Choice value="life"/>
            <Choice value="mineral"/>
        </Choices>
    </View>

    <View visibleWhen="choice-selected" whenTagName="relevant" whenChoiceValue="Relevant,Uncertain">
        <Header value="Dimension 4"/>
        <Choices name="ideal_state" toName="post_text" choice="single" showInline="true">
            <Choice value="ideal_state"/>
            <Choice value="alteration"/>
        </Choices>
    </View>

    <View>
        <Header value="Notes"/>
        <TextArea toName="post_text" name="notes"/>
    </View>
</View>
    
    
    {
  "choices": {
    "relevant": [
      "Relevant",
      "Not relevant",
      "Uncertain"
    ],
    "landscape": [
      "artificial surfaces",
      "agricultural",
      "Uncertain",
      "forest and seminatural areas",
      "wetlands",
      "water bodies",
      "not identifiable",
      "ambigious"
    ],
    "non_human": [
      "non-human",
      "human"
    ],
    "material": [
      "material",
      "ideel"
    ],
    "life": [
      "life",
      "mineral"
    ],
    "ideal_state": [
      "ideal_state",
      "alteration"
    ]
  },
  "free_text": [
    "notes"
  ]
}
    """
