# Assuming I am receiving a json object that is valid to be processed

import json
import uuid

data_string = r'''
{
  "resource_uri": "https://www.courtlistener.com/api/rest/v3/opinions/4582560/",
  "id": 4582560,
  "absolute_url": "/opinion/4802213/united-states-v-jurado-nazario/",
  "cluster": "https://www.courtlistener.com/api/rest/v3/clusters/4802213/",
  "author": null,
  "joined_by": [],
  "author_str": "",
  "per_curiam": false,
  "joined_by_str": "",
  "date_created": "2020-10-30T21:00:16.205128Z",
  "date_modified": "2020-10-30T21:53:16.208598Z",
  "type": "010combined",
  "sha1": "172eea9c42e59a13ede3c067d3dce0ed57ab0a86",
  "page_count": 8,
  "download_url": "http://media.ca1.uscourts.gov/pdf.opinions/18-1679P-01A.pdf",
  "local_path": "pdf/2020/10/30/united_states_v._jurado-nazario.pdf",
  "plain_text": "          United States Court of Appeals\n                     For the First Circuit\nNo. 18-1679\n\n                    UNITED STATES OF AMERICA,\n\n                            Appellee,\n\n                               v.\n\n                      EDWIN JURADO-NAZARIO,\n\n                      Defendant, Appellant.\n\n\n          APPEAL FROM THE UNITED STATES DISTRICT COURT\n                 FOR THE DISTRICT OF PUERTO RICO\n\n       [Hon. Carmen Consuelo Cerezo, U.S. District Judge]\n\n\n                             Before\n\n                        Lynch and Boudin,*\n                         Circuit Judges.\n\n\n     Johnny Rivera-González on brief for appellant.\n     W. Stephen Muldrow, United States Attorney, Mariana E. Bauzá-\nAlmonte, Assistant United States Attorney, Chief, Appellate\nDivision, on brief for appellee.\n\n\n                        October 30, 2020\n\n\n\n\n* While this case was submitted to a panel that included Judge\nTorruella, he did not participate in the issuance of the panel's\nopinion. The remaining two panelists therefore issued the opinion\npursuant to 28 U.S.C. § 46(d).\n\f             BOUDIN, Circuit Judge.       Edwin Jurado-Nazario pled guilty\n\nto two counts of Production of Child Pornography, see 18 U.S.C. §\n\n2551(a) and (e), and two counts of Transportation of a Minor with\n\nthe Intent to Engage in Criminal Sexual Activity, see 18 U.S.C. §\n\n2423(a).     For   these   offenses,    his   plea   agreement      tentatively\n\ncalculated a prison term of 210 to 262 months.\n\n",
  "html": "",
  "html_lawbox": "",
  "html_columbia": "",
  "xml_harvard": "",
  "html_with_citations": "<pre class=\"inline\">          United States Court of Appeals\n                     For the First Circuit\nNo. 18-1679\n\n                    UNITED STATES OF AMERICA,\n\n                            Appellee,\n\n                               v.\n\n                      EDWIN JURADO-NAZARIO,\n\n                      Defendant, Appellant.\n\n\n          APPEAL FROM THE UNITED STATES DISTRICT COURT\n                 FOR THE DISTRICT OF PUERTO RICO\n\n       [Hon. Carmen Consuelo Cerezo, U.S. District Judge]\n\n\n                             Before\n\n                        Lynch and Boudin,*\n                         Circuit Judges.\n\n\n     Johnny Rivera-González on brief for appellant.\n     W. Stephen Muldrow, United States Attorney, Mariana E. Bauzá-\nAlmonte, Assistant United States Attorney, Chief, Appellate\nDivision, on brief for appellee.\n\n\n                        October 30, 2020\n\n\n\n\n* While",
  "extracted_by_ocr": false,
  "opinions_cited": [
    "https://www.courtlistener.com/api/rest/v3/opinions/108416/",
    "https://www.courtlistener.com/api/rest/v3/opinions/145843/",
    "https://www.courtlistener.com/api/rest/v3/opinions/198902/",
    "https://www.courtlistener.com/api/rest/v3/opinions/202770/",
    "https://www.courtlistener.com/api/rest/v3/opinions/618655/"
  ]
}
'''

# Internal Data Model
# {
#     'id': '',
#     'metadata': {
#         'api_id': '',
#         'api_resource_url': '',
#         'api_date_modified': '',
#         'court_from': '',
#         'citations': '',
#         'created': '',
#         'modified': '[]',
#         'pages': '',
#         'document_type': '',
#         'document_date': '',
#         'precedence': '',
#         'case_type': ''
#     },
#     'content': {
#         'document_title': '',
#         'document_text': ''
#     }
# }

# Parses string into json object
data = json.loads(data_string, strict=False)

# Initialize internal data model object
output_data = dict()
output_data['metadata'] = dict()
output_data['content'] = dict()

# Generate a UUID from the resource_uri
output_data['id'] = str(uuid.uuid5(uuid.NAMESPACE_URL, data.get('resource_uri')))

# Transform input json data into IDM object
output_data['metadata']['api_id'] = data.get('id', '')
output_data['metadata']['api_resource_url'] = data.get('resource_uri', '')
output_data['metadata']['api_date_modified'] = data.get('date_modified', '')
output_data['metadata']['court_from'] = ''
output_data['metadata']['citations'] = list()
for citation in data.get('opinions_cited', list()):
    output_data['metadata']['citations'].append(citation)
output_data['metadata']['created'] = data.get('date_created', '')
output_data['metadata']['modified'] = data.get('date_modified', '')
output_data['metadata']['pages'] = data.get('page_count', '')
output_data['metadata']['document_type'] = data.get('type', '')
output_data['metadata']['document_date'] = ''
output_data['metadata']['precedence'] = ''
output_data['metadata']['case_type'] = ''

output_data['content']['document_title'] = ''
output_data['content']['document_text'] = data.get('plain_text', '')


#
output_json = json.dumps(output_data, indent=2)


print(output_json)
