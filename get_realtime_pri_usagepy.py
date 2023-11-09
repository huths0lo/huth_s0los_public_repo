import xml.etree.ElementTree as ET
import asyncio
import aiohttp
#from dotenv import load_dotenv



#load_dotenv()

auth = aiohttp.BasicAuth(user, password)
url = 'https://demopub.beyondvoip.net/ast/ASTIsapi.dll?GetPreCannedInfo&Items=getGatewayActivityRequest'
headers = None

async def async_get(url, headers, text=False, extend_timeout=False, auth=None, verify_ssl=False):
    if extend_timeout:
        timeout = 10
    else:
        timeout = 120
    async with aiohttp.ClientSession() as session:
        async with session.get(url=url, headers=headers, auth=auth, verify_ssl=verify_ssl, timeout=timeout) as response:
            status = response.status
            if extend_timeout:
                response = await response.read()
            elif text:
                response = await response.text()
            else:
                response = await response.json(content_type=None)
    return status, response


status, response = asyncio.run(async_get(url, headers, text=True, extend_timeout=False, auth=auth, verify_ssl=False))

tree = ET.fromstring(response)
cm_nodes = int(tree[0].get('NumOfCmNodes'))



def parse_xml(response):
    i = 0
    all_data = []
    while i < len(tree[0]):
        xml_items = item_breakout(tree[0][i])
        all_data.append(xml_items)
        i += 1
    return all_data



def item_breakout(xml_item):
    xml_items = []
    for item in xml_item:
        xml_items.append([item.tag, item.attrib])
    return xml_items

stat_type = 'PRI'
datatype = 'ChannelsActive'

def get_stat(stat_type, datatype, all_data):
    stat_qty = 0
    for devices in all_data:
        for item in devices:
            if item[0] == stat_type:
                stat_qty += int(item[1][datatype])
    return stat_qty
