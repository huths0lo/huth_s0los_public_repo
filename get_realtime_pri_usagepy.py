import xml.etree.ElementTree as ET
import asyncio
import aiohttp
import os
from dotenv import load_dotenv



load_dotenv()

user = os.getenv('user')
password = os.getenv('password')
server = os.getenv('server')



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


def parse_xml(xml_tree):
    i = 0
    all_data = []
    while i < len(xml_tree[0]):
        xml_items = item_breakout(xml_tree[0][i])
        all_data.append(xml_items)
        i += 1
    return all_data



def item_breakout(xml_item):
    xml_items = []
    for item in xml_item:
        xml_items.append([item.tag, item.attrib])
    return xml_items



def get_stat(stat_type, datatype, all_data):
    stat_qty = 0
    for devices in all_data:
        for item in devices:
            if item[0] == stat_type:
                stat_qty += int(item[1][datatype])
    return stat_qty


async def get_pri_stats():
    auth = aiohttp.BasicAuth(user, password)
    url = f'https://{server}/ast/ASTIsapi.dll?GetPreCannedInfo&Items=getGatewayActivityRequest'
    headers = None
    status, response = await async_get(url, headers, text=True, extend_timeout=False, auth=auth, verify_ssl=False)
    xml_tree = ET.fromstring(response)
    stat_type = 'PRI'
    datatype = 'ChannelsActive'
    all_data = parse_xml(xml_tree)
    active_channels = get_stat(stat_type, datatype, all_data)
    return active_channels