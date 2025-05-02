import httpx
import xmltodict
from bs4 import BeautifulSoup


def get_datasets(base_url: str, ipt_url: str):
    res = httpx.get(f"{ipt_url}/rss")
    soup = BeautifulSoup(res.text, features="lxml-xml")
    for item in soup.find_all("item"):
        content = {
            k.replace(":", "_"): v
            for k, v in xmltodict.parse(item.prettify())["item"].items()
        }
        resource_id = content["link"].split("=")[1]
        yield {
            **content,
            "id": resource_id,
            "version": content["guid"]["#text"].split("/")[1].replace("v", ""),
            "url": f"{base_url}resources/{resource_id}",
        }


def get_dataset_metadata(ipt_url: str, resource_id: str):
    url = ipt_url + "/eml.do?r=" + resource_id
    res = httpx.get(url)
    return res.text
