import pathlib
import re

import fsspec
from bs4 import BeautifulSoup


class SourceLayer:
    def __init__(self, node, base_path, extension=False) -> None:
        self.type = pathlib.Path(node.find("location").text).stem

        self.path = f'zip://{base_path}/{node.find("location").text}'

        with fsspec.open(
            f'zip://{node.find("location").text}::{base_path}',
            encoding=node["encoding"],
            mode="r",
        ) as f:
            sep = re.compile(node["fieldsTerminatedBy"])
            headers = re.split(sep, f.readline().rstrip())

            # extensions nodes have a "coreid" fields that contains the id of "core" row, this is needed for the join
            id_field_lookup = "coreid" if extension else "id"
            self.id = headers[int(node.find(id_field_lookup)["index"])]

    def __str__(self) -> str:
        return self.type


def get_context_from_metafile(resource_path: str):
    meta_file = fsspec.open(f"zip://meta.xml::{resource_path}")
    with meta_file as meta:
        soup = BeautifulSoup(meta, features="lxml-xml")
        extensions = []

        core = SourceLayer(soup.find("core"), resource_path)
        for extension in soup.find_all("extension"):
            extensions.append(SourceLayer(extension, resource_path, extension=True))

        return {
            "core": core,
            "extensions": extensions,
        }
