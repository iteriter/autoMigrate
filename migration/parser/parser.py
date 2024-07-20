from collections import Counter, OrderedDict, defaultdict, namedtuple
from pprint import pprint
from typing import (
    Any,
    Literal,
    Sequence,
    Iterable,
    Collection,
    List,
    Mapping,
    TypedDict,
)
import enum

import bson
from rbloom import Bloom


class TypeHierarchy(enum.Enum):
    # BOOLEAN = bool
    INTEGER = int
    FLOAT = float
    STRING = str
    # dict = enum.auto()
    # list = enum.auto()
    # set = enum.auto()
    # tuple = enum.auto()
    # object = enum.auto()


SQL_TYPES = {
    bool: "BOOLEAN",
    int: "INTEGER",
    float: "REAL",
    str: "TEXT",
    bson.objectid.ObjectId: "TEXT",
    # dict: "TEXT",
    # list: "TEXT",
    # set: "TEXT",
    # tuple: "TEXT",
}


Field = namedtuple("Field", ["name", "value"])


class FieldSpec(TypedDict):
    infered_type: type
    types: Counter[type]


class UniqueFinder:
    def __init__(self) -> None:
        self.candidates = {}
        self.candidate_maps = {}

    def check_unique(self, field_name, value):
        """
        For a given field value, check if the value
        has been encountered already,
        If yes, then the field is not unique.
        Otherwise, add the value to the bloom filter
        for future checks
        """
        if isinstance(value, Iterable):
            return

        if not self.candidates.get(field_name, True):
            return

        if field_name not in self.candidates:
            self.candidates[field_name] = True
            self.candidate_maps[field_name] = Bloom(10_000_000, 0.001)

        # value has already been seen, hence mark field as not unique
        # and discard the filter
        if value in self.candidate_maps[field_name]:
            self.candidates[field_name] = False
            del self.candidate_maps[field_name]
            return

        self.candidate_maps[field_name].add(value)

    def get_uniques(self):
        return [
            field_name for field_name, is_unique in self.candidates.items() if is_unique
        ]


def print_schema(fields, relationships = None, level=0):
    relationships = relationships or {}

    prefix = "\t\t" * level
    print(prefix + "fields:" + "_" * 40)
    fields = prefix + f"\n{prefix}".join(
        [
            f"+ {k:<26s}:" \
            f"infered type: {v['infered_type'].__name__:<8s} | stats:" \
            f" {', '.join({f"{k.__name__}: {f}" for k, f in v['types'].most_common()})}"
            for k, v in fields.items()
        ]
    )
    print(fields)
    if level == 0:
        print("relationships:")
        for r, r_fields in relationships.items():
            print(f"\t{r}:")
            print_schema(r_fields, [], level=level + 1)


class SchemaParser:
    def __init__(self) -> None:
        self.uniques = UniqueFinder()

    @staticmethod
    def _is_complex(value):
        if isinstance(value, List):
            if not any([v for v in value if isinstance(v, List | Mapping)]):
                return False

        if isinstance(value, Mapping):
            if len(value) > 3:
                return True
            if not any([v for v in value.values() if isinstance(v, List | Mapping)]):
                return False

        return True

    def infer_type(self, value):
        for t in list(TypeHierarchy):
            type_ = t.value
            if isinstance(value, type_):
                return type_

            try:
                value = type_(value)
                return type_
            except (ValueError, TypeError):
                continue
        return t.value

    def parse_object(self, doc):
        fields = {}
        relationships = {}

        for field_name, value in doc.items():
            datatype = type(value)

            if datatype in SQL_TYPES:
                fields[field_name] = value
                continue

            # field either represents an array, field set, or relationship of some kind
            if not self._is_complex(value):
                # if it is a list, then it is an array
                if isinstance(value, list):
                    fields[field_name] = value
                    continue
                # if it is a dict, expand it
                if isinstance(value, dict):
                    # pprint(f"expanding field {value}")
                    extended = {f"{field_name}_{k}": v for k, v in value.items()}
                    fields.update(extended)
                    continue

            if isinstance(value, dict):
                relationships[field_name] = self.parse_object(value)
                continue

            # todo: handle relationships contained in a list
            # if isinstance(value, list):
            #     relationships[field_name] = (self.parse_object(v) for v in value)
            #     continue
            # raise TypeError(f"Unsupported datatype: {datatype}")

        return fields, relationships

    def generate_schema(self, collection, sub=False):
        """
        e.g.
        field_map  = {
            'fieldName': {
                'int': 123,
                'str': 12,
            }
        }
        """
        field_map: dict[str, FieldSpec] = defaultdict(
            lambda: {
                "types": Counter(),
                "infered_type": str,
            }
        )  # type: ignore
        rel_map: dict[str, dict] = {}

        for doc in collection:
            fields, relationships = self.parse_object(doc)

            for field, content in fields.items():
                instance_type = self.infer_type(content)
                field_map[field]["infered_type"] = instance_type
                field_map[field]["types"][instance_type] += 1

                self.uniques.check_unique(field, content)

            for name, rel in relationships.items():
                subparser = SchemaParser()
                rel_map[name] = subparser.generate_schema(rel, sub=True)

        # field_map.update({"age": [int]})
        if not sub:
            print_schema(field_map, rel_map)
        # pprint(self.uniques.get_uniques())
        return field_map
