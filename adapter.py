#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BioCypher - CKG prototype
"""

import csv

from biocypher._logger import logger

logger.debug(f"Loading module {__name__}.")


class DepMapAdapter:
    def __init__(
        self,
        id_batch_size: int = int(1e6),
    ):

        self.id_batch_size = id_batch_size


    def get_nodes(self):
        """
        Get nodes from CSV and yield them to the batch writer.

        Args:
            label: input label of nodes to be read

        Returns:
            generator of tuples representing nodes
        """

        loc_dict = {
            "gene": "data/v0.5/genes/gene_all.csv",
            "compound": "data/v0.5/compounds/compounds_all.csv",
            "cellModel": "data/v0.5/cellModels/cellModels_all.csv",
            "CFE": "data/v0.5/cellModels/CFE_all.csv",
        }

        node_labels = ["gene", "compound", "cellModel", "CFE"]

        for label in node_labels:
            # read csv for each label

            with (open(loc_dict[label], "r")) as f:
                reader = csv.reader(f)
                prop_items = next(reader)
                for row in reader:
                    _id = _process_node_id(row[0], label)
                    _label = label
                    _props = _process_properties(
                        dict(zip(prop_items[1:], row[1:]))
                    )
                    yield _id, _label, _props

    def get_edges(self):
        """
        Get edges from CSV and yield them to the batch writer.

        Args:
            label: input label of edges to be read

        Returns:
            generator of tuples representing edges
        """

        loc_dict = {
            "gene_int": "data/v0.5/genes/gene_int_all.csv",
            "CRISPRKO": "data/v0.5/cellModels/CRISPRKO_all.csv",
            "CFEinv": "data/v0.5/cellModels/CFEinv_all.csv",
            "CFEobs": "data/v0.5/cellModels/CFEobs_all.csv",
            "response": "data/v0.5/compounds/response_all.csv",
            "compound_Tsim": "data/v0.5/compounds/compound_Tsim_ALL.csv",
            "compoundTarget": "data/v0.5/compounds/compoundTarget_lit.csv",
        }

        rel_labels = [
            "gene_int",
            "CRISPRKO",
            "CFEinv",
            "CFEobs",
            "response",
            "compound_Tsim",
            "compoundTarget",
        ]

        for label in rel_labels:

            # read csv for each label
            with (open(loc_dict[label], "r")) as f:
                reader = csv.reader(f)
                prop_items = next(reader)
                for row in reader:
                    _src = _process_node_id(row[0], label)
                    _tar = _process_node_id(row[1], label)
                    _label = label
                    _props = _process_properties(
                        dict(zip(prop_items[2:], row[2:]))
                    )
                    yield _src, _tar, _label, _props


def _process_node_id(_id, _type):
    """
    Add prefixes to avoid multiple assignment. Fix other small issues.
    """

    if '"' in _id:
        _id = _id.replace('"', "")

    return _id


def _process_properties(_props):
    """
    Process properties.
    """

    for key, value in _props.items():
        if '"' in value:
            _props[key] = value.replace('"', "")

    return _props


# multi-line fields: only due to line 832 in cellModels_all.csv?
