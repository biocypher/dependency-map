#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BioCypher - CKG prototype
"""

import csv
import os
import biocypher
from biocypher._logger import logger

logger.debug(f"Loading module {__name__}.")


class BioCypherAdapter:
    def __init__(
        self,
        dirname=None,
        db_name="neo4j",
        id_batch_size: int = int(1e6),
        user_schema_config_path="config/schema_config.yaml",
    ):

        self.db_name = db_name
        self.id_batch_size = id_batch_size

        # write driver
        self.bcy = biocypher.Driver(
            offline=True,  # set offline to true,
            # connect to running DB for input data via the neo4j driver
            user_schema_config_path=user_schema_config_path,
            delimiter="¦",
        )
        # # start writer
        # self.bcy.start_bl_adapter()
        # self.bcy.start_batch_writer(dirname=dirname, db_name=self.db_name)

    def write_to_csv_for_admin_import(self):
        """
        Write nodes and edges to admin import csv files.
        """

        self.write_nodes()
        # self.write_edges()
        # self.bcy.write_import_call()

    def write_nodes(self):
        """
        Write nodes to admin import csv files.
        """

        node_labels = ["gene", "compound", "cellModel", "CFE"]

        for label in node_labels:

            self.bcy.write_nodes(self._get_nodes(label))

    def write_edges(self) -> None:
        """
        Write edges to admin import csv files.
        """

        rel_labels = ["gene_int", "CRISPRKO", "CFEinv", "CFEobs", "response", "compound_Tsim", "compoundTarget"]

        for label in rel_labels:

            self.bcy.write_edges(self._get_edges(label))

    def _get_nodes(self, label):
        """
        Get nodes from CSV.
        """

        loc_dict = {
            "gene": "data/v0.5/genes/gene_all.csv",
            "compound": "data/v0.5/compounds/compounds_all.csv",
            "cellModel": "data/v0.5/cellModels/cellModels_all.csv",
            "CFE": "data/v0.5/cellModels/CFE_all.csv",
        }

        # read csv for each label
        with(open(loc_dict[label], "r")) as f:
            reader = csv.reader(f)
            prop_items = next(reader)
            for row in reader:
                _id = row[0]
                _label = label
                _props = dict(zip(prop_items[1:], row[1:]))
                yield _id, _label, _props


    def _get_edges(self, label):
        """
        Get edges from CSV.
        """

        loc_dict = {
            "gene_int": "data/v0.5/genes/gene_int.csv",
            "CRISPRKO": "data/v0.5/genes/CRISPRKO.csv",
            "CFEinv": "data/v0.5/cellModels/CFEinv.csv",
            "CFEobs": "data/v0.5/cellModels/CFEobs.csv",
            "response": "data/v0.5/cellModels/response.csv",
            "compound_Tsim": "data/v0.5/compounds/compound_Tsim.csv",
            "compoundTarget": "data/v0.5/compounds/compoundTarget.csv",
        }

        # read csv for each label
        with(open(loc_dict[label], "r")) as f:
            reader = csv.reader(f)
            next(reader)
            edges = [row for row in reader]

        return edges

def _process_node_id(_id, _type):
    """
    Add prefixes to avoid multiple assignment.
    """

    return _id
