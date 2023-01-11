#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BioCypher - Dependency Map adapter prototype
"""

import csv
from enum import Enum
from typing import Optional
from bioregistry import normalize_curie
from itertools import chain

from biocypher._logger import logger

logger.debug(f"Loading module {__name__}.")


class DepMapNodeType(Enum):
    """
    DepMap nodes.
    """

    GENE = "gene"
    CELL_LINE = "cellModel"
    COMPOUND = "compound"
    SEQUENCE_VARIANT = "CFE"


class DepMapGeneNodeField(Enum):
    """
    Fields available for DepMap genes.
    """

    GENE_SYMBOL = "geneName:ID(GeneID)"
    _PRIMARY_ID = GENE_SYMBOL

    GENE_ENSEMBL_ID = "geneId"
    GENE_CONTIG = "contig"
    GENE_STRAND = "strand"
    GENE_START = "start"
    GENE_END = "end"
    GENE_BIOTYPE = "biotype"
    GENE_GENOME_RELEASE = "genomeRelease"
    GENE_SPECIES = "species"
    GENE_BAGEL_ESSENTIALITY = "BAGELEssential"


class DepMapCellLineNodeField(Enum):
    """
    Fields available for DepMap cell lines.
    """

    CELL_LINE_NAME = "modelName:ID(CellLine-ID)"
    _PRIMARY_ID = CELL_LINE_NAME

    CELL_LINE_MODEL_PASSPORT = "modelId"
    CELL_LINE_SYNONYMS = "synonyms"
    CELL_LINE_TYPE = "modelType"
    CELL_LINE_GROWTH_PROPERTIES = "growthProperties"
    CELL_LINE_DOI = "doi"
    CELL_LINE_PUBMED_ID = "pmed"
    CELL_LINE_TREATMENT = "modelTreatment"
    CELL_LINE_COMMENTS = "modelComments"
    CELL_LINE_MSI_STATUS = "msiStatus"
    CELL_LINE_MUTATIONAL_BURDEN = "mutationalBurden"
    CELL_LINE_PLOIDY = "ploidy"
    CELL_LINE_PARENT_ID = "parentId"
    CELL_LINE_MUTATION_DATA = "mutationData"
    CELL_LINE_METHYLATION_DATA = "methylationData"
    CELL_LINE_EXPRESSION_DATA = "expressionData"
    CELL_LINE_CNV_DATA = "cnvData"
    CELL_LINE_CRISPR_KO_DATA = "crisprKoData"
    CELL_LINE_FUSION_DATA = "fusionData"
    CELL_LINE_DRUG_DATA = "drugData"
    CELL_LINE_RNA_SEQ_DATA = "rnaseqData"
    CELL_LINE_SAMPLE_ID = "sampleId"
    CELL_LINE_TISSUE = "tissue"
    CELL_LINE_TISSUE_STATUS = "tissueStatus"
    CELL_LINE_CANCER_TYPE = "cancerType"
    CELL_LINE_CANCER_TYPE_DETAIL = "cancerTypeDetail"
    CELL_LINE_CANCER_TYPE_NCIT_ID = "cancerTypeNcitId"
    CELL_LINE_AGE_AT_SAMPLING = "ageAtSampling"
    CELL_LINE_SAMPLING_DAY = "samplingDay"
    CELL_LINE_SAMPLING_MONTH = "samplingMonth"
    CELL_LINE_SAMPLING_YEAR = "samplingYear"
    CELL_LINE_SAMPLING_SITE = "sampleSite"
    CELL_LINE_TNMT = "tnmT"
    CELL_LINE_TUMOUR_GRADE = "tumourGrade"
    CELL_LINE_PATIENT_ID = "patientId"
    CELL_LINE_SPECIES = "species"
    CELL_LINE_GENDER = "gender"
    CELL_LINE_ETHNICITY = "ethnicity"
    CELL_LINE_SMOKING_STATUS = "smokingStatus"
    CELL_LINE_COSMIC_ID = "cosmicId"
    CELL_LINE_BROAD_ID = "broadId"
    CELL_LINE_CCLE_ID = "ccleId"
    CELL_LINE_RR_ID = "rrid"
    CELL_LINE_SUPPLIERS = "suppliers"


class DepMapCompoundNodeField(Enum):
    """
    Fields available for DepMap compounds.
    """

    COMPOUND_NAME = "compoundName:ID(Compound-ID)"
    _PRIMARY_ID = COMPOUND_NAME

    COMPOUND_ATC_CLASSIFICATION = "atcClassifications"
    COMPOUND_AVAILABILITY_TYPE = "availabilityType"
    COMPOUND_BIOTHERAPEUTIC = "biotherapeutic"
    COMPOUND_BLACK_BOX_WARNING = "blackBoxWarning"
    COMPOUND_CHEBI_PAR_ID = "chebiParId"
    COMPOUND_CHIRALITY = "chirality"
    COMPOUND_CROSSREFERENCES = "crossReferences"
    COMPOUND_DOSED_INGREDIENT = "dosedIngredient"
    COMPOUND_FIRST_APPROVAL = "firstApproval"
    COMPOUND_FIRST_IN_CLASS = "firstInClass"
    COMPOUND_HELM_NOTATION = "helmNotation"
    COMPOUND_INDICATION_CLASS = "indicationClass"
    COMPOUND_INORGANIC_FLAG = "inorganicFlag"
    COMPOUND_MAX_PHASE = "maxPhase"
    COMPOUND_MOLECULE_CHEMBL_ID = "moleculeChemblId"
    COMPOUND_MOLECULE_HIERARCHY = "moleculeHierarchy"
    COMPOUND_MOLECULE_PROPERTIES = "moleculeProperties"
    COMPOUND_MOLECULE_STRUCTURES = "moleculeStructures"
    COMPOUND_MOLECULE_SYNONYMS = "moleculeSynonyms"
    COMPOUND_MOLECULE_TYPE = "moleculeType"
    COMPOUND_NATURAL_PRODUCT = "naturalProduct"
    COMPOUND_ORAL = "oral"
    COMPOUND_PARENTERAL = "parenteral"
    COMPOUND_POLYMER_FLAG = "polymerFlag"
    COMPOUND_PREFERRED_NAME = "prefName"
    COMPOUND_PRODRUG = "prodrug"
    COMPOUND_SCORE = "score"
    COMPOUND_STRUCTURE_TYPE = "structureType"
    COMPOUND_THERAPEUTIC_FLAG = "therapeuticFlag"
    COMPOUND_TOPICAL = "topical"
    COMPOUND_USAN_STEM = "usanStem"
    COMPOUND_USAN_STEM_DEFINITION = "usanStemDefinition"
    COMPOUND_USAN_SUBSTEM = "usanSubstem"
    COMPOUND_USAN_YEAR = "usanYear"
    COMPOUND_WITHDRAWN_CLASS = "withdrawnClass"
    COMPOUND_WITHDRAWN_COUNTRY = "withdrawnCountry"
    COMPOUND_WITHDRAWN_FLAG = "withdrawnFlag"
    COMPOUND_WITHDRAWN_REASON = "withdrawnReason"
    COMPOUND_WITHDRAWN_YEAR = "withdrawnYear"
    COMPOUND_MOLECULAR_WEIGHT = "molecularWeight"
    COMPOUND_SMILES = "smiles"


class DepMapSequenceVariantNodeField(Enum):
    """
    Fields available for DepMap sequence variants.
    """

    SEQUENCE_VARIANT_NAME = "cfeName:ID(CFE-ID)"
    _PRIMARY_ID = SEQUENCE_VARIANT_NAME


class DepMapEdgeType(Enum):
    """
    DepMap edges.
    """

    GENE_TO_GENE = "gene_int"
    GENE_TO_CELL_LINE = "CRISPRKO"
    SEQUENCE_VARIANT_TO_GENE = "CFEinv"
    SEQUENCE_VARIANT_TO_CELL_LINE = "CFEobs"
    CELL_LINE_TO_COMPOUND = "response"
    COMPOUND_TO_COMPOUND = "compound_Tsim"
    COMPOUND_TO_GENE = "compoundTarget"


class DepMapGeneToGeneEdgeField(Enum):
    """
    Fields available for DepMap gene to gene edges.
    """

    SOURCE_GENE_SYMBOL = "sourceGenesymbol:START_ID(Gene-ID)"
    _PRIMARY_SOURCE_ID = SOURCE_GENE_SYMBOL

    TARGET_GENE_SYMBOL = "targetGenesymbol:END_ID(Gene-ID)"
    _PRIMARY_TARGET_ID = TARGET_GENE_SYMBOL

    SOURCE_UNIPROT_ID = "source"
    TARGET_UNIPROT_ID = "target"
    DIRECTED_FLAG = "isDirected"
    STIMULATION_FLAG = "isStimulation"
    INHIBITION_FLAG = "isInhibition"
    CONSENSUS_DIRECTION = "consensusDirection"
    CONSENSUS_STIMULATION = "consensusStimulation"
    CONSENSUS_INHIBITION = "consensusInhibition"
    DIP_URL = "dipURL"
    SOURCE_DATABASES = "sources"
    LITERATURE = "references"


class DepMapGeneToCellLineEdgeField(Enum):
    """
    Fields available for DepMap gene to cell line edges.
    """

    GENE_NAME = "geneName:START_ID(Gene-ID)"
    _PRIMARY_SOURCE_ID = GENE_NAME

    CELL_LINE_NAME = "modelName:END_ID(CellLine-ID)"
    _PRIMARY_TARGET_ID = CELL_LINE_NAME

    DEPENDENCY_SCORE_BINARY = "depScoreBin"
    DEPENDENCY_SCORE_NORMALISED = "depScoreNorm"


class DepMapSequenceVariantToGeneEdgeField(Enum):
    """
    Fields available for DepMap sequence variant to gene edges.
    """

    SEQUENCE_VARIANT_NAME = "cfeName:START_ID(CFE-ID)"
    _PRIMARY_SOURCE_ID = SEQUENCE_VARIANT_NAME

    GENE_NAME = "geneName:END_ID(Gene-ID)"
    _PRIMARY_TARGET_ID = GENE_NAME


class DepMapSequenceVariantToCellLineEdgeField(Enum):
    """
    Fields available for DepMap sequence variant to cell line edges.
    """

    SEQUENCE_VARIANT_NAME = "cfeName:START_ID(CFE-ID)"
    _PRIMARY_SOURCE_ID = SEQUENCE_VARIANT_NAME

    CELL_LINE_NAME = "modelName:END_ID(CellLine-ID)"
    _PRIMARY_TARGET_ID = CELL_LINE_NAME


class DepMapCellLineToCompoundEdgeField(Enum):
    """
    Fields available for DepMap cell line to compound edges.
    """

    CELL_LINE_NAME = "modelName:START_ID(CellLine-ID)"
    _PRIMARY_SOURCE_ID = CELL_LINE_NAME

    COMPOUND_NAME = "compoundName:END_ID(Compound-ID)"
    _PRIMARY_TARGET_ID = COMPOUND_NAME

    SOURCE_DATABASE = "source"
    IC_50 = "ic50"
    IC_50_MAX = "ic50<MaxConc"
    IC_50_RATIO = "ic50Ratio"


class DepMapCompoundToCompoundEdgeField(Enum):
    """
    Fields available for DepMap compound to compound edges.
    """

    SOURCE_COMPOUND_NAME = "compoundName1:START_ID(Compound-ID)"
    _PRIMARY_SOURCE_ID = SOURCE_COMPOUND_NAME

    TARGET_COMPOUND_NAME = "compoundName2:END_ID(Compound-ID)"
    _PRIMARY_TARGET_ID = TARGET_COMPOUND_NAME

    TANIMOTO_SIMILARITY_SCORE = "tanimotoSim"


class DepMapCompoundToGeneEdgeField(Enum):
    """
    Fields available for DepMap compound to gene edges.
    """

    COMPOUND_NAME = "drugName:START_ID(Compound-ID)"
    _PRIMARY_SOURCE_ID = COMPOUND_NAME

    GENE_NAME = "geneName:END_ID(Gene-ID)"
    _PRIMARY_TARGET_ID = GENE_NAME

    COMPOUND_CHEMBL_ID = "drugChemblId"
    GENE_ENSEMBL_ID = "targetEnsgId"

    LITERATURE = "targetAnnotSource"


class DepMapAdapter:
    def __init__(
        self,
        id_batch_size: int = int(1e6),
        node_types: Optional[list] = None,
        node_fields: Optional[list] = None,
        edge_types: Optional[list] = None,
        edge_fields: Optional[list] = None,
    ):

        self.id_batch_size = id_batch_size

        if node_types:
            self.node_types = [field.value for field in node_types]
        else:
            self.node_types = [field.value for field in DepMapNodeType]

        if edge_types:
            self.edge_types = [field.value for field in edge_types]
        else:
            self.edge_types = [field.value for field in DepMapEdgeType]

        if node_fields:
            self.node_fields = [field.value for field in node_fields]
        else:
            self.node_fields = [
                field.value
                for field in chain(
                    DepMapGeneNodeField,
                    DepMapCellLineNodeField,
                    DepMapCompoundNodeField,
                    DepMapSequenceVariantNodeField,
                )
            ]
        if edge_fields:
            self.edge_fields = [field.value for field in edge_fields]
        else:
            self.edge_fields = [
                field.value
                for field in chain(
                    DepMapGeneToGeneEdgeField,
                    DepMapGeneToCellLineEdgeField,
                    DepMapSequenceVariantToGeneEdgeField,
                    DepMapSequenceVariantToCellLineEdgeField,
                    DepMapCellLineToCompoundEdgeField,
                    DepMapCompoundToCompoundEdgeField,
                    DepMapCompoundToGeneEdgeField,
                )
            ]

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

        for label in self.node_types:
            # read csv for each label

            with (open(loc_dict[label], "r")) as f:
                reader = csv.reader(f)
                prop_items = next(reader)
                for row in reader:
                    _id = _process_node_id(row[0], label)
                    _label = label
                    _props = self._process_properties(
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

        for label in self.edge_types:

            # read csv for each label
            with (open(loc_dict[label], "r")) as f:
                reader = csv.reader(f)
                prop_items = next(reader)
                for row in reader:
                    _src = _process_source_id(row[0], label)
                    _tar = _process_target_id(row[1], label)
                    _label = label
                    _props = self._process_properties(
                        dict(zip(prop_items[2:], row[2:]))
                    )
                    yield _src, _tar, _label, _props

    def _process_properties(self, _props):
        """
        Process properties.
        """

        for key, value in _props.items():

            if key not in self.node_fields and key not in self.edge_fields:
                continue

            if '"' in value:
                _props[key] = value.replace('"', "")

        return _props


def _process_node_id(_id, _type):
    """
    Add prefixes to avoid multiple assignment. Fix other small issues.
    """

    if '"' in _id:
        _id = _id.replace('"', "")

    if _type == "gene":
        _id = normalize_curie("hgnc.symbol:" + _id)
    elif _type == "cellModel":
        _id = normalize_curie("cosmic.cell:" + _id)
    elif _type == "compound":
        _id = "compoundname:" + _id
    elif _type == "CFE":
        _id = "variant:" + _id

    return _id


def _process_source_id(_id, _type):
    """
    Process source ids.
    """

    if _type in ["gene_int", "CRISPRKO"]:

        _id = normalize_curie("hgnc.symbol:" + _id)

    elif _type in ["CFEinv", "CFEobs", "response"]:

        _id = "variant:" + _id

    elif _type in ["compound_Tsim", "compoundTarget"]:

        _id = "compoundname:" + _id

    return _id


def _process_target_id(_id, _type):
    """
    Process target ids.
    """

    if _type in ["gene_int", "CFEinv", "compoundTarget"]:

        _id = normalize_curie("hgnc.symbol:" + _id)

    elif _type in ["CRISPRKO", "CFEobs"]:

        _id = normalize_curie("cosmic.cell:" + _id)

    elif _type in ["response", "compound_Tsim"]:

        _id = "compoundname:" + _id

    return _id


# multi-line fields: only due to line 832 in cellModels_all.csv?
