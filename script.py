import cProfile
import io
import pstats

from dmb.adapter import (
    DepMapAdapter,
    DepMapEdgeType,
    DepMapNodeType,
    DepMapGeneNodeField,
    DepMapCellLineNodeField,
    DepMapCompoundNodeField,
    DepMapSequenceVariantNodeField,
    DepMapGeneToGeneEdgeField,
    DepMapGeneToCellLineEdgeField,
    DepMapSequenceVariantToGeneEdgeField,
    DepMapSequenceVariantToCellLineEdgeField,
    DepMapCellLineToCompoundEdgeField,
    DepMapCompoundToCompoundEdgeField,
    DepMapCompoundToGeneEdgeField,
)
import biocypher

PROFILE = False

# Configure node types and fields
node_types = [
    DepMapNodeType.GENE,
    DepMapNodeType.CELL_LINE,
    DepMapNodeType.COMPOUND,
    DepMapNodeType.SEQUENCE_VARIANT,
]

node_fields = [
    DepMapGeneNodeField._PRIMARY_ID,
    DepMapGeneNodeField.GENE_ENSEMBL_ID,
    DepMapGeneNodeField.GENE_BIOTYPE,
    DepMapGeneNodeField.GENE_BAGEL_ESSENTIALITY,
    DepMapCellLineNodeField._PRIMARY_ID,
    DepMapCellLineNodeField.CELL_LINE_NAME,
    DepMapCellLineNodeField.CELL_LINE_CCLE_ID,
    DepMapCellLineNodeField.CELL_LINE_COSMIC_ID,
    DepMapCellLineNodeField.CELL_LINE_BROAD_ID,
    DepMapCellLineNodeField.CELL_LINE_TISSUE,
    DepMapCompoundNodeField._PRIMARY_ID,
    DepMapCompoundNodeField.COMPOUND_NAME,
    DepMapCompoundNodeField.COMPOUND_SCORE,
    DepMapCompoundNodeField.COMPOUND_CHEBI_PAR_ID,
    DepMapSequenceVariantNodeField._PRIMARY_ID,
    DepMapSequenceVariantNodeField.SEQUENCE_VARIANT_NAME,
]


edge_types = [
    DepMapEdgeType.GENE_TO_GENE,
    DepMapEdgeType.GENE_TO_CELL_LINE,
    DepMapEdgeType.SEQUENCE_VARIANT_TO_GENE,
    DepMapEdgeType.SEQUENCE_VARIANT_TO_CELL_LINE,
    DepMapEdgeType.CELL_LINE_TO_COMPOUND,
    DepMapEdgeType.COMPOUND_TO_COMPOUND,
    DepMapEdgeType.COMPOUND_TO_GENE,
]

edge_fields = [
    DepMapGeneToGeneEdgeField._PRIMARY_SOURCE_ID,
    DepMapGeneToGeneEdgeField._PRIMARY_TARGET_ID,
    DepMapGeneToGeneEdgeField.SOURCE_DATABASES,
    DepMapGeneToGeneEdgeField.SOURCE_UNIPROT_ID,
    DepMapGeneToGeneEdgeField.TARGET_UNIPROT_ID,
    DepMapGeneToGeneEdgeField.SOURCE_DATABASES,
    DepMapGeneToGeneEdgeField.LITERATURE,
    DepMapGeneToCellLineEdgeField._PRIMARY_SOURCE_ID,
    DepMapGeneToCellLineEdgeField._PRIMARY_TARGET_ID,
    DepMapGeneToCellLineEdgeField.DEPENDENCY_SCORE_BINARY,
    DepMapGeneToCellLineEdgeField.DEPENDENCY_SCORE_NORMALISED,
    DepMapSequenceVariantToGeneEdgeField._PRIMARY_SOURCE_ID,
    DepMapSequenceVariantToGeneEdgeField._PRIMARY_TARGET_ID,
    DepMapSequenceVariantToCellLineEdgeField._PRIMARY_SOURCE_ID,
    DepMapSequenceVariantToCellLineEdgeField._PRIMARY_TARGET_ID,
    DepMapCellLineToCompoundEdgeField._PRIMARY_SOURCE_ID,
    DepMapCellLineToCompoundEdgeField._PRIMARY_TARGET_ID,
    DepMapCellLineToCompoundEdgeField.IC_50,
    DepMapCellLineToCompoundEdgeField.SOURCE_DATABASE,
    DepMapCompoundToCompoundEdgeField._PRIMARY_SOURCE_ID,
    DepMapCompoundToCompoundEdgeField._PRIMARY_TARGET_ID,
    DepMapCompoundToCompoundEdgeField.TANIMOTO_SIMILARITY_SCORE,
    DepMapCompoundToGeneEdgeField._PRIMARY_SOURCE_ID,
    DepMapCompoundToGeneEdgeField._PRIMARY_TARGET_ID,
    DepMapCompoundToGeneEdgeField.COMPOUND_CHEMBL_ID,
    DepMapCompoundToGeneEdgeField.GENE_ENSEMBL_ID,
    DepMapCompoundToGeneEdgeField.LITERATURE,
]


def main():
    """
    Connect BioCypher to DepMap adapter to import data into Neo4j.

    Optionally, run with profiling.
    """
    if PROFILE:
        profile = cProfile.Profile()
        profile.enable()

    ###############
    # ACTUAL CODE #
    ###############

    # start biocypher
    driver = biocypher.Driver(
        offline=True,
        db_name="neo4j",
        user_schema_config_path="config/schema_config.yaml",
        quote_char='"',
        skip_duplicate_nodes=True,
        skip_bad_relationships=True,
    )

    # create adapter
    depmap = DepMapAdapter(
        node_types=node_types,
        node_fields=node_fields,
        edge_types=edge_types,
        edge_fields=edge_fields,
    )

    # write nodes and edges to csv
    driver.write_nodes(depmap.get_nodes())
    driver.write_edges(depmap.get_edges())

    # convenience and stats
    driver.write_import_call()
    driver.log_missing_bl_types()
    driver.log_duplicates()
    driver.show_ontology_structure()

    ######################
    # END OF ACTUAL CODE #
    ######################

    if PROFILE:
        profile.disable()

        s = io.StringIO()
        sortby = pstats.SortKey.CUMULATIVE
        ps = pstats.Stats(profile, stream=s).sort_stats(sortby)
        ps.print_stats()

        ps.dump_stats("adapter.prof")
        # look at stats using snakeviz


if __name__ == "__main__":
    main()
