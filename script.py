import cProfile
import io
import pstats

from adapter import DepMapAdapter, DepMapEdgeType, DepMapNodeType
import biocypher

PROFILE = False


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

    # select fields from adapter
    node_fields = [
        DepMapNodeType.GENE,
        DepMapNodeType.CELL_LINE,
        DepMapNodeType.COMPOUND,
        DepMapNodeType.SEQUENCE_VARIANT,
    ]

    edge_fields = [
        DepMapEdgeType.GENE_TO_GENE,
        DepMapEdgeType.GENE_TO_CELL_LINE,
        DepMapEdgeType.SEQUENCE_VARIANT_TO_GENE,
        DepMapEdgeType.SEQUENCE_VARIANT_TO_CELL_LINE,
        DepMapEdgeType.CELL_LINE_TO_COMPOUND,
        DepMapEdgeType.COMPOUND_TO_COMPOUND,
        DepMapEdgeType.COMPOUND_TO_GENE,
    ]

    # create adapter
    depmap = DepMapAdapter(node_types=node_fields, edge_types=edge_fields)

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
