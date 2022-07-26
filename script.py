import cProfile
import io
import pstats

from adapter import BioCypherAdapter

PROFILE = False


def main():
    if PROFILE:
        profile = cProfile.Profile()
        profile.enable()

    adapter = BioCypherAdapter(db_name="import")

    adapter.write_to_csv_for_admin_import()

    if PROFILE:
        profile.disable()

        s = io.StringIO()
        sortby = pstats.SortKey.CUMULATIVE
        ps = pstats.Stats(profile, stream=s).sort_stats(sortby)
        ps.print_stats()
        # print(s.getvalue())
        ps.dump_stats("adapter.prof")


if __name__ == "__main__":
    main()
