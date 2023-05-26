#!/usr/bin/env python3

#
# Example on how to transfer data from atos to lumi.
# Data will be temporary located under hpc-login:$SCRATCH/case/run/...
#
# So far the transfer implementation is by date-hour only
#
# Ulf Andrae, SMHI, 2023
#

import os
from datetime import datetime

import isodate

from .cases import Cases, hub


def main():
    import argparse

    argparser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    argparser.add_argument("case", help="ex `gavle_2021`")
    argparser.add_argument("run", help="ex `deode_cy46ref`")
    argparser.add_argument("start_time", type=isodate.parse_datetime)
    argparser.add_argument("end_time", type=isodate.parse_datetime)
    argparser.add_argument("dt_step", type=isodate.parse_duration)
    argparser.add_argument("--remote", default="lumi_transfer")
    args = argparser.parse_args()

    step = args.dt_step
    sdate = args.start_time
    edate = args.end_time
    case = args.case
    run = args.run
    remote = args.remote

    # Load the data
    example = Cases(selection={case: [run]})

    # Construct a list of dates
    dates = []
    if sdate is not None and edate is not None:
        while sdate <= edate:
            dates.append(sdate)
            sdate += step

    for case in example.names:
        for exp in example.cases.names:
            x = example.cases.runs.data
            file_template = example.cases.runs.file_templates[0]

            for date in x[file_template].keys():
                if len(dates) > 0:
                    cdate = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
                    if cdate not in dates:
                        continue

                print(" fetch:", date)

                # Parse the paths and expand the dates
                outpath_template = example.meta[case][run][remote]["path_template"]
                scratch_template = os.path.join(
                    os.environ["SCRATCH"], case, exp, "%Y/%m/%d/%H/"
                )
                remote_outpath = hub(outpath_template, date)
                scratch_outpath = hub(scratch_template, date)

                # Get a list of files
                files = example.reconstruct(dtg=date)

                # Do the actual copy from ecf to scratch and rsync to lumi,
                # and clean the intermediate files
                example.transfer(
                    files, scratch_outpath, {"host": remote, "outpath": remote_outpath}
                )


if __name__ == "__main__":
    main()
