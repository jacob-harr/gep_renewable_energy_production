'''
NatCap TEEMs Global GEP: Renewable Energy — Pipeline Runner

Runs the pipeline in order:

  01  01_data_getter.py                     clean data in ../data/raw
                                            -> ../data/{}
  02  02_run_renewable_energy_provisions.py  compute GEP (2019 USD)
                                            -> ../output/*_provision_gep.csv
  03  03_results_check.py                   CF/lambda diagnostics + figures
                                            -> ../output/diagnostics/

Note: GEP is reported in 2019 USD — there is no PPP conversion. Stage 01
still runs to refresh ../data/WB_PPP_data.csv, but stage 02 no longer
consumes it.

Each stage runs as a subprocess from this script's directory, so the
scripts' relative paths (../data, ../output) resolve correctly. If any
stage exits non-zero, the runner stops and reports which stage failed.

Usage:
    python run_all.py
'''

import os
import sys
import subprocess

# Ordered pipeline stages: (label, script filename)
STAGES = [
    ('01', '01_data_getter.py'),
    ('02', '02_run_renewable_energy_provisions.py'),
    ('03', '03_results_check.py'),
]


def main():
    here = os.path.dirname(os.path.abspath(__file__))

    for label, script in STAGES:
        script_path = os.path.join(here, script)
        if not os.path.exists(script_path):
            print(f"\n[STAGE {label}] ERROR: {script} not found in {here}")
            sys.exit(1)

        print(f"\n{'=' * 72}")
        print(f"STAGE {label}: {script}")
        print('=' * 72)

        # Run with the same Python interpreter, from this directory
        result = subprocess.run([sys.executable, script], cwd=here)

        if result.returncode != 0:
            print(f"\n{'!' * 72}")
            print(f"STAGE {label} FAILED (exit code {result.returncode}). "
                  f"Pipeline stopped.")
            print('!' * 72)
            sys.exit(result.returncode)

    print(f"\n{'=' * 72}")
    print("PIPELINE COMPLETE — all stages finished successfully.")
    print('=' * 72)


if __name__ == '__main__':
    main()