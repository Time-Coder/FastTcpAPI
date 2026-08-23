import os
import subprocess
import sys

self_folder = os.path.dirname(os.path.abspath(__file__))

subprocess.check_call([
    sys.executable,
    "-m",
    "build",
    "--no-isolation",
    "--wheel",
    "--sdist",
    "--outdir",
    os.path.join(self_folder, "dist"),
], cwd=self_folder)