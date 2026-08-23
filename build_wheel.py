import os
import shutil
import subprocess
import sys

self_folder = os.path.dirname(os.path.abspath(__file__))
build_folder = os.path.join(self_folder, "build")
if os.path.isdir(build_folder):
    shutil.rmtree(build_folder)

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
