import os
import subprocess
import sys

self_folder = os.path.dirname(os.path.abspath(__file__)).replace("\\", "/")
subprocess.check_call([sys.executable, "-m", "twine", "upload", f"{self_folder}/dist/fasttcpapi-*.tar.gz", f"{self_folder}/dist/fasttcpapi-*.whl", "--verbose"])
