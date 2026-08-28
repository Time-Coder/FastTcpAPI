"""PyInstaller hook for FastTcpAPI package data."""

from PyInstaller.utils.hooks import collect_data_files


datas = collect_data_files("fasttcpapi", includes=["client.pyi.in"])
