"""
Main cli or app entry point
"""

from mylib.extract import extract
from mylib.load import load
from mylib.query import query

if __name__ == "__main__":
    extract()
    load()
    query()
