"""Generic GTFS machinery — stdlib only (decision D2: no third-party imports here).

Modules planned by the M1 plan (Rev. 9, §B); each is a placeholder until its
stage starts:

- ``errors``    — categorised load-error hierarchy (Stage S1).
- ``types``     — dataclasses for the network dataset (Stage S1).
- ``csvio``     — robust GTFS CSV reader (Stage S1).
- ``extractor`` — download, zip-slip-safe extraction, manifest, promotion
  under the immutable ``promote.lock`` record (Stage S2).
- ``parser``    — per-file parsers with validation thresholds (Stage S4,
  after the S3 live archive inspection).
"""
