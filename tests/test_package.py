def test_package_import():
    import pipelines
    import pipelines.ingest as ingest

    assert hasattr(ingest, "load_cfg")
    assert hasattr(ingest, "EsiosClient")
